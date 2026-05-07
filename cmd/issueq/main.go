package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"issueq/internal/config"
	"issueq/internal/daemon"
	"issueq/internal/dispatcher"
	"issueq/internal/eventcore"
	issuegithub "issueq/internal/github"
	"issueq/internal/jobwrapper"
	"issueq/internal/logging"
	"issueq/internal/model"
	"issueq/internal/poller"
	"issueq/internal/router"
	sqlitestore "issueq/internal/store/sqlite"

	"github.com/spf13/cobra"
)

func main() {
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	var configPath string
	var mode string

	cmd := &cobra.Command{
		Use:           "issueq",
		SilenceUsage:  true,
		SilenceErrors: true,
		Short:         "Local automation queue runner",
		Long: "issueq runs configured local automation commands from a durable SQLite queue. " +
			"For the event hard-cutover, production daemon mode claims issueq-event/v1 automation events; legacy issue polling commands remain available only as explicit compatibility subcommands.",
		RunE: func(cmd *cobra.Command, args []string) error { return cmd.Help() },
	}
	cmd.PersistentFlags().StringVar(&configPath, "config", config.DefaultConfigPath, "path to issueq YAML config")
	cmd.PersistentFlags().StringVar(&mode, "mode", "events", "run mode for daemon/once: events (default cutover path) or legacy")
	cmd.AddCommand(
		eventCommand(&configPath),
		eventsCommand(&configPath),
		eventRunCommand(&configPath),
		daemonCommand(&configPath, &mode),
		onceCommand(&configPath, &mode),
		pollCommand(&configPath),
		routeCommand(&configPath),
		dispatchCommand(&configPath),
		jobWrapperCommand(),
		jobsCommand(&configPath),
		issuesCommand(&configPath),
		configCheckCommand("config-check", "Validate issueq configuration", &configPath),
		configCheckCommand("doctor", "Check local issueq setup", &configPath),
	)
	return cmd
}

func configCheckCommand(use, short string, configPath *string) *cobra.Command {
	return &cobra.Command{Use: use, Short: short, RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := config.LoadFile(*configPath); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "config OK: %s\n", *configPath)
		return nil
	}}
}

func daemonCommand(configPath *string, mode *string) *cobra.Command {
	return &cobra.Command{Use: "daemon", Short: "Run the long-lived issueq daemon (event mode by default)", RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		switch *mode {
		case "", "events", "event":
			cfg, store, err := openConfiguredStore(ctx, *configPath)
			if err != nil {
				return err
			}
			defer store.Close()
			leaseOwner := fmt.Sprintf("%s-%d", runnerID(*cfg), os.Getpid())
			err = eventcore.RunLoop(ctx, *cfg, store, logging.New(), eventcore.RunOptions{LeaseOwner: leaseOwner, Lease: cfg.Queue.LeaseDuration.Duration, Workdir: cfg.Workdir.Path, Runner: model.RunnerInfo{ID: leaseOwner, Name: cfg.Runner.Name}}, cfg.Polling.Interval.Duration)
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil
			}
			return err
		case "legacy":
			cfg, store, gh, err := openGitHubStore(ctx, *configPath)
			if err != nil {
				return err
			}
			defer store.Close()
			err = daemon.Run(ctx, *cfg, store, gh, logging.New())
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil
			}
			return err
		default:
			return fmt.Errorf("unsupported mode %q (use events or legacy)", *mode)
		}
	}}
}

func onceCommand(configPath *string, mode *string) *cobra.Command {
	var noWait bool
	c := &cobra.Command{Use: "once", Short: "Run one event claim/run/finalize cycle (event mode by default)", RunE: func(cmd *cobra.Command, args []string) error {
		if noWait {
			return fmt.Errorf("once --no-wait is not supported until background child supervision is implemented")
		}
		switch *mode {
		case "", "events", "event":
			cfg, store, err := openConfiguredStore(cmd.Context(), *configPath)
			if err != nil {
				return err
			}
			defer store.Close()
			leaseOwner := fmt.Sprintf("%s-%d", runnerID(*cfg), os.Getpid())
			result, key, err := eventcore.RunOnce(cmd.Context(), *cfg, store, eventcore.RunOptions{LeaseOwner: leaseOwner, Lease: cfg.Queue.LeaseDuration.Duration, Workdir: cfg.Workdir.Path, Runner: model.RunnerInfo{ID: leaseOwner, Name: cfg.Runner.Name}})
			if err != nil {
				return err
			}
			if result.Claimed == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "once OK: no event claimed")
				return nil
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "once OK: event_key=%s claimed=%d finalized=%d\n", key, result.Claimed, result.Finalized)
			return nil
		case "legacy":
			cfg, store, gh, err := openGitHubStore(cmd.Context(), *configPath)
			if err != nil {
				return err
			}
			defer store.Close()
			result, err := daemon.Once(cmd.Context(), *cfg, store, gh)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "once OK: fetched=%d routed=%d claimed=%d succeeded=%d failed=%d\n", result.Poll.IssuesFetched, result.Route.JobsCreated, result.Dispatch.Claimed, result.Dispatch.Succeeded, result.Dispatch.Failed)
			return nil
		default:
			return fmt.Errorf("unsupported mode %q (use events or legacy)", *mode)
		}
	}}
	c.Flags().BoolVar(&noWait, "no-wait", false, "unsupported: background reconciliation is not implemented")
	return c
}

func pollCommand(configPath *string) *cobra.Command {
	return &cobra.Command{Use: "poll", Short: "Poll GitHub issues into the local store", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, store, gh, err := openGitHubStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		result, err := poller.Poll(cmd.Context(), *cfg, gh, store)
		if err != nil {
			return fmt.Errorf("poll failed: %w", err)
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "poll OK: fetched=%d upserted=%d\n", result.IssuesFetched, result.IssuesUpserted)
		return nil
	}}
}

func routeCommand(configPath *string) *cobra.Command {
	return &cobra.Command{Use: "route", Short: "Route locally stored issues into jobs", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		result, err := router.Route(cmd.Context(), *cfg, store)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "route OK: issues=%d matched=%d created=%d existing=%d\n", result.IssuesEvaluated, result.RoutesMatched, result.JobsCreated, result.JobsExisting)
		return nil
	}}
}

func dispatchCommand(configPath *string) *cobra.Command {
	var localNoGitHub bool
	c := &cobra.Command{Use: "dispatch", Short: "Dispatch eligible queued jobs", RunE: func(cmd *cobra.Command, args []string) error {
		var cfg *config.Config
		var store *sqlitestore.Store
		var gh issuegithub.Client
		var err error
		if localNoGitHub {
			cfg, store, err = openConfiguredStore(cmd.Context(), *configPath)
		} else {
			cfg, store, gh, err = openGitHubStore(cmd.Context(), *configPath)
		}
		if err != nil {
			return err
		}
		defer store.Close()
		result, err := dispatcher.DispatchWithGitHub(cmd.Context(), *cfg, store, gh)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "dispatch OK: claimed=%d succeeded=%d failed=%d dead=%d skipped=%d\n", result.Claimed, result.Succeeded, result.Failed, result.Dead, result.Skipped)
		return nil
	}}
	c.Flags().BoolVar(&localNoGitHub, "local-no-github", false, "dispatch without GitHub refresh/actions/attempt enforcement; intended for local fixtures only")
	return c
}

func jobWrapperCommand() *cobra.Command {
	var specPath string
	c := &cobra.Command{Use: "job-wrapper", Short: "internal durable job execution wrapper", Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if specPath == "" {
			return fmt.Errorf("--spec is required")
		}
		spec, err := jobwrapper.LoadSpec(specPath)
		if err != nil {
			return err
		}
		sigCh := make(chan os.Signal, 2)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sigCh)
		_, err = jobwrapper.Run(cmd.Context(), spec, jobwrapper.Options{Cancel: sigCh})
		return err
	}}
	c.Flags().StringVar(&specPath, "spec", "", "path to job wrapper launch spec JSON")
	return c
}

func jobsCommand(configPath *string) *cobra.Command {
	var jsonOut bool
	c := &cobra.Command{Use: "jobs", Short: "List local queued jobs", RunE: func(cmd *cobra.Command, args []string) error {
		_, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		jobs, err := store.ListJobs(cmd.Context())
		if err != nil {
			return err
		}
		if jsonOut {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(jobs)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "ID\tSTATUS\tROUTE\tKIND\tPRI\tERROR")
		for _, job := range jobs {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\t%d\t%s\n", job.ID, job.Status, job.RouteName, job.Kind, job.Priority, job.LastError)
		}
		return nil
	}}
	c.Flags().BoolVar(&jsonOut, "json", false, "emit JSON")
	return c
}

func issuesCommand(configPath *string) *cobra.Command {
	var jsonOut bool
	c := &cobra.Command{Use: "issues", Short: "List local issue snapshots", RunE: func(cmd *cobra.Command, args []string) error {
		_, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		issues, err := store.ListIssues(cmd.Context())
		if err != nil {
			return err
		}
		if jsonOut {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(issues)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "ISSUE\tSTATE\tTITLE")
		for _, issue := range issues {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", issue.IssueKey, issue.State, issue.Title)
		}
		return nil
	}}
	c.Flags().BoolVar(&jsonOut, "json", false, "emit JSON")
	return c
}

func openGitHubStore(ctx context.Context, configPath string) (*config.Config, *sqlitestore.Store, issuegithub.Client, error) {
	cfg, store, err := openConfiguredStoreWithOptions(ctx, configPath, config.ValidateOptions{RequireGitHubToken: true})
	if err != nil {
		return nil, nil, nil, err
	}
	token := os.Getenv(cfg.GitHub.TokenEnv)
	gh, err := issuegithub.NewRESTClient(cfg.GitHub.Host, token)
	if err != nil {
		_ = store.Close()
		return nil, nil, nil, err
	}
	return cfg, store, gh, nil
}

func openConfiguredStore(ctx context.Context, configPath string) (*config.Config, *sqlitestore.Store, error) {
	return openConfiguredStoreWithOptions(ctx, configPath, config.ValidateOptions{})
}

func openConfiguredStoreWithOptions(ctx context.Context, configPath string, opts config.ValidateOptions) (*config.Config, *sqlitestore.Store, error) {
	cfg, err := config.LoadFileWithOptions(configPath, opts)
	if err != nil {
		return nil, nil, err
	}
	store, err := sqlitestore.Open(ctx, cfg.Queue.SQLite.Path)
	if err != nil {
		return nil, nil, err
	}
	return cfg, store, nil
}

func eventCommand(configPath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "event", Short: "Manage automation events"}
	var jsonPath string
	upsert := &cobra.Command{Use: "upsert", Short: "Upsert an automation event", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		var data []byte
		if jsonPath == "" || jsonPath == "-" {
			data, err = io.ReadAll(cmd.InOrStdin())
		} else {
			data, err = os.ReadFile(jsonPath)
		}
		if err != nil {
			return err
		}
		var in eventcore.EventUpsert
		if err := json.Unmarshal(data, &in); err != nil {
			return err
		}
		ev, inserted, protected, err := eventcore.Upsert(cmd.Context(), *cfg, store, in)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "event upsert OK: key=%s inserted=%t terminal_protected=%t status=%s\n", ev.EventKey, inserted, protected, ev.Status)
		return nil
	}}
	upsert.Flags().StringVar(&jsonPath, "json", "-", "event JSON file path, or -/empty for stdin")
	cmd.AddCommand(upsert)
	return cmd
}

func eventsCommand(configPath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "events", Short: "Inspect automation events"}
	var jsonOut bool
	list := &cobra.Command{Use: "list", Short: "List automation events", RunE: func(cmd *cobra.Command, args []string) error {
		_, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		events, err := store.ListAutomationEvents(cmd.Context())
		if err != nil {
			return err
		}
		if jsonOut {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(events)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "KEY\tSTATUS\tROUTE\tKIND\tATTEMPTS")
		for _, ev := range events {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\t%d\n", ev.EventKey, ev.Status, ev.RouteName, ev.Kind, ev.AttemptCount)
		}
		return nil
	}}
	list.Flags().BoolVar(&jsonOut, "json", false, "emit JSON")
	show := &cobra.Command{Use: "show <event-key>", Args: cobra.ExactArgs(1), Short: "Show automation event", RunE: func(cmd *cobra.Command, args []string) error {
		_, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		ev, err := store.GetAutomationEvent(cmd.Context(), args[0])
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(ev)
	}}
	cancel := &cobra.Command{Use: "cancel <event-key>", Args: cobra.ExactArgs(1), Short: "Cancel automation event", RunE: func(cmd *cobra.Command, args []string) error {
		_, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		if err := store.CancelAutomationEvent(cmd.Context(), args[0]); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "event cancelled: %s\n", args[0])
		return nil
	}}
	retry := &cobra.Command{Use: "retry <event-key>", Args: cobra.ExactArgs(1), Short: "Retry failed/cancelled automation event", RunE: func(cmd *cobra.Command, args []string) error {
		_, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		if err := store.RetryAutomationEvent(cmd.Context(), args[0]); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "event retry requested: %s\n", args[0])
		return nil
	}}
	cmd.AddCommand(list, show, cancel, retry)
	return cmd
}

func eventRunCommand(configPath *string) *cobra.Command {
	var leaseOwner string
	c := &cobra.Command{Use: "event-run-once", Short: "Claim and run one automation event", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		if leaseOwner == "" {
			leaseOwner = fmt.Sprintf("%s-%d", runnerID(*cfg), os.Getpid())
		}
		runnerInfo := model.RunnerInfo{ID: leaseOwner, Name: cfg.Runner.Name}
		result, key, err := eventcore.RunOnce(cmd.Context(), *cfg, store, eventcore.RunOptions{LeaseOwner: leaseOwner, Lease: cfg.Queue.LeaseDuration.Duration, Workdir: cfg.Workdir.Path, Runner: runnerInfo})
		if err != nil {
			return err
		}
		if result.Claimed == 0 {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "event-run-once: no event claimed")
			return nil
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "event-run-once OK: key=%s finalized=%t\n", key, result.Finalized == 1)
		return nil
	}}
	c.Flags().StringVar(&leaseOwner, "lease-owner", "", "lease owner id")
	return c
}

func runnerID(cfg config.Config) string {
	if cfg.Runner.Name != "" {
		return cfg.Runner.Name
	}
	return "issueq-local"
}
