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
	"issueq/internal/eventcore"
	issuegithub "issueq/internal/github"
	"issueq/internal/logging"
	"issueq/internal/model"
	"issueq/internal/projection"
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
	cmd := &cobra.Command{
		Use:           "issueq",
		SilenceUsage:  true,
		SilenceErrors: true,
		Short:         "Local automation event queue runner",
		Long: "issueq runs configured local automation commands from durable issueq-event/v1 rows. " +
			"Bridge issue, label scheduler, and comment-handoff commands have been removed from this hard-cutover build.",
		RunE: func(cmd *cobra.Command, args []string) error { return cmd.Help() },
	}
	cmd.PersistentFlags().StringVar(&configPath, "config", config.DefaultConfigPath, "path to issueq YAML config")
	cmd.AddCommand(
		eventCommand(&configPath),
		eventsCommand(&configPath),
		projectCommand(&configPath),
		eventRunCommand(&configPath),
		daemonCommand(&configPath),
		onceCommand(&configPath),
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

func daemonCommand(configPath *string) *cobra.Command {
	return &cobra.Command{Use: "daemon", Short: "Run the long-lived issueq event daemon", RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
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
	}}
}

func onceCommand(configPath *string) *cobra.Command {
	return &cobra.Command{Use: "once", Short: "Run one event claim/run/finalize cycle", RunE: func(cmd *cobra.Command, args []string) error {
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
	}}
}

func openGitHubStore(ctx context.Context, configPath string) (*config.Config, *sqlitestore.Store, *issuegithub.RESTClient, error) {
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
	runUpsert := func(cmd *cobra.Command, args []string) error {
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
	}
	create := &cobra.Command{Use: "create", Short: "Create or upsert an automation event", RunE: runUpsert}
	create.Flags().StringVar(&jsonPath, "json", "-", "event JSON file path, or -/empty for stdin")
	upsert := &cobra.Command{Use: "upsert", Short: "Upsert an automation event", RunE: runUpsert}
	upsert.Flags().StringVar(&jsonPath, "json", "-", "event JSON file path, or -/empty for stdin")
	cmd.AddCommand(create, upsert)
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
	retry := &cobra.Command{Use: "retry <event-key>", Args: cobra.ExactArgs(1), Short: "Retry failed/cancelled/blocked automation event", RunE: func(cmd *cobra.Command, args []string) error {
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
	var decision, nextKind string
	approve := &cobra.Command{Use: "approve <event-key>", Args: cobra.ExactArgs(1), Short: "Store a trusted approval handoff and create a policy-allowed next event", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, store, err := openConfiguredStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		res, err := eventcore.Approve(cmd.Context(), *cfg, store, args[0], eventcore.ApprovalInput{Decision: decision, NextKind: nextKind})
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "event approved: source=%s handoff=%s next=%s\n", args[0], res.Handoff.ID, res.Event.EventKey)
		return nil
	}}
	approve.Flags().StringVar(&decision, "decision", "", "trusted approval decision")
	approve.Flags().StringVar(&nextKind, "next-kind", "", "policy-allowed next event kind")
	_ = approve.MarkFlagRequired("decision")
	_ = approve.MarkFlagRequired("next-kind")
	cmd.AddCommand(list, show, cancel, retry, approve)
	return cmd
}

func projectCommand(configPath *string) *cobra.Command {
	return &cobra.Command{Use: "project <event-key>", Args: cobra.ExactArgs(1), Short: "Project event state to GitHub managed comment/optional UI labels", RunE: func(cmd *cobra.Command, args []string) error {
		_, store, gh, err := openGitHubStore(cmd.Context(), *configPath)
		if err != nil {
			return err
		}
		defer store.Close()
		ev, err := store.GetAutomationEvent(cmd.Context(), args[0])
		if err != nil {
			return err
		}
		res, err := projection.ProjectEvent(cmd.Context(), gh, ev)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "projection OK: event_key=%s target=%d created=%t updated=%t\n", ev.EventKey, res.TargetNumber, res.Created, res.Updated)
		return nil
	}}
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
