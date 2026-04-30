package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"issueq/internal/config"
	"issueq/internal/daemon"
	"issueq/internal/dispatcher"
	issuegithub "issueq/internal/github"
	"issueq/internal/logging"
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

	cmd := &cobra.Command{
		Use:           "issueq",
		SilenceUsage:  true,
		SilenceErrors: true,
		Short:         "Local GitHub issue automation queue runner",
		Long: "issueq polls GitHub issues, routes matching labels into a local SQLite queue, " +
			"and dispatches bounded subprocess jobs.",
		RunE: func(cmd *cobra.Command, args []string) error { return cmd.Help() },
	}
	cmd.PersistentFlags().StringVar(&configPath, "config", config.DefaultConfigPath, "path to issueq YAML config")
	cmd.AddCommand(
		daemonCommand(&configPath),
		onceCommand(&configPath),
		pollCommand(&configPath),
		routeCommand(&configPath),
		dispatchCommand(&configPath),
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

func daemonCommand(configPath *string) *cobra.Command {
	return &cobra.Command{Use: "daemon", Short: "Run the long-lived issueq daemon", RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
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
	}}
}

func onceCommand(configPath *string) *cobra.Command {
	var noWait bool
	c := &cobra.Command{Use: "once", Short: "Run one poll-route-dispatch reconciliation cycle", RunE: func(cmd *cobra.Command, args []string) error {
		if noWait {
			return fmt.Errorf("once --no-wait is not supported until background child supervision is implemented")
		}
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
		if _, err := store.ReleaseExpiredLeases(cmd.Context(), time.Now().UTC()); err != nil {
			return err
		}
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
