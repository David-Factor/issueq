package main

import (
	"context"
	"fmt"
	"os"

	"issueq/internal/config"
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
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.PersistentFlags().StringVar(&configPath, "config", config.DefaultConfigPath, "path to issueq YAML config")

	cmd.AddCommand(
		stubCommand("daemon", "Run the long-lived issueq daemon", &configPath),
		stubCommand("once", "Run one poll-route-dispatch reconciliation cycle", &configPath),
		stubCommand("poll", "Poll GitHub issues into the local store", &configPath),
		routeCommand(&configPath),
		stubCommand("dispatch", "Dispatch eligible queued jobs", &configPath),
		jobsCommand(&configPath),
		issuesCommand(&configPath),
		configCheckCommand("config-check", "Validate issueq configuration", &configPath),
		configCheckCommand("doctor", "Check local issueq setup", &configPath),
	)

	return cmd
}

func stubCommand(use, short string, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s is not implemented yet (config: %s)\n", use, *configPath)
			return nil
		},
	}
}

func configCheckCommand(use, short string, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := config.LoadFile(*configPath); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "config OK: %s\n", *configPath)
			return nil
		},
	}
}

func routeCommand(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "route",
		Short: "Route locally stored issues into jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
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
		},
	}
}

func jobsCommand(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "jobs",
		Short: "List local queued jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, store, err := openConfiguredStore(cmd.Context(), *configPath)
			if err != nil {
				return err
			}
			defer store.Close()

			jobs, err := store.ListJobs(cmd.Context())
			if err != nil {
				return err
			}
			_ = cfg
			for _, job := range jobs {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\t%d\n", job.ID, job.Status, job.RouteName, job.Kind, job.Priority)
			}
			return nil
		},
	}
}

func issuesCommand(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "issues",
		Short: "List local issue snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, store, err := openConfiguredStore(cmd.Context(), *configPath)
			if err != nil {
				return err
			}
			defer store.Close()

			issues, err := store.ListIssues(cmd.Context())
			if err != nil {
				return err
			}
			for _, issue := range issues {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", issue.IssueKey, issue.State, issue.Title)
			}
			return nil
		},
	}
}

func openConfiguredStore(ctx context.Context, configPath string) (*config.Config, *sqlitestore.Store, error) {
	cfg, err := config.LoadFile(configPath)
	if err != nil {
		return nil, nil, err
	}
	store, err := sqlitestore.Open(ctx, cfg.Queue.SQLite.Path)
	if err != nil {
		return nil, nil, err
	}
	return cfg, store, nil
}
