package main

import (
	"fmt"
	"os"

	"issueq/internal/config"

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
		stubCommand("route", "Route locally stored issues into jobs", &configPath),
		stubCommand("dispatch", "Dispatch eligible queued jobs", &configPath),
		stubCommand("jobs", "List local queued jobs", &configPath),
		stubCommand("issues", "List local issue snapshots", &configPath),
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
