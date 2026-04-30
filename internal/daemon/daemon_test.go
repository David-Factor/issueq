package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
)

func TestOnceWaitsAndProcessesLocalRouteDispatch(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupOnce(t)
	defer store.Close()
	result, err := Once(ctx, cfg, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Route.JobsCreated != 1 || result.Dispatch.Succeeded != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusSucceeded {
		t.Fatalf("jobs = %#v", jobs)
	}
}

func TestRunStopsCleanlyOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: 20 * time.Millisecond}
	cancel()
	if err := Run(ctx, cfg, store, nil, nil); err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func setupOnce(t *testing.T) (*sqlitestore.Store, config.Config) {
	t.Helper()
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "issueq.db"))
	if err != nil {
		t.Fatal(err)
	}
	issue := model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "task.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Runner:  config.RunnerConfig{Name: "runner", Capabilities: []string{"code"}, Env: config.EnvConfig{Pass: []string{"PATH", "HOME"}}},
		Queue:   config.QueueConfig{MaxGlobalConcurrency: 1, LeaseDuration: config.Duration{Duration: time.Minute}},
		Workdir: config.WorkdirConfig{Path: filepath.Join(t.TempDir(), ".issueq")},
		Polling: config.PollingConfig{Interval: config.Duration{Duration: time.Minute}},
		GitHub:  config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo", TokenEnv: "GITHUB_TOKEN"},
		Routes:  []config.RouteConfig{{Name: "code", When: config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}, Job: config.JobConfig{Kind: "code", Command: []string{script}, Timeout: config.Duration{Duration: time.Second}, Concurrency: 1, MaxAttempts: 3}}},
	}
	return store, cfg
}
