package dispatcher

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
)

func TestDispatchRunsLocalFixtureJobEndToEnd(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho hello\necho err >&2\nexit 0\n")
	defer store.Close()
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatalf("Dispatch error = %v", err)
	}
	if result.Claimed != 1 || result.Succeeded != 1 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job := jobs[0]
	if job.Status != model.JobStatusSucceeded || job.StdoutPath == "" || job.StderrPath == "" || job.ContextPath == "" {
		t.Fatalf("job = %#v", job)
	}
	assertContains(t, job.StdoutPath, "hello")
	assertContains(t, job.StderrPath, "err")
}

func TestDispatchMarksFailure(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho fail >&2\nexit 1\n")
	defer store.Close()
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusFailed || jobs[0].LastError == "" {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchTimeout(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\nsleep 2\n")
	defer store.Close()
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: 20 * time.Millisecond}
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusFailed || !strings.Contains(jobs[0].LastError, "timed out") {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func setupDispatch(t *testing.T, scriptBody string) (*sqlitestore.Store, config.Config) {
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
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: issue.IssueKey, RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe"}); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "task.sh")
	if err := os.WriteFile(script, []byte(scriptBody), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Runner:  config.RunnerConfig{Name: "runner", Capabilities: []string{"code"}, Env: config.EnvConfig{Pass: []string{"PATH", "HOME"}}},
		Queue:   config.QueueConfig{MaxGlobalConcurrency: 1, LeaseDuration: config.Duration{Duration: time.Minute}},
		Workdir: config.WorkdirConfig{Path: filepath.Join(t.TempDir(), ".issueq")},
		GitHub:  config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo", TokenEnv: "GITHUB_TOKEN"},
		Routes:  []config.RouteConfig{{Name: "code", Job: config.JobConfig{Kind: "code", Command: []string{script}, Timeout: config.Duration{Duration: time.Second}, Concurrency: 1, MaxAttempts: 3}}},
	}
	return store, cfg
}

func assertContains(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), want) {
		t.Fatalf("%s = %q, want %q", path, data, want)
	}
}
