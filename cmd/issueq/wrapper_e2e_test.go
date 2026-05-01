package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
)

func TestDispatchLocalNoGitHubRealWrapperSmoke(t *testing.T) {
	bin := buildIssueqBinary(t)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	script := filepath.Join(dir, "task.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho wrapper-smoke\nprintf '{\"comment\":\"ok\"}' > \"$2\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeConfigWithCommand(t, configPath, dbPath, script)
	seedCLIJob(t, dbPath, "wrapper-smoke")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", configPath, "dispatch", "--local-no-github")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("dispatch error = %v output=%s", err, out.String())
	}
	if !strings.Contains(out.String(), "succeeded=1") {
		t.Fatalf("dispatch output = %q", out.String())
	}
	store, err := sqlitestore.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	jobs, err := store.ListJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.JobStatusSucceeded || jobs[0].SupervisorKind != "" {
		t.Fatalf("jobs = %#v", jobs)
	}
	stdout, err := os.ReadFile(jobs[0].StdoutPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stdout), "wrapper-smoke") {
		t.Fatalf("stdout = %q", stdout)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(jobs[0].ContextPath), filepath.Base(jobs[0].ContextPath))); err != nil {
		t.Fatal(err)
	}
}

func TestProductionDepsExcludeAttachedSupervisor(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "issueq/cmd/issueq", "issueq/internal/daemon", "issueq/internal/dispatcher")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list error = %v output=%s", err, out)
	}
	if strings.Contains(string(out), "issueq/internal/supervisor/attached") {
		t.Fatalf("production deps include attached supervisor:\n%s", out)
	}
}

func buildIssueqBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "issueq")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", bin, "issueq/cmd/issueq")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build error = %v output=%s", err, out)
	}
	return bin
}

func seedCLIJob(t *testing.T, dbPath, dedupe string) {
	t.Helper()
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	issue := model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Seed", Labels: []string{"agent-triage"}, State: "open"}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: issue.IssueKey, RouteName: "triage", Kind: "triage", Priority: 1, DedupeKey: dedupe}); err != nil {
		t.Fatal(err)
	}
}
