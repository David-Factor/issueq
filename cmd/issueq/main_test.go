package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
)

func TestRootCommandHelpIncludesPhase0Commands(t *testing.T) {
	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--help"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"--config string",
		"daemon",
		"once",
		"poll",
		"route",
		"dispatch",
		"jobs",
		"issues",
		"doctor",
		"config-check",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("help output missing %q:\n%s", want, out)
		}
	}
}

func TestJobWrapperCommandIsHiddenFromHelp(t *testing.T) {
	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.Contains(buf.String(), "job-wrapper") {
		t.Fatalf("help unexpectedly includes hidden job-wrapper command:\n%s", buf.String())
	}
}

func TestConfigCheckValidConfig(t *testing.T) {
	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", filepath.Join("..", "..", "testdata", "valid-config.yaml"), "config-check"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(buf.String(), "config OK:") {
		t.Fatalf("output = %q, want config OK", buf.String())
	}
}

func TestConfigCheckInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "issueq.yaml")
	if err := os.WriteFile(path, []byte(`
github:
  owner: example-org
queue:
  sqlite:
    path: ./issueq.db
routes:
  - name: triage
    job:
      kind: triage
      command: ["./tasks/triage.sh"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", path, "config-check"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil")
	}
	if !strings.Contains(err.Error(), "github.repo is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestCLIResolvesRelativeDBPathFromConfigDir(t *testing.T) {
	dir := t.TempDir()
	cwd := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	content := `github:
  owner: example-org
  repo: example-repo
queue:
  sqlite:
    path: ./issueq.db
routes:
  - name: triage
    job:
      kind: triage
      command: ["./tasks/triage.sh"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(cwd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldwd) })

	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", configPath, "jobs"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("jobs Execute() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "issueq.db")); err != nil {
		t.Fatalf("config-relative DB missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cwd, "issueq.db")); !os.IsNotExist(err) {
		t.Fatalf("cwd DB existence err = %v, want not exist", err)
	}
}

func TestPollCommandMissingTokenEnvFails(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	writeConfig(t, configPath, dbPath)
	t.Setenv("GITHUB_TOKEN", "")
	_ = os.Unsetenv("GITHUB_TOKEN")

	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", configPath, "poll"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil")
	}
	if !strings.Contains(err.Error(), "environment variable GITHUB_TOKEN named by github.token_env is not set") {
		t.Fatalf("error = %v", err)
	}
}

func TestRouteCommandSeedsJobFromStoredIssue(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	writeConfig(t, configPath, dbPath)

	seed := runCommand(t, "--config", configPath, "config-check")
	if seed != "config OK: "+configPath+"\n" {
		t.Fatalf("config-check output = %q", seed)
	}

	// Seed through the store API to keep Phase 3 free of GitHub/network dependencies.
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertIssue(ctx, model.IssueSnapshot{
		IssueKey:        "github.com/example-org/example-repo#1",
		Host:            "github.com",
		Owner:           "example-org",
		Repo:            "example-repo",
		Number:          1,
		Title:           "Seed issue",
		Labels:          []string{"agent-triage"},
		State:           "open",
		GitHubUpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", configPath, "route"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("route Execute() error = %v", err)
	}
	if !strings.Contains(buf.String(), "created=1") {
		t.Fatalf("route output = %q", buf.String())
	}

	cmd = newRootCommand()
	buf = new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", configPath, "jobs"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("jobs Execute() error = %v", err)
	}
	if !strings.Contains(buf.String(), "triage") || !strings.Contains(buf.String(), "pending") {
		t.Fatalf("jobs output = %q", buf.String())
	}
}

func TestInspectCommandsJSON(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	writeConfig(t, configPath, dbPath)
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	issue := model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "JSON issue", Labels: []string{"agent-triage"}, State: "open"}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: issue.IssueKey, RouteName: "triage", Kind: "triage", DedupeKey: "json"}); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	for _, name := range []string{"jobs", "issues"} {
		cmd := newRootCommand()
		buf := new(bytes.Buffer)
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs([]string{"--config", configPath, name, "--json"})
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		var parsed []map[string]any
		if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
			t.Fatalf("%s output not JSON: %q err=%v", name, buf.String(), err)
		}
		if len(parsed) != 1 {
			t.Fatalf("%s parsed len = %d", name, len(parsed))
		}
	}
}

func TestDispatchCommandRequiresGitHubTokenByDefault(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	writeConfig(t, configPath, dbPath)
	t.Setenv("GITHUB_TOKEN", "")
	_ = os.Unsetenv("GITHUB_TOKEN")

	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", configPath, "dispatch"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil")
	}
	if !strings.Contains(err.Error(), "environment variable GITHUB_TOKEN named by github.token_env is not set") {
		t.Fatalf("error = %v", err)
	}
}

func TestDispatchCommandLocalNoGitHubRequiresConfiguredStore(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	script := filepath.Join(dir, "task.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho cli-dispatch\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeConfigWithCommand(t, configPath, dbPath, script)
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	issue := model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Seed", Labels: []string{"agent-triage"}, State: "open"}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: issue.IssueKey, RouteName: "triage", Kind: "triage", Priority: 1, DedupeKey: "cli-dispatch"}); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", configPath, "dispatch"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil")
	}
	if !strings.Contains(err.Error(), "environment variable GITHUB_TOKEN named by github.token_env is not set") {
		t.Fatalf("error = %v", err)
	}
}

func TestOnceNoWaitUnsupported(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	writeConfig(t, configPath, dbPath)

	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", configPath, "once", "--no-wait"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil")
	}
	if !strings.Contains(err.Error(), "once --no-wait is not supported") {
		t.Fatalf("error = %v", err)
	}
}

func TestJobsAndIssuesCommandsWorkOnEmptyDB(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	writeConfig(t, configPath, dbPath)

	for _, name := range []string{"jobs", "issues"} {
		t.Run(name, func(t *testing.T) {
			cmd := newRootCommand()
			buf := new(bytes.Buffer)
			cmd.SetOut(buf)
			cmd.SetErr(buf)
			cmd.SetArgs([]string{"--config", configPath, name})

			if err := cmd.Execute(); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if !strings.Contains(buf.String(), "\t") {
				t.Fatalf("output = %q, want table header", buf.String())
			}
		})
	}
}

func writeConfig(t *testing.T, path, dbPath string) {
	t.Helper()
	content := `github:
  owner: example-org
  repo: example-repo
queue:
  sqlite:
    path: ` + dbPath + `
routes:
  - name: triage
    job:
      kind: triage
      command: ["./tasks/triage.sh"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func runCommand(t *testing.T, args ...string) string {
	t.Helper()
	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("issueq %v failed: %v\n%s", args, err, buf.String())
	}
	return buf.String()
}

func writeConfigWithCommand(t *testing.T, path, dbPath, command string) {
	t.Helper()
	content := `runner:
  name: test-runner
  capabilities: [triage]
  env:
    inherit: false
    pass: [PATH, HOME]
github:
  owner: example-org
  repo: example-repo
queue:
  sqlite:
    path: ` + dbPath + `
  max_global_concurrency: 1
  lease_duration: 30m
workdir:
  path: ` + filepath.Join(filepath.Dir(path), ".issueq") + `
routes:
  - name: triage
    job:
      kind: triage
      command: ["` + command + `"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
