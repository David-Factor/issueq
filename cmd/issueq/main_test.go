package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootCommandHelpIncludesEventCommandsOnly(t *testing.T) {
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
		"event",
		"events",
		"project",
		"config-check",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("help output missing %q:\n%s", want, out)
		}
	}
	for _, removed := range []string{"--mode", " legacy", "poll", "route", "dispatch", "jobs", "issues", "job-wrapper"} {
		if strings.Contains(out, removed) {
			t.Fatalf("help output still exposes removed legacy surface %q:\n%s", removed, out)
		}
	}
}

func TestLegacyCommandsAreRemoved(t *testing.T) {
	for _, args := range [][]string{
		{"poll"},
		{"route"},
		{"dispatch"},
		{"jobs"},
		{"issues"},
		{"--mode", "legacy", "daemon"},
		{"once", "--no-wait"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			cmd := newRootCommand()
			buf := new(bytes.Buffer)
			cmd.SetOut(buf)
			cmd.SetErr(buf)
			cmd.SetArgs(args)
			if err := cmd.Execute(); err == nil {
				t.Fatalf("issueq %v unexpectedly succeeded; output=%s", args, buf.String())
			}
		})
	}
}

func TestConfigCheckValidEventConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	writeConfig(t, configPath, dbPath)

	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", configPath, "config-check"})

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
    event_kind: triage
    job:
      kind: event
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
    event_kind: triage
    job:
      kind: event
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
	cmd.SetArgs([]string{"--config", configPath, "events", "list"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("events list Execute() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "issueq.db")); err != nil {
		t.Fatalf("config-relative DB missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cwd, "issueq.db")); !os.IsNotExist(err) {
		t.Fatalf("cwd DB existence err = %v, want not exist", err)
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
    event_kind: triage
    job:
      kind: event
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

func TestEventCreateAndApproveCommands(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	dbPath := filepath.Join(dir, "issueq.db")
	content := `github:
  owner: example-org
  repo: example-repo
queue:
  sqlite:
    path: ` + dbPath + `
routes:
  - name: pr-review
    event_kind: pr-review
    job:
      kind: event
      command: ["/bin/true"]
      timeout: 10m
      concurrency: 1
      max_attempts: 1
      follow_ups:
      - decision: fix_candidate
        kind: pr-fix
        route: pr-fix
  - name: pr-fix
    event_kind: pr-fix
    requires:
      handoff:
        from: pr-review
        decisions: [fix_candidate]
        expected_next: true
    job:
      kind: event
      command: ["/bin/true"]
      timeout: 10m
      concurrency: 1
      max_attempts: 1
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	eventPath := filepath.Join(dir, "event.json")
	if err := os.WriteFile(eventPath, []byte(`{"schema":"issueq-event/v1","kind":"pr-review","repo":{"host":"github.com","owner":"example-org","name":"example-repo"},"target":{"kind":"pull_request","key":"pr-1","fingerprint":"head-a"},"payload":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out := runCommand(t, "--config", configPath, "event", "create", "--json", eventPath)
	if !strings.Contains(out, "event upsert OK") {
		t.Fatalf("create output=%q", out)
	}
	out = runCommand(t, "--config", configPath, "events", "approve", "pr-review:github.com/example-org/example-repo:pr-1:head-a", "--decision", "fix_candidate", "--next-kind", "pr-fix")
	if !strings.Contains(out, "event approved:") || !strings.Contains(out, "pr-fix:github.com/example-org/example-repo:pr-1:head-a") {
		t.Fatalf("approve output=%q", out)
	}
	out = runCommand(t, "--config", configPath, "events", "list")
	if !strings.Contains(out, "pr-fix") {
		t.Fatalf("list output=%q", out)
	}
}
