package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestStubCommandsAcceptConfigFlag(t *testing.T) {
	cmd := newRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--config", "custom.yaml", "jobs"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	want := "jobs is not implemented yet (config: custom.yaml)"
	if !strings.Contains(buf.String(), want) {
		t.Fatalf("output missing %q:\n%s", want, buf.String())
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
