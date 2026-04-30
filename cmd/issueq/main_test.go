package main

import (
	"bytes"
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
