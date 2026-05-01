package jobwrapper

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"issueq/internal/runner"
	"issueq/internal/supervisor"
)

func TestRunSuccessWritesMetadataAndCapturesOutput(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "task.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho hello\necho err >&2\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec := testSpec(t, dir, []string{script})
	metadata, err := Run(context.Background(), spec, Options{})
	if err != nil {
		t.Fatalf("Run error = %v metadata=%#v", err, metadata)
	}
	loaded, err := LoadMetadata(spec.MetadataPath)
	if err != nil {
		t.Fatalf("LoadMetadata error = %v", err)
	}
	if loaded.JobID != spec.JobID || loaded.LaunchToken != spec.LaunchToken || loaded.ExitCode != 0 || loaded.PID == 0 || loaded.StartedAt.IsZero() || loaded.FinishedAt.IsZero() {
		t.Fatalf("metadata = %#v", loaded)
	}
	assertContains(t, spec.StdoutPath, "hello")
	assertContains(t, spec.StderrPath, "err")
	obs := ObservationFromMetadata(loaded, spec.JobID, spec.LaunchToken)
	if obs.State != supervisor.RunExited || !obs.HasExitCode || obs.ExitCode != 0 {
		t.Fatalf("obs = %#v", obs)
	}
}

func TestRunFailureTimeoutCancelAndValidationMetadata(t *testing.T) {
	dir := t.TempDir()
	failure := filepath.Join(dir, "failure.sh")
	if err := os.WriteFile(failure, []byte("#!/bin/sh\necho fail >&2\nexit 7\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec := testSpec(t, dir, []string{failure})
	metadata, err := Run(context.Background(), spec, Options{})
	if err == nil || metadata.ExitCode != 7 || metadata.Error == "" {
		t.Fatalf("failure metadata=%#v err=%v", metadata, err)
	}

	timeout := filepath.Join(dir, "timeout.sh")
	if err := os.WriteFile(timeout, []byte("#!/bin/sh\nsleep 2\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec = testSpec(t, dir, []string{timeout})
	spec.TimeoutSeconds = 1
	metadata, err = Run(context.Background(), spec, Options{})
	if err == nil || !metadata.TimedOut || metadata.Cancelled || !strings.Contains(metadata.Error, "timed out") {
		t.Fatalf("timeout metadata=%#v err=%v", metadata, err)
	}

	cancelScript := filepath.Join(dir, "cancel.sh")
	if err := os.WriteFile(cancelScript, []byte("#!/bin/sh\nsleep 2\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec = testSpec(t, dir, []string{cancelScript})
	cancelCh := make(chan os.Signal, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancelCh <- os.Interrupt
	}()
	metadata, err = Run(context.Background(), spec, Options{Cancel: cancelCh})
	if err == nil || !metadata.Cancelled || metadata.TimedOut {
		t.Fatalf("cancel metadata=%#v err=%v", metadata, err)
	}

	spec = testSpec(t, dir, []string{failure})
	if err := os.WriteFile(spec.ContextPath, []byte(`{"job":{"id":"other"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	metadata, err = Run(context.Background(), spec, Options{})
	if err == nil || !strings.Contains(metadata.Error, "does not match") {
		t.Fatalf("validation metadata=%#v err=%v", metadata, err)
	}
}

func TestSpecAndMetadataValidation(t *testing.T) {
	dir := t.TempDir()
	spec := testSpec(t, dir, []string{"/bin/true"})
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	specPath := filepath.Join(dir, "spec.json")
	if err := os.WriteFile(specPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSpec(specPath)
	if err != nil {
		t.Fatalf("LoadSpec error = %v", err)
	}
	if loaded.JobID != spec.JobID || loaded.Command[0] != spec.Command[0] {
		t.Fatalf("loaded = %#v", loaded)
	}
	bad := Metadata{Version: SpecVersion, JobID: "other", LaunchToken: spec.LaunchToken}
	obs := ObservationFromMetadata(bad, spec.JobID, spec.LaunchToken)
	if obs.State != supervisor.RunUnknown {
		t.Fatalf("mismatch obs = %#v", obs)
	}
}

func testSpec(t *testing.T, dir string, command []string) Spec {
	t.Helper()
	paths := runner.PreparePaths(dir, "job_1")
	ctxData := runner.Context{Job: runner.JobContext{ID: "job_1", Route: "code", Kind: "code", Attempt: 1, MaxAttempts: 3}}
	if err := runner.WriteContext(paths, ctxData); err != nil {
		t.Fatal(err)
	}
	return Spec{
		Version:        SpecVersion,
		JobID:          "job_1",
		LaunchToken:    "launch-token",
		Command:        command,
		Env:            os.Environ(),
		Workdir:        dir,
		ContextPath:    paths.ContextPath,
		ResultPath:     paths.ResultPath,
		StdoutPath:     paths.StdoutPath,
		StderrPath:     paths.StderrPath,
		MetadataPath:   filepath.Join(paths.Dir, "run.json"),
		TimeoutSeconds: 5,
	}
}

func assertContains(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), want) {
		t.Fatalf("%s = %q, want substring %q", path, data, want)
	}
}
