package wrapper

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"issueq/internal/jobwrapper"
	"issueq/internal/runner"
	"issueq/internal/supervisor"
)

func TestWrapperSupervisorLaunchInspectSuccessAndStaleMetadata(t *testing.T) {
	bin := buildIssueq(t)
	dir := t.TempDir()
	task := filepath.Join(dir, "task.sh")
	if err := os.WriteFile(task, []byte("#!/bin/sh\necho hello\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec := testLaunchSpec(t, dir, []string{task}, "tok-success")
	backend := New(bin)
	record, err := backend.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch error = %v", err)
	}
	if record.Kind != supervisor.KindWrapper || record.PID == 0 || record.LaunchToken != spec.LaunchToken || record.MetadataPath != spec.MetadataPath {
		t.Fatalf("record = %#v", record)
	}
	waitForObservation(t, backend, record, supervisor.RunExited)
	metadata, err := jobwrapper.LoadMetadata(spec.MetadataPath)
	if err != nil {
		t.Fatalf("LoadMetadata error = %v", err)
	}
	if metadata.ExitCode != 0 || metadata.JobID != spec.JobID || metadata.LaunchToken != spec.LaunchToken {
		t.Fatalf("metadata = %#v", metadata)
	}
	stale := metadata
	stale.LaunchToken = "old"
	stalePath := filepath.Join(dir, "stale-run.json")
	if err := jobwrapper.WriteMetadataAtomic(stalePath, stale); err != nil {
		t.Fatal(err)
	}
	obs, err := backend.Inspect(context.Background(), supervisor.LaunchRecord{Kind: supervisor.KindWrapper, JobID: spec.JobID, LaunchToken: spec.LaunchToken, MetadataPath: stalePath})
	if err != nil {
		t.Fatal(err)
	}
	if obs.State != supervisor.RunUnknown {
		t.Fatalf("stale metadata obs = %#v", obs)
	}
}

func TestWrapperSupervisorCancelAndMissingMetadataUnknown(t *testing.T) {
	bin := buildIssueq(t)
	dir := t.TempDir()
	task := filepath.Join(dir, "sleep.sh")
	if err := os.WriteFile(task, []byte("#!/bin/sh\necho $$ > child.pid\nsleep 5\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec := testLaunchSpec(t, dir, []string{task}, "tok-cancel")
	backend := New(bin)
	record, err := backend.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch error = %v", err)
	}
	waitForState(t, backend, record, supervisor.RunRunning)
	if err := backend.Cancel(context.Background(), record, supervisor.CancelOperator); err != nil {
		t.Fatalf("Cancel error = %v", err)
	}
	waitForObservation(t, backend, record, supervisor.RunUnknown)
	if pidFromFile(t, filepath.Join(dir, "child.pid")) > 0 {
		waitForProcessExit(t, pidFromFile(t, filepath.Join(dir, "child.pid")))
	}

	obs, err := backend.Inspect(context.Background(), supervisor.LaunchRecord{Kind: supervisor.KindWrapper, JobID: spec.JobID, LaunchToken: "missing", PID: -1, TimeoutAt: time.Now().Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if obs.State != supervisor.RunUnknown {
		t.Fatalf("missing metadata obs = %#v", obs)
	}
}

func TestWrapperSupervisorTimeout(t *testing.T) {
	bin := buildIssueq(t)
	dir := t.TempDir()
	task := filepath.Join(dir, "timeout.sh")
	if err := os.WriteFile(task, []byte("#!/bin/sh\nsleep 5\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec := testLaunchSpec(t, dir, []string{task}, "tok-timeout")
	spec.Timeout = time.Second
	backend := New(bin)
	record, err := backend.Launch(context.Background(), spec)
	if err != nil {
		t.Fatalf("Launch error = %v", err)
	}
	waitForObservation(t, backend, record, supervisor.RunTimedOut)
}

func testLaunchSpec(t *testing.T, dir string, command []string, token string) supervisor.LaunchSpec {
	t.Helper()
	paths := runner.PreparePaths(dir, "job_1")
	ctxData := runner.Context{Job: runner.JobContext{ID: "job_1", Route: "code", Kind: "code", Attempt: 1, MaxAttempts: 3}}
	if err := runner.WriteContext(paths, ctxData); err != nil {
		t.Fatal(err)
	}
	return supervisor.LaunchSpec{JobID: "job_1", LaunchToken: token, Command: command, Env: os.Environ(), Workdir: dir, ContextPath: paths.ContextPath, ResultPath: paths.ResultPath, StdoutPath: paths.StdoutPath, StderrPath: paths.StderrPath, MetadataPath: filepath.Join(paths.Dir, token+"-run.json"), SpecPath: filepath.Join(paths.Dir, token+"-spec.json"), Timeout: 5 * time.Second}
}

func buildIssueq(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "issueq-test")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/issueq")
	cmd.Dir = filepath.Join("..", "..", "..")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build issueq: %v\n%s", err, out)
	}
	return bin
}

func waitForObservation(t *testing.T, backend *Supervisor, record supervisor.LaunchRecord, want supervisor.RunState) supervisor.Observation {
	t.Helper()
	deadline := time.After(8 * time.Second)
	for {
		obs, err := backend.Inspect(context.Background(), record)
		if err != nil {
			t.Fatalf("Inspect error = %v", err)
		}
		if obs.State == want {
			return obs
		}
		if obs.State != supervisor.RunStarting && obs.State != supervisor.RunRunning {
			t.Fatalf("obs = %#v, want eventually %s", obs, want)
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func pidFromFile(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return pid
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for processExists(pid) {
		select {
		case <-deadline:
			t.Fatalf("process %d still exists", pid)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func waitForState(t *testing.T, backend *Supervisor, record supervisor.LaunchRecord, want supervisor.RunState) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		obs, err := backend.Inspect(context.Background(), record)
		if err != nil {
			t.Fatalf("Inspect error = %v", err)
		}
		if obs.State == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s; last obs=%#v", want, obs)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestWriteSpecAtomic(t *testing.T) {
	dir := t.TempDir()
	spec := jobwrapper.Spec{Version: jobwrapper.SpecVersion, JobID: "job", LaunchToken: "tok", Command: []string{"/bin/true"}, ContextPath: filepath.Join(dir, "context"), ResultPath: filepath.Join(dir, "result"), StdoutPath: filepath.Join(dir, "out"), StderrPath: filepath.Join(dir, "err"), MetadataPath: filepath.Join(dir, "run"), TimeoutSeconds: 1}
	path := filepath.Join(dir, "spec.json")
	if err := writeSpecAtomic(path, spec); err != nil {
		t.Fatalf("writeSpecAtomic error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got jobwrapper.Spec
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.JobID != spec.JobID || got.LaunchToken != spec.LaunchToken {
		t.Fatalf("got = %#v", got)
	}
}
