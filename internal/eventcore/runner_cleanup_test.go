//go:build !windows

package eventcore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"issueq/internal/model"
	"issueq/internal/store"
	sqlitestore "issueq/internal/store/sqlite"
)

func TestRunOnceCancellationTerminatesCommandProcessGroupAndFinalizes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := sqlitestore.Open(ctx, filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := testConfig(dir)
	agentPath := filepath.Join(dir, "agent.sh")
	pidPath := filepath.Join(dir, "child.pid")
	if err := os.WriteFile(agentPath, []byte(fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
pid_file=%q
ctx=${1:?}
res=${2:?}
sh -c 'trap "" TERM; echo $$ > "$1"; while :; do sleep 0.2; done' child "$pid_file" &
child=$!
wait "$child"
`, pidPath)), 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Routes[0].Job.Command = []string{agentPath}
	cfg.Routes[0].Job.Timeout.Duration = time.Minute
	cfg.Routes[0].Job.MaxAttempts = 1
	if _, _, _, err := Upsert(ctx, cfg, st, sampleUpsert("pr-review")); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, _, err := RunOnce(runCtx, cfg, st, RunOptions{LeaseOwner: "r", Lease: time.Minute, Workdir: dir, Runner: model.RunnerInfo{ID: "r", Name: "runner"}})
		done <- err
	}()

	pid := waitForPIDFile(t, pidPath, time.Second)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunOnce returned error: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("RunOnce did not return after cancellation")
	}
	waitForProcessExit(t, pid, 3*time.Second)

	ev, err := st.GetAutomationEvent(ctx, "pr-review:h/o/r:pr-1:head-abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != model.AutomationEventStatusFailed {
		t.Fatalf("status=%s result=%s", ev.Status, ev.ResultJSON)
	}
	if ev.LeaseOwner != "" || ev.LeaseExpiresAt != nil {
		t.Fatalf("lease not cleared: %#v", ev)
	}
	if !strings.Contains(ev.ResultJSON, "command_failed") {
		t.Fatalf("unexpected result: %s", ev.ResultJSON)
	}
}

func waitForPIDFile(t *testing.T, path string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(b)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for pid file %s", path)
	return 0
}

func waitForProcessExit(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if processExited(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	t.Fatalf("process %d still existed after timeout", pid)
}

func processExited(pid int) bool {
	if data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat")); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) >= 3 && fields[2] == "Z" {
			return true
		}
	}
	err := syscall.Kill(pid, 0)
	return errors.Is(err, syscall.ESRCH)
}

func TestRunOnceCommandTimeoutTerminatesProcessGroupAndFinalizes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := sqlitestore.Open(ctx, filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := testConfig(dir)
	agentPath := filepath.Join(dir, "agent-timeout.sh")
	pidPath := filepath.Join(dir, "timeout-child.pid")
	if err := os.WriteFile(agentPath, []byte(fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
pid_file=%q
sh -c 'echo $$ > "$1"; while :; do sleep 0.2; done' child "$pid_file" &
wait "$!"
`, pidPath)), 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Routes[0].Job.Command = []string{agentPath}
	cfg.Routes[0].Job.Timeout.Duration = 100 * time.Millisecond
	cfg.Routes[0].Job.MaxAttempts = 1
	if _, _, _, err := Upsert(ctx, cfg, st, sampleUpsert("pr-review")); err != nil {
		t.Fatal(err)
	}
	result, _, err := RunOnce(ctx, cfg, st, RunOptions{LeaseOwner: "r", Lease: time.Minute, Workdir: dir, Runner: model.RunnerInfo{ID: "r", Name: "runner"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Finalized != 1 {
		t.Fatalf("result=%#v", result)
	}
	pid := waitForPIDFile(t, pidPath, time.Second)
	waitForProcessExit(t, pid, 3*time.Second)
	ev, err := st.GetAutomationEvent(ctx, "pr-review:h/o/r:pr-1:head-abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != model.AutomationEventStatusFailed {
		t.Fatalf("status=%s result=%s", ev.Status, ev.ResultJSON)
	}
}

func TestFinalizeUsesLiveContextAfterCancelledRunContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := t.TempDir()
	st, err := sqlitestore.Open(context.Background(), filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := testConfig(dir)
	ev, _, _, err := Upsert(context.Background(), cfg, st, sampleUpsert("pr-review"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := st.ClaimAutomationEvent(context.Background(), store.EventClaimOptions{RouteName: "pr-review", LeaseOwner: "r", LeaseDuration: time.Minute, MaxAttempts: 1, Now: time.Now()})
	if err != nil || claimed == nil {
		t.Fatalf("claim %#v %v", claimed, err)
	}
	resultPath := filepath.Join(dir, "result.json")
	if err := os.WriteFile(resultPath, []byte(`{"schema":"issueq-agent-result/v1","event_key":"`+ev.EventKey+`","route":"pr-review","status":"failed","decision":"cancelled","summary_markdown":"cancelled","work_started":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	ok, err := FinalizeFromResult(context.WithoutCancel(ctx), cfg, st, *claimed, cfg.Routes[0], "r", resultPath)
	if err != nil || !ok {
		t.Fatalf("finalize %v %v", ok, err)
	}
}
