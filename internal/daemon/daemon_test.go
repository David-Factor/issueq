package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
	"issueq/internal/supervisor"
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
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: 20 * time.Millisecond}
	cancel()
	if err := Run(ctx, cfg, store, nil, nil); err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunLaunchesPendingJobThroughWrapperSupervisor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe"}); err != nil {
		t.Fatal(err)
	}
	backend := newFakeDaemonSupervisor()
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool { return backend.launchCount() == 1 })
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, job := range jobs {
			if job.DedupeKey == "dedupe" && job.SupervisorKind == supervisor.KindWrapper && job.LaunchState == model.LaunchStateRunning {
				return true
			}
		}
		return false
	})
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunReapsCompletedObservationAndRefills(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	cfg.Routes[0].When.LabelsInclude = []string{"no-route"}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 2, DedupeKey: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "second"}); err != nil {
		t.Fatal(err)
	}
	backend := newFakeDaemonSupervisor()
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool { return backend.launchCount() == 1 })
	var first string
	waitFor(t, time.Second, func() bool {
		first = backend.firstJobID()
		return first != ""
	})
	backend.complete(first, supervisor.RunExited)
	waitFor(t, 2*time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		var firstSucceeded bool
		for _, job := range jobs {
			if job.ID == first && job.Status == model.JobStatusSucceeded {
				firstSucceeded = true
			}
		}
		return firstSucceeded && backend.launchCount() == 2
	})
	var second string
	waitFor(t, time.Second, func() bool {
		second = backend.lastJobID()
		return second != "" && second != first
	})
	backend.complete(second, supervisor.RunExited)
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		seen := 0
		for _, job := range jobs {
			if job.DedupeKey != "first" && job.DedupeKey != "second" {
				continue
			}
			seen++
			if job.Status != model.JobStatusSucceeded {
				return false
			}
		}
		return seen == 2
	})
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunRenewsRunningWrapperRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe"}); err != nil {
		t.Fatal(err)
	}
	backend := newFakeDaemonSupervisor()
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool { return backend.launchCount() == 1 })
	var firstLease time.Time
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		if len(jobs) == 0 || jobs[0].LeaseUntil == nil {
			return false
		}
		firstLease = *jobs[0].LeaseUntil
		return true
	})
	waitFor(t, 2*time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		return len(jobs) > 0 && jobs[0].LeaseUntil != nil && jobs[0].LeaseUntil.After(firstLease)
	})
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunShutdownCancelsWrapperJobFinalizesCancelledAndDeletesHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe"}); err != nil {
		t.Fatal(err)
	}
	backend := newFakeDaemonSupervisor()
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, job := range jobs {
			if job.DedupeKey == "dedupe" && job.LaunchState == model.LaunchStateRunning {
				return true
			}
		}
		return false
	})
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
	if backend.cancelCount() != 1 {
		t.Fatalf("cancel count = %d, want 1", backend.cancelCount())
	}
	jobs, _ := store.ListJobs(context.Background())
	var cancelled bool
	for _, job := range jobs {
		if job.DedupeKey == "dedupe" {
			cancelled = job.Status == model.JobStatusCancelled && job.LastError != ""
		}
	}
	if !cancelled {
		t.Fatalf("jobs = %#v", jobs)
	}
	if got := countHeartbeats(t, cfg); got != 0 {
		t.Fatalf("heartbeats = %d, want 0", got)
	}
}

func TestRunShutdownFinalizesPreparingRowsAndDeletesHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: time.Second}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "preparing"}); err != nil {
		t.Fatal(err)
	}
	owner := model.RunnerIdentity{RunnerID: "runner", InstanceID: "runner-preparing"}
	claimed, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"code": 1}, time.Second)
	if err != nil || claimed == nil {
		t.Fatalf("claim preparing=%#v err=%v", claimed, err)
	}
	loop := newLoop(cfg, store, nil, nil, newFakeDaemonSupervisor())
	loop.identity = owner
	loop.runnerInfo = model.RunnerInfo{ID: owner.RunnerID}
	if err := loop.heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if err := loop.shutdown(context.Canceled); err != nil {
		t.Fatalf("shutdown error = %v", err)
	}
	loop.deleteHeartbeatAfterCleanShutdown()
	jobs, _ := store.ListJobs(context.Background())
	if jobs[0].Status != model.JobStatusCancelled {
		t.Fatalf("jobs = %#v", jobs)
	}
	if got := countHeartbeats(t, cfg); got != 0 {
		t.Fatalf("heartbeats = %d, want 0", got)
	}
}

func TestRunExpiredNonDurableReleasedButDurableForeignNotRequeued(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 80 * time.Millisecond}
	old := model.RunnerIdentity{RunnerID: "old", InstanceID: "old-instance"}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 2, DedupeKey: "nondurable"}); err != nil {
		t.Fatal(err)
	}
	nondurable, err := store.ClaimNextJob(ctx, old, []string{"code"}, 10, map[string]int{"code": 10}, 50*time.Millisecond)
	if err != nil || nondurable == nil {
		t.Fatalf("claim nondurable=%#v err=%v", nondurable, err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "durable"}); err != nil {
		t.Fatal(err)
	}
	durable, err := store.ClaimNextJob(ctx, old, []string{"code"}, 10, map[string]int{"code": 10}, 500*time.Millisecond)
	if err != nil || durable == nil {
		t.Fatalf("claim durable=%#v err=%v", durable, err)
	}
	timeoutAt := time.Now().Add(time.Minute)
	if err := store.PersistLaunchSpecOwned(ctx, durable.ID, old.InstanceID, model.LaunchSpecRecord{SupervisorKind: supervisor.KindWrapper, LaunchToken: "tok", LaunchSpecPath: "spec", RunMetadataPath: "run", TimeoutAt: timeoutAt}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobLaunchingOwned(ctx, durable.ID, old.InstanceID, "tok"); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistLaunchRecordOwned(ctx, durable.ID, old.InstanceID, model.LaunchRecord{SupervisorKind: supervisor.KindWrapper, SupervisorID: "pid-1", LaunchToken: "tok", PID: 1, RunMetadataPath: "run", TimeoutAt: timeoutAt}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	backend := newFakeDaemonSupervisor()
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		var nonPending, durableRunning bool
		for _, job := range jobs {
			if job.ID == nondurable.ID && job.Status == model.JobStatusPending {
				nonPending = true
			}
			if job.ID == durable.ID && job.Status == model.JobStatusRunning && job.RunnerInstanceID == old.InstanceID {
				durableRunning = true
			}
		}
		return nonPending && durableRunning
	})
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunPrunesStaleHeartbeats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 50 * time.Millisecond}
	stale := model.RunnerIdentity{RunnerID: "old", InstanceID: "old-instance"}
	if err := store.HeartbeatRunner(ctx, stale, 99, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, store, nil, nil) }()
	waitFor(t, time.Second, func() bool { return oneHeartbeatAndNoStale(t, cfg) })
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
	if got := staleHeartbeats(t, cfg); got != 0 {
		t.Fatalf("stale heartbeats = %d, want 0", got)
	}
}

func setupOnce(t *testing.T) (*sqlitestore.Store, config.Config) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "issueq.db")
	store, err := sqlitestore.Open(ctx, dbPath)
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
		Queue:   config.QueueConfig{SQLite: config.SQLiteConfig{Path: dbPath}, MaxGlobalConcurrency: 1, LeaseDuration: config.Duration{Duration: time.Minute}},
		Workdir: config.WorkdirConfig{Path: filepath.Join(t.TempDir(), ".issueq")},
		Polling: config.PollingConfig{Interval: config.Duration{Duration: time.Minute}},
		GitHub:  config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo", TokenEnv: "GITHUB_TOKEN"},
		Routes:  []config.RouteConfig{{Name: "code", When: config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}, Job: config.JobConfig{Kind: "code", Command: []string{script}, Timeout: config.Duration{Duration: time.Second}, Concurrency: 1, MaxAttempts: 3}}},
	}
	return store, cfg
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func oneHeartbeatAndNoStale(t *testing.T, cfg config.Config) bool {
	t.Helper()
	db, err := sql.Open("sqlite", cfg.Queue.SQLite.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM runner_heartbeats WHERE runner_instance_id != 'old-instance'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	var stale int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM runner_heartbeats WHERE runner_instance_id = 'old-instance'`).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	return count == 1 && stale == 0
}

func staleHeartbeats(t *testing.T, cfg config.Config) int {
	t.Helper()
	db, err := sql.Open("sqlite", cfg.Queue.SQLite.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM runner_heartbeats WHERE runner_instance_id = 'old-instance'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func countHeartbeats(t *testing.T, cfg config.Config) int {
	t.Helper()
	db, err := sql.Open("sqlite", cfg.Queue.SQLite.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM runner_heartbeats`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type fakeDaemonSupervisor struct {
	mu            sync.Mutex
	launches      []supervisor.LaunchSpec
	observations  map[string]supervisor.Observation
	cancellations []supervisor.FakeCancellation
}

func newFakeDaemonSupervisor() *fakeDaemonSupervisor {
	return &fakeDaemonSupervisor{observations: map[string]supervisor.Observation{}}
}

func (f *fakeDaemonSupervisor) Launch(ctx context.Context, spec supervisor.LaunchSpec) (supervisor.LaunchRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.launches = append(f.launches, spec)
	record := supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: spec.JobID + "-wrapper", JobID: spec.JobID, LaunchToken: spec.LaunchToken, PID: 1000 + len(f.launches), MetadataPath: spec.MetadataPath, StartedAt: time.Now().UTC(), TimeoutAt: time.Now().UTC().Add(spec.Timeout)}
	f.observations[spec.JobID] = supervisor.Observation{State: supervisor.RunRunning, StartedAt: record.StartedAt, ResultPath: spec.ResultPath, StdoutPath: spec.StdoutPath, StderrPath: spec.StderrPath}
	return record, nil
}

func (f *fakeDaemonSupervisor) Inspect(ctx context.Context, record supervisor.LaunchRecord) (supervisor.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obs, ok := f.observations[record.JobID]
	if !ok {
		return supervisor.Observation{State: supervisor.RunUnknown, Error: "missing fake observation"}, nil
	}
	if obs.ResultPath == "" {
		obs.ResultPath = record.MetadataPath
	}
	return obs, nil
}

func (f *fakeDaemonSupervisor) Cancel(ctx context.Context, record supervisor.LaunchRecord, reason supervisor.CancelReason) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancellations = append(f.cancellations, supervisor.FakeCancellation{Record: record, Reason: reason})
	f.observations[record.JobID] = supervisor.Observation{State: supervisor.RunCancelled, Error: "runner shutting down", FinishedAt: time.Now().UTC(), ResultPath: record.MetadataPath}
	return nil
}

func (f *fakeDaemonSupervisor) launchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.launches)
}
func (f *fakeDaemonSupervisor) cancelCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cancellations)
}
func (f *fakeDaemonSupervisor) firstJobID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.launches) == 0 {
		return ""
	}
	return f.launches[0].JobID
}
func (f *fakeDaemonSupervisor) lastJobID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.launches) == 0 {
		return ""
	}
	return f.launches[len(f.launches)-1].JobID
}
func (f *fakeDaemonSupervisor) complete(jobID string, state supervisor.RunState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obs := f.observations[jobID]
	obs.State = state
	obs.FinishedAt = time.Now().UTC()
	obs.HasExitCode = true
	obs.ExitCode = 0
	f.observations[jobID] = obs
}
