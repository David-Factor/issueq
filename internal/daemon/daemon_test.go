package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/runner"
	sqlitestore "issueq/internal/store/sqlite"
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

func TestRunPollsRoutesWhileLongJobActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: 25 * time.Millisecond}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 80 * time.Millisecond}
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: time.Second}

	proc := newFakeProcess(3101)
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe"}); err != nil {
		t.Fatal(err)
	}
	restore := runner.SetProcessStarterForTest(&fakeStarter{processes: []*fakeProcess{proc}})
	defer restore()

	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, store, nil, nil) }()
	waitFor(t, 500*time.Millisecond, func() bool { return proc.started() })
	if _, _, err := store.EnqueueJob(context.Background(), model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "created-while-active"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 500*time.Millisecond, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, job := range jobs {
			if job.DedupeKey == "created-while-active" && job.Status == model.JobStatusPending {
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

func TestRunReapRefillWithoutWaitingForPollInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 80 * time.Millisecond}
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: time.Second}
	cfg.Routes[0].When.LabelsInclude = []string{"no-route"}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "second"}); err != nil {
		t.Fatal(err)
	}
	proc1 := newFakeProcess(3201)
	proc2 := newFakeProcess(3202)
	restore := runner.SetProcessStarterForTest(&fakeStarter{processes: []*fakeProcess{proc1, proc2}})
	defer restore()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, store, nil, nil) }()
	waitFor(t, 500*time.Millisecond, func() bool { return proc1.started() })
	proc1.finish(nil)
	waitFor(t, 2*time.Second, func() bool { return proc2.started() })
	proc2.finish(nil)
	waitFor(t, 500*time.Millisecond, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, job := range jobs {
			if job.Status != model.JobStatusSucceeded {
				return false
			}
		}
		return true
	})
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunShutdownCancelsActiveJobFinalizesCancelledAndDeletesHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 80 * time.Millisecond}
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: time.Second}
	proc := newFakeProcess(3301)
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe"}); err != nil {
		t.Fatal(err)
	}
	restore := runner.SetProcessStarterForTest(&fakeStarter{processes: []*fakeProcess{proc}})
	defer restore()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, store, nil, nil) }()
	waitFor(t, 500*time.Millisecond, func() bool { return proc.started() && countHeartbeats(t, cfg) == 1 })
	cancel()
	select {
	case <-proc.killCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("process was not killed on shutdown")
	}
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
	jobs, _ := store.ListJobs(context.Background())
	if jobs[0].Status != model.JobStatusCancelled || jobs[0].LastError == "" {
		t.Fatalf("job = %#v", jobs[0])
	}
	if got := staleHeartbeats(t, cfg); got != 0 {
		t.Fatalf("stale heartbeats = %d, want 0", got)
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

type fakeStarter struct {
	processes []*fakeProcess
	started   int
}

func (s *fakeStarter) StartProcess(spec runner.ProcessSpec) (runner.Process, error) {
	if s.started >= len(s.processes) {
		return nil, errors.New("unexpected process start")
	}
	proc := s.processes[s.started]
	s.started++
	proc.markStarted()
	return proc, nil
}

type fakeProcess struct {
	pid       int
	waitCh    chan error
	killCh    chan struct{}
	startCh   chan struct{}
	killOnce  chan struct{}
	finishMux chan struct{}
}

func newFakeProcess(pid int) *fakeProcess {
	return &fakeProcess{pid: pid, waitCh: make(chan error, 1), killCh: make(chan struct{}), startCh: make(chan struct{}), killOnce: make(chan struct{}, 1), finishMux: make(chan struct{}, 1)}
}

func (p *fakeProcess) PID() int { return p.pid }
func (p *fakeProcess) Wait() error {
	return <-p.waitCh
}
func (p *fakeProcess) KillTree() error {
	select {
	case p.killOnce <- struct{}{}:
		close(p.killCh)
		p.finish(errors.New("killed"))
	default:
	}
	return nil
}
func (p *fakeProcess) markStarted() { close(p.startCh) }
func (p *fakeProcess) started() bool {
	select {
	case <-p.startCh:
		return true
	default:
		return false
	}
}
func (p *fakeProcess) finish(err error) {
	select {
	case p.finishMux <- struct{}{}:
		p.waitCh <- err
	default:
	}
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
