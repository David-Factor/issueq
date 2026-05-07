package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
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
	result, err := onceWithSupervisor(ctx, cfg, store, nil, newFakeDaemonSupervisor())
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
	backend.defaultRunning = true
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
	backend.defaultRunning = true
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

func TestRunAppliesGitHubLifecycleAroundWrapperJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	cfg.Routes[0].Job.OnStart = config.ActionConfig{LabelsRemove: []string{"agent-ready"}, LabelsAdd: []string{"agent-running"}}
	cfg.Routes[0].Job.OnSuccess = config.ActionConfig{LabelsRemove: []string{"agent-running"}, LabelsAdd: []string{"agent-done"}, Comment: "done"}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "lifecycle"}); err != nil {
		t.Fatal(err)
	}
	gh := &fakeDaemonGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	backend := newFakeDaemonSupervisor()
	backend.defaultRunning = true
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, gh, nil, backend) }()
	waitFor(t, time.Second, func() bool { return backend.launchCount() == 1 })
	if got := strings.Join(gh.calls, "|"); !strings.Contains(got, "set:agent-running") {
		t.Fatalf("pre-launch calls = %v", gh.calls)
	}
	var jobID string
	waitFor(t, time.Second, func() bool {
		jobID = backend.lastJobID()
		return jobID != ""
	})
	backend.complete(jobID, supervisor.RunExited)
	waitFor(t, 2*time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, job := range jobs {
			if job.ID == jobID && job.Status == model.JobStatusSucceeded {
				return true
			}
		}
		return false
	})
	calls := strings.Join(gh.calls, "|")
	if !strings.Contains(calls, "set:agent-done") || !strings.Contains(calls, "comment") {
		t.Fatalf("calls = %v", gh.calls)
	}
	events, err := store.ListJobEvents(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "action_on_start") || !hasEvent(events, "job_succeeded") {
		t.Fatalf("events = %#v", events)
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunSkipsStaleGitHubRouteBeforeWrapperLaunch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "stale"}); err != nil {
		t.Fatal(err)
	}
	gh := &fakeDaemonGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"other"}, State: "open"}}
	backend := newFakeDaemonSupervisor()
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, gh, nil, backend) }()
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		return len(jobs) == 1 && jobs[0].Status == model.JobStatusSkipped
	})
	if backend.launchCount() != 0 {
		t.Fatalf("launch count = %d, want 0", backend.launchCount())
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunAppliesResultJSONActionsOnWrapperCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	cfg.Routes[0].Job.OnSuccess = config.ActionConfig{LabelsAdd: []string{"base"}}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "result-actions"}); err != nil {
		t.Fatal(err)
	}
	gh := &fakeDaemonGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	backend := newFakeDaemonSupervisor()
	backend.defaultRunning = true
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, gh, nil, backend) }()
	waitFor(t, time.Second, func() bool { return backend.launchCount() == 1 })
	jobID := backend.lastJobID()
	var resultPath string
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, job := range jobs {
			if job.ID == jobID && job.ResultPath != "" {
				resultPath = job.ResultPath
				return true
			}
		}
		return false
	})
	if err := os.WriteFile(resultPath, []byte(`{"labels_add":["from-result"],"comment":"result comment"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	backend.complete(jobID, supervisor.RunExited)
	waitFor(t, 2*time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, job := range jobs {
			if job.ID == jobID && job.Status == model.JobStatusSucceeded {
				return true
			}
		}
		return false
	})
	calls := strings.Join(gh.calls, "|")
	if !strings.Contains(calls, "set:agent-ready,base,from-result") || !strings.Contains(calls, "comment") {
		t.Fatalf("calls = %v", gh.calls)
	}
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
	backend.defaultRunning = true
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
	backend.defaultRunning = true
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

func TestRunAdoptsStaleDurableRunningWrapperAndRenews(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	job := seedStaleDurableJob(t, ctx, store, cfg, "adopt-running")
	backend := newFakeDaemonSupervisor()
	backend.setObservation(job.ID, supervisor.Observation{State: supervisor.RunRunning, StartedAt: time.Now().UTC()})
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, got := range jobs {
			if got.ID == job.ID && got.RunnerInstanceID != job.RunnerInstanceID && got.Status == model.JobStatusRunning && got.LeaseUntil != nil && got.LeaseUntil.After(time.Now()) {
				return true
			}
		}
		return false
	})
	if backend.launchCount() != 0 {
		t.Fatalf("launch count = %d, want 0", backend.launchCount())
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunAdoptsStaleCompletedWrapperAndFinalizes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	job := seedStaleDurableJob(t, ctx, store, cfg, "adopt-complete")
	backend := newFakeDaemonSupervisor()
	backend.setObservation(job.ID, supervisor.Observation{State: supervisor.RunExited, HasExitCode: true, ExitCode: 0, FinishedAt: time.Now().UTC(), ResultPath: job.ResultPath, StdoutPath: job.StdoutPath, StderrPath: job.StderrPath})
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, got := range jobs {
			if got.ID == job.ID && got.Status == model.JobStatusSucceeded && got.RunnerInstanceID == "" {
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

func TestRunMarksStaleDurableUnknownWithoutRequeueOrLaunch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	job := seedStaleDurableJob(t, ctx, store, cfg, "unknown")
	backend := newFakeDaemonSupervisor()
	backend.setObservation(job.ID, supervisor.Observation{State: supervisor.RunUnknown, Error: "missing"})
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, got := range jobs {
			if got.ID == job.ID && got.Status == model.JobStatusRunning && got.RunnerInstanceID == job.RunnerInstanceID && got.LaunchState == model.LaunchStateUnknown {
				return true
			}
		}
		return false
	})
	if backend.launchCount() != 0 {
		t.Fatalf("launch count = %d, want 0", backend.launchCount())
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func TestRunMarksUnsupportedStaleDurableUnknownWithoutInspect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, cfg := setupOnce(t)
	defer store.Close()
	cfg.Polling.Interval = config.Duration{Duration: time.Hour}
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	job := seedStaleDurableJob(t, ctx, store, cfg, "unsupported")
	if _, err := openTestDB(t, cfg).ExecContext(ctx, `UPDATE jobs SET supervisor_kind = ? WHERE id = ?`, supervisor.KindSystemd, job.ID); err != nil {
		t.Fatal(err)
	}
	backend := newFakeDaemonSupervisor()
	done := make(chan error, 1)
	go func() { done <- runWithSupervisor(ctx, cfg, store, nil, nil, backend) }()
	waitFor(t, time.Second, func() bool {
		jobs, _ := store.ListJobs(context.Background())
		for _, got := range jobs {
			if got.ID == job.ID && got.Status == model.JobStatusRunning && got.RunnerInstanceID == job.RunnerInstanceID && got.LaunchState == model.LaunchStateUnknown {
				return true
			}
		}
		return false
	})
	if backend.inspectCount() != 0 || backend.launchCount() != 0 {
		t.Fatalf("inspects=%d launches=%d, want 0", backend.inspectCount(), backend.launchCount())
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run error = %v", err)
	}
}

func seedStaleDurableJob(t *testing.T, ctx context.Context, store *sqlitestore.Store, cfg config.Config, dedupe string) model.Job {
	t.Helper()
	old := model.RunnerIdentity{RunnerID: "old", InstanceID: "old-" + dedupe}
	if err := store.HeartbeatRunner(ctx, old, 99, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: dedupe}); err != nil {
		t.Fatal(err)
	}
	job, err := store.ClaimNextJob(ctx, old, []string{"code"}, 10, map[string]int{"code": 10}, 50*time.Millisecond)
	if err != nil || job == nil {
		t.Fatalf("claim stale durable=%#v err=%v", job, err)
	}
	pathsDir := filepath.Join(cfg.Workdir.Path, "jobs", job.ID)
	if err := os.MkdirAll(pathsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	timeoutAt := now.Add(time.Minute)
	spec := model.LaunchSpecRecord{SupervisorKind: supervisor.KindWrapper, LaunchToken: "tok-" + dedupe, LaunchSpecPath: filepath.Join(pathsDir, "spec.json"), ContextPath: filepath.Join(pathsDir, "context.json"), ResultPath: filepath.Join(pathsDir, "result.json"), StdoutPath: filepath.Join(pathsDir, "stdout.log"), StderrPath: filepath.Join(pathsDir, "stderr.log"), RunMetadataPath: filepath.Join(pathsDir, "run.json"), TimeoutAt: timeoutAt}
	if err := store.PersistLaunchSpecOwned(ctx, job.ID, old.InstanceID, spec); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobLaunchingOwned(ctx, job.ID, old.InstanceID, spec.LaunchToken); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistLaunchRecordOwned(ctx, job.ID, old.InstanceID, model.LaunchRecord{SupervisorKind: supervisor.KindWrapper, SupervisorID: "sid-" + dedupe, LaunchToken: spec.LaunchToken, PID: 123, RunMetadataPath: spec.RunMetadataPath, LaunchSpecPath: spec.LaunchSpecPath, ContextPath: spec.ContextPath, ResultPath: spec.ResultPath, StdoutPath: spec.StdoutPath, StderrPath: spec.StderrPath, TimeoutAt: timeoutAt}); err != nil {
		t.Fatal(err)
	}
	if _, err := openTestDB(t, cfg).ExecContext(ctx, `UPDATE jobs SET lease_until = ? WHERE id = ?`, formatTestTime(now.Add(-time.Second)), job.ID); err != nil {
		t.Fatal(err)
	}
	jobs, _ := store.ListJobs(ctx)
	for _, got := range jobs {
		if got.ID == job.ID {
			return got
		}
	}
	t.Fatalf("job %s not found", job.ID)
	return model.Job{}
}

func formatTestTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}

func openTestDB(t *testing.T, cfg config.Config) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", cfg.Queue.SQLite.Path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
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

func hasEvent(events []model.JobEvent, eventType string) bool {
	for _, event := range events {
		if event.EventType == eventType {
			return true
		}
	}
	return false
}

type fakeDaemonGitHub struct {
	issue    model.IssueSnapshot
	calls    []string
	comments []string
}

func (f *fakeDaemonGitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	return []model.IssueSnapshot{f.issue}, nil
}

func (f *fakeDaemonGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]model.IssueComment, error) {
	return nil, nil
}

func (f *fakeDaemonGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	f.calls = append(f.calls, "get")
	return f.issue, nil
}

func (f *fakeDaemonGitHub) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "add:"+strings.Join(labels, ","))
	f.issue.Labels = append(f.issue.Labels, labels...)
	return nil
}

func (f *fakeDaemonGitHub) SetLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "set:"+strings.Join(labels, ","))
	f.issue.Labels = append([]string(nil), labels...)
	return nil
}

func (f *fakeDaemonGitHub) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "remove:"+strings.Join(labels, ","))
	blocked := map[string]bool{}
	for _, label := range labels {
		blocked[label] = true
	}
	out := f.issue.Labels[:0]
	for _, label := range f.issue.Labels {
		if !blocked[label] {
			out = append(out, label)
		}
	}
	f.issue.Labels = out
	return nil
}

func (f *fakeDaemonGitHub) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	f.calls = append(f.calls, "comment")
	f.comments = append(f.comments, body)
	return nil
}

type fakeDaemonSupervisor struct {
	mu             sync.Mutex
	launches       []supervisor.LaunchSpec
	observations   map[string]supervisor.Observation
	cancellations  []supervisor.FakeCancellation
	inspections    int
	defaultRunning bool
}

func (f *fakeDaemonGitHub) UpdateComment(ctx context.Context, owner, repo string, commentID string, body string) error {
	return nil
}

func newFakeDaemonSupervisor() *fakeDaemonSupervisor {
	return &fakeDaemonSupervisor{observations: map[string]supervisor.Observation{}}
}

func (f *fakeDaemonSupervisor) Launch(ctx context.Context, spec supervisor.LaunchSpec) (supervisor.LaunchRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.launches = append(f.launches, spec)
	record := supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: spec.JobID + "-wrapper", JobID: spec.JobID, LaunchToken: spec.LaunchToken, PID: 1000 + len(f.launches), MetadataPath: spec.MetadataPath, StartedAt: time.Now().UTC(), TimeoutAt: time.Now().UTC().Add(spec.Timeout)}
	state := supervisor.RunExited
	finishedAt := record.StartedAt
	hasExitCode := true
	if f.defaultRunning {
		state = supervisor.RunRunning
		finishedAt = time.Time{}
		hasExitCode = false
	}
	f.observations[spec.JobID] = supervisor.Observation{State: state, StartedAt: record.StartedAt, FinishedAt: finishedAt, HasExitCode: hasExitCode, ExitCode: 0, ResultPath: spec.ResultPath, StdoutPath: spec.StdoutPath, StderrPath: spec.StderrPath}
	return record, nil
}

func (f *fakeDaemonSupervisor) Inspect(ctx context.Context, record supervisor.LaunchRecord) (supervisor.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obs, ok := f.observations[record.JobID]
	f.inspections++
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
func (f *fakeDaemonSupervisor) inspectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inspections
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
func (f *fakeDaemonSupervisor) holdRunning(jobID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obs := f.observations[jobID]
	obs.State = supervisor.RunRunning
	obs.FinishedAt = time.Time{}
	obs.HasExitCode = false
	obs.ExitCode = 0
	f.observations[jobID] = obs
}

func (f *fakeDaemonSupervisor) setObservation(jobID string, obs supervisor.Observation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observations[jobID] = obs
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
