package dispatcher

import (
	"context"
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

func TestDispatchWithSupervisorCompletesBoundedFrontier(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t)
	defer store.Close()
	enqueue(t, ctx, store, "one", 2)
	enqueue(t, ctx, store, "two", 1)
	backend := newFakeDispatchSupervisor()
	result, err := DispatchWithSupervisor(ctx, cfg, store, nil, backend)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 2 || result.Succeeded != 2 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
	if backend.launchCount() != 2 {
		t.Fatalf("launch count = %d, want 2", backend.launchCount())
	}
}

func TestDispatchCapturesInitialFrontierOnly(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t)
	defer store.Close()
	enqueue(t, ctx, store, "first", 1)
	backend := newFakeDispatchSupervisor()
	backend.onLaunch = func() {
		enqueue(t, ctx, store, "late", 1)
	}
	result, err := DispatchWithSupervisor(ctx, cfg, store, nil, backend)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Succeeded != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var latePending bool
	for _, job := range jobs {
		if job.DedupeKey == "late" && job.Status == model.JobStatusPending {
			latePending = true
		}
	}
	if !latePending {
		t.Fatalf("jobs = %#v", jobs)
	}
}

func TestDispatchReturnsWhenFrontierBlockedByUnrelatedCapacity(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t)
	defer store.Close()
	enqueue(t, ctx, store, "blocked", 1)
	old := model.RunnerIdentity{RunnerID: "old", InstanceID: "old-instance"}
	if err := store.HeartbeatRunner(ctx, old, 123, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	enqueue(t, ctx, store, "running", 2)
	claimed, err := store.ClaimNextJob(ctx, old, []string{"code"}, 1, map[string]int{"code": 1}, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim unrelated = %#v err=%v", claimed, err)
	}
	result, err := DispatchWithSupervisor(ctx, cfg, store, nil, newFakeDispatchSupervisor())
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 0 {
		t.Fatalf("result = %#v, want no claims", result)
	}
	jobs, _ := store.ListJobs(ctx)
	var blockedPending bool
	for _, job := range jobs {
		if job.DedupeKey == "blocked" && job.Status == model.JobStatusPending {
			blockedPending = true
		}
	}
	if !blockedPending {
		t.Fatalf("jobs = %#v", jobs)
	}
}

func TestDispatchWithGitHubSkipsStaleRouteBeforeLaunch(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t)
	defer store.Close()
	enqueue(t, ctx, store, "stale", 1)
	gh := &fakeDispatchGitHub{issue: model.IssueSnapshot{IssueKey: issueKey, Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"other"}, State: "open"}}
	backend := newFakeDispatchSupervisor()
	result, err := DispatchWithSupervisor(ctx, cfg, store, gh, backend)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Skipped != 1 || backend.launchCount() != 0 {
		t.Fatalf("result=%#v launches=%d", result, backend.launchCount())
	}
}

const issueKey = "github.com/example-org/example-repo#1"

func setupDispatch(t *testing.T) (*sqlitestore.Store, config.Config) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	store, err := sqlitestore.Open(ctx, filepath.Join(dir, "issueq.db"))
	if err != nil {
		t.Fatal(err)
	}
	issue := model.IssueSnapshot{IssueKey: issueKey, Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "task.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Runner:  config.RunnerConfig{Name: "runner", Capabilities: []string{"code"}, Env: config.EnvConfig{Pass: []string{"PATH", "HOME"}}},
		Queue:   config.QueueConfig{MaxGlobalConcurrency: 1, LeaseDuration: config.Duration{Duration: 500 * time.Millisecond}},
		Workdir: config.WorkdirConfig{Path: filepath.Join(dir, ".issueq")},
		GitHub:  config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo", TokenEnv: "GITHUB_TOKEN"},
		Routes:  []config.RouteConfig{{Name: "code", When: config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}, Job: config.JobConfig{Kind: "code", Command: []string{script}, Timeout: config.Duration{Duration: time.Second}, Concurrency: 1, MaxAttempts: 3}}},
	}
	return store, cfg
}

func enqueue(t *testing.T, ctx context.Context, store *sqlitestore.Store, dedupe string, priority int) {
	t.Helper()
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: issueKey, RouteName: "code", Kind: "code", Priority: priority, DedupeKey: dedupe}); err != nil {
		t.Fatal(err)
	}
}

type fakeDispatchSupervisor struct {
	mu           sync.Mutex
	launches     []supervisor.LaunchSpec
	observations map[string]supervisor.Observation
	onLaunch     func()
}

func newFakeDispatchSupervisor() *fakeDispatchSupervisor {
	return &fakeDispatchSupervisor{observations: map[string]supervisor.Observation{}}
}

func (f *fakeDispatchSupervisor) Launch(ctx context.Context, spec supervisor.LaunchSpec) (supervisor.LaunchRecord, error) {
	f.mu.Lock()
	f.launches = append(f.launches, spec)
	if f.onLaunch != nil {
		f.onLaunch()
		f.onLaunch = nil
	}
	finished := time.Now().UTC()
	f.observations[spec.JobID] = supervisor.Observation{State: supervisor.RunExited, HasExitCode: true, ExitCode: 0, StartedAt: finished, FinishedAt: finished, ResultPath: spec.ResultPath, StdoutPath: spec.StdoutPath, StderrPath: spec.StderrPath}
	f.mu.Unlock()
	return supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: spec.JobID + "-wrapper", JobID: spec.JobID, LaunchToken: spec.LaunchToken, PID: 1000 + len(f.launches), MetadataPath: spec.MetadataPath, StartedAt: finished, TimeoutAt: finished.Add(spec.Timeout)}, nil
}

func (f *fakeDispatchSupervisor) Inspect(ctx context.Context, record supervisor.LaunchRecord) (supervisor.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obs, ok := f.observations[record.JobID]
	if !ok {
		return supervisor.Observation{State: supervisor.RunUnknown, Error: "missing fake observation"}, nil
	}
	return obs, nil
}

func (f *fakeDispatchSupervisor) Cancel(ctx context.Context, record supervisor.LaunchRecord, reason supervisor.CancelReason) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observations[record.JobID] = supervisor.Observation{State: supervisor.RunCancelled, Error: "runner shutting down", FinishedAt: time.Now().UTC(), ResultPath: record.MetadataPath}
	return nil
}

func (f *fakeDispatchSupervisor) launchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.launches)
}

type fakeDispatchGitHub struct{ issue model.IssueSnapshot }

func (f *fakeDispatchGitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	return []model.IssueSnapshot{f.issue}, nil
}
func (f *fakeDispatchGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]model.IssueComment, error) {
	return nil, nil
}
func (f *fakeDispatchGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	return f.issue, nil
}
func (f *fakeDispatchGitHub) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.issue.Labels = append(f.issue.Labels, labels...)
	return nil
}
func (f *fakeDispatchGitHub) SetLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.issue.Labels = append([]string(nil), labels...)
	return nil
}

func (f *fakeDispatchGitHub) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	return nil
}
func (f *fakeDispatchGitHub) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	return nil
}
