package attached

import (
	"context"
	"errors"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/runner"
	"issueq/internal/supervisor"
)

type fakeStarter struct {
	process *fakeProcess
}

func (s *fakeStarter) StartProcess(spec runner.ProcessSpec) (runner.Process, error) {
	return s.process, nil
}

type fakeProcess struct {
	pid    int
	waitCh chan error
	killCh chan struct{}
}

func (p *fakeProcess) PID() int { return p.pid }
func (p *fakeProcess) Wait() error {
	return <-p.waitCh
}
func (p *fakeProcess) KillTree() error {
	select {
	case <-p.killCh:
	default:
		close(p.killCh)
		p.waitCh <- errors.New("killed")
	}
	return nil
}

func TestLaunchInspectCachesTerminalObservation(t *testing.T) {
	proc := &fakeProcess{pid: 1234, waitCh: make(chan error, 1), killCh: make(chan struct{})}
	restore := runner.SetProcessStarterForTest(&fakeStarter{process: proc})
	defer restore()
	backend := New()
	record, err := backend.LaunchJob(context.Background(), testLaunchSpec(t, "tok"))
	if err != nil {
		t.Fatalf("LaunchJob error = %v", err)
	}
	if record.Kind != supervisor.KindAttached || record.ID == "" || record.PID != proc.pid || record.LaunchToken != "tok" {
		t.Fatalf("record = %#v", record)
	}
	obs, err := backend.Inspect(context.Background(), record)
	if err != nil {
		t.Fatalf("Inspect running error = %v", err)
	}
	if obs.State != supervisor.RunRunning {
		t.Fatalf("running obs = %#v", obs)
	}
	proc.waitCh <- nil
	waitForDone(t, record, backend)
	obs, err = backend.Inspect(context.Background(), record)
	if err != nil {
		t.Fatalf("Inspect terminal error = %v", err)
	}
	if obs.State != supervisor.RunExited || !obs.HasExitCode || obs.ExitCode != 0 {
		t.Fatalf("terminal obs = %#v", obs)
	}
	repeated, err := backend.Inspect(context.Background(), record)
	if err != nil {
		t.Fatalf("Inspect repeated error = %v", err)
	}
	if repeated.State != obs.State || repeated.ExitCode != obs.ExitCode {
		t.Fatalf("repeated obs = %#v, want %#v", repeated, obs)
	}
}

func TestCancelIsIdempotent(t *testing.T) {
	proc := &fakeProcess{pid: 5678, waitCh: make(chan error, 1), killCh: make(chan struct{})}
	restore := runner.SetProcessStarterForTest(&fakeStarter{process: proc})
	defer restore()
	backend := New()
	record, err := backend.LaunchJob(context.Background(), testLaunchSpec(t, "tok-cancel"))
	if err != nil {
		t.Fatalf("LaunchJob error = %v", err)
	}
	if err := backend.Cancel(context.Background(), record, supervisor.CancelShutdown); err != nil {
		t.Fatalf("Cancel error = %v", err)
	}
	if err := backend.Cancel(context.Background(), record, supervisor.CancelShutdown); err != nil {
		t.Fatalf("repeated Cancel error = %v", err)
	}
	waitForDone(t, record, backend)
	obs, err := backend.Inspect(context.Background(), record)
	if err != nil {
		t.Fatalf("Inspect error = %v", err)
	}
	if obs.State != supervisor.RunCancelled || obs.HasExitCode {
		t.Fatalf("cancel obs = %#v", obs)
	}
}

func waitForDone(t *testing.T, record supervisor.LaunchRecord, backend *Supervisor) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		obs, err := backend.Inspect(context.Background(), record)
		if err != nil {
			t.Fatalf("Inspect while waiting error = %v", err)
		}
		if obs.State != supervisor.RunRunning {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for attached process completion")
		case <-time.After(time.Millisecond):
		}
	}
}

func testLaunchSpec(t *testing.T, token string) JobLaunchSpec {
	t.Helper()
	cfg := config.Config{
		Runner:  config.RunnerConfig{Name: "runner", Env: config.EnvConfig{Pass: []string{"PATH", "HOME"}}},
		GitHub:  config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo", TokenEnv: "GITHUB_TOKEN"},
		Workdir: config.WorkdirConfig{Path: t.TempDir()},
	}
	route := config.RouteConfig{Name: "code", Job: config.JobConfig{Kind: "code", Command: []string{"fake-task"}, Timeout: config.Duration{Duration: time.Second}, MaxAttempts: 3}}
	job := model.Job{ID: "job_1", IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Attempts: 1}
	issue := model.IssueSnapshot{IssueKey: job.IssueKey, Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}
	paths := runner.PreparePaths(cfg.Workdir.Path, job.ID)
	return JobLaunchSpec{LaunchSpec: supervisor.LaunchSpec{JobID: job.ID, LaunchToken: token, Timeout: route.Job.Timeout.Duration}, Config: cfg, Route: route, Job: job, Issue: issue, Runner: model.RunnerInfo{ID: "runner"}, Paths: paths}
}
