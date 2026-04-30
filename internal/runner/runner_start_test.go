package runner

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"issueq/internal/model"
)

type fakeStarter struct {
	process *fakeProcess
	err     error
	spec    ProcessSpec
}

func (s *fakeStarter) StartProcess(spec ProcessSpec) (Process, error) {
	s.spec = spec
	if s.err != nil {
		return nil, s.err
	}
	return s.process, nil
}

type fakeProcess struct {
	pid      int
	waitCh   chan error
	killOnce sync.Once
	killCh   chan struct{}
}

func newFakeProcess(pid int) *fakeProcess {
	return &fakeProcess{pid: pid, waitCh: make(chan error, 1), killCh: make(chan struct{})}
}

func (p *fakeProcess) PID() int { return p.pid }
func (p *fakeProcess) Wait() error {
	return <-p.waitCh
}
func (p *fakeProcess) KillTree() error {
	p.killOnce.Do(func() {
		close(p.killCh)
		p.waitCh <- errors.New("killed")
	})
	return nil
}

func TestStartExposesPathsAndPIDBeforeWait(t *testing.T) {
	proc := newFakeProcess(4242)
	starter := &fakeStarter{process: proc}
	restore := SetProcessStarterForTest(starter)
	defer restore()
	cfg := testConfig(t, []string{"fake-task", "arg1"})
	handle, err := Start(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{ID: "runner-1"})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	if handle.PID != 4242 || handle.Paths.ContextPath == "" || handle.Paths.StdoutPath == "" || handle.Paths.StderrPath == "" || handle.Paths.ResultPath == "" {
		t.Fatalf("handle = %#v", handle)
	}
	if _, err := os.Stat(handle.Paths.ContextPath); err != nil {
		t.Fatalf("context path missing before wait: %v", err)
	}
	if got := strings.Join(starter.spec.Args, " "); !strings.Contains(got, "arg1") || !strings.Contains(got, handle.Paths.ContextPath) || !strings.Contains(got, handle.Paths.ResultPath) {
		t.Fatalf("args = %#v", starter.spec.Args)
	}
	select {
	case <-handle.Done:
		t.Fatal("handle done before fake process completed")
	default:
	}
	proc.waitCh <- nil
	res := Wait(handle)
	if res.Error != nil || res.ExitCode != 0 || res.PID != 4242 {
		t.Fatalf("Wait result = %#v", res)
	}
}

func TestStartFailureReturnsResultAndClosesLogs(t *testing.T) {
	starter := &fakeStarter{err: errors.New("boom")}
	restore := SetProcessStarterForTest(starter)
	defer restore()
	cfg := testConfig(t, []string{"fake-task"})
	_, err := Start(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	var startErr StartError
	if !errors.As(err, &startErr) || startErr.Result.Error == nil || !strings.Contains(startErr.Result.Error.Error(), "boom") {
		t.Fatalf("Start err = %#v", err)
	}
	if startErr.Result.Paths.StdoutPath == "" || startErr.Result.Paths.StderrPath == "" {
		t.Fatalf("start result paths missing: %#v", startErr.Result.Paths)
	}
}

func TestFakeProcessStarterSimulatesFailureTimeoutCancellationAndBlocked(t *testing.T) {
	cfg := testConfig(t, []string{"fake-task"})

	failure := newFakeProcess(10)
	restore := SetProcessStarterForTest(&fakeStarter{process: failure})
	handle, err := Start(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	if err != nil {
		t.Fatal(err)
	}
	failure.waitCh <- errors.New("failed")
	if res := Wait(handle); res.Error == nil || res.ExitCode != -1 || res.TimedOut || res.Cancelled {
		t.Fatalf("failure result = %#v", res)
	}
	restore()

	timeout := newFakeProcess(11)
	restore = SetProcessStarterForTest(&fakeStarter{process: timeout})
	cfg.Routes[0].Job.Timeout.Duration = 10 * time.Millisecond
	handle, err = Start(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if res := Wait(handle); !res.TimedOut || res.Cancelled || res.Error == nil {
		t.Fatalf("timeout result = %#v", res)
	}
	restore()

	cancelled := newFakeProcess(12)
	restore = SetProcessStarterForTest(&fakeStarter{process: cancelled})
	cfg.Routes[0].Job.Timeout.Duration = time.Second
	handle, err = Start(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	if err != nil {
		t.Fatal(err)
	}
	handle.Cancel(errors.New("stop"))
	if res := Wait(handle); !res.Cancelled || res.TimedOut || res.Error == nil || !strings.Contains(res.Error.Error(), "stop") {
		t.Fatalf("cancel result = %#v", res)
	}
	restore()

	blocked := newFakeProcess(13)
	restore = SetProcessStarterForTest(&fakeStarter{process: blocked})
	handle, err = Start(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-handle.Done:
		t.Fatal("blocked fake process unexpectedly done")
	default:
	}
	blocked.waitCh <- nil
	if res := Wait(handle); res.Error != nil || res.ExitCode != 0 {
		t.Fatalf("blocked success result = %#v", res)
	}
	restore()
}
