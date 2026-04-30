// Package runner executes configured subprocess commands for jobs.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
)

type Context struct {
	Issue  model.IssueSnapshot `json:"issue"`
	Job    JobContext          `json:"job"`
	Runner model.RunnerInfo    `json:"runner"`
}

type JobContext struct {
	ID          string `json:"id"`
	Route       string `json:"route"`
	Kind        string `json:"kind"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"max_attempts"`
}

type Paths struct {
	Dir         string
	ContextPath string
	ResultPath  string
	StdoutPath  string
	StderrPath  string
}

type Result struct {
	ExitCode   int
	TimedOut   bool
	Cancelled  bool
	Error      error
	Paths      Paths
	StartedAt  time.Time
	FinishedAt time.Time
	PID        int
}

func (r Result) ErrorString() string {
	if r.Error != nil {
		return r.Error.Error()
	}
	if r.ExitCode != 0 {
		return fmt.Sprintf("subprocess exited with code %d", r.ExitCode)
	}
	return ""
}

func PreparePaths(workdir, jobID string) Paths {
	dir := filepath.Join(workdir, "jobs", jobID)
	return Paths{
		Dir:         dir,
		ContextPath: filepath.Join(dir, "context.json"),
		ResultPath:  filepath.Join(dir, "result.json"),
		StdoutPath:  filepath.Join(dir, "stdout.log"),
		StderrPath:  filepath.Join(dir, "stderr.log"),
	}
}

func WriteContext(paths Paths, ctxData Context) error {
	if err := os.MkdirAll(paths.Dir, 0o700); err != nil {
		return fmt.Errorf("create job workdir: %w", err)
	}
	data, err := json.MarshalIndent(ctxData, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal context: %w", err)
	}
	if err := os.WriteFile(paths.ContextPath, data, 0o600); err != nil {
		return fmt.Errorf("write context: %w", err)
	}
	return nil
}

func currentAttempt(attempts int) int {
	if attempts < 1 {
		return 1
	}
	return attempts
}

type ProcessSpec struct {
	Command string
	Args    []string
	Env     []string
	Stdout  *os.File
	Stderr  *os.File
}

type Process interface {
	PID() int
	Wait() error
	KillTree() error
}

type ProcessStarter interface {
	StartProcess(spec ProcessSpec) (Process, error)
}

type realProcessStarter struct{}

type realProcess struct {
	cmd *exec.Cmd
}

func (realProcessStarter) StartProcess(spec ProcessSpec) (Process, error) {
	cmd := exec.Command(spec.Command, spec.Args...)
	prepareCommand(cmd)
	cmd.Stdout = spec.Stdout
	cmd.Stderr = spec.Stderr
	cmd.Env = spec.Env
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &realProcess{cmd: cmd}, nil
}

func (p *realProcess) PID() int        { return p.cmd.Process.Pid }
func (p *realProcess) Wait() error     { return p.cmd.Wait() }
func (p *realProcess) KillTree() error { return killProcessTree(p.cmd.Process) }

var (
	processStarterMu sync.Mutex
	processStarter   ProcessStarter = realProcessStarter{}
)

func SetProcessStarterForTest(starter ProcessStarter) func() {
	processStarterMu.Lock()
	previous := processStarter
	processStarter = starter
	processStarterMu.Unlock()
	return func() {
		processStarterMu.Lock()
		processStarter = previous
		processStarterMu.Unlock()
	}
}

func currentProcessStarter() ProcessStarter {
	processStarterMu.Lock()
	defer processStarterMu.Unlock()
	return processStarter
}

type Handle struct {
	Job         model.Job
	Issue       model.IssueSnapshot
	Route       config.RouteConfig
	Runner      model.RunnerInfo
	Paths       Paths
	PID         int
	StartedAt   time.Time
	Timeout     time.Duration
	Done        <-chan struct{}
	ContextDone <-chan struct{}
	Ready       <-chan struct{}

	ctx        context.Context
	cancel     context.CancelCauseFunc
	timer      *time.Timer
	process    Process
	stdout     *os.File
	stderr     *os.File
	done       chan struct{}
	waitErr    error
	waitErrMu  sync.Mutex
	closeOnce  sync.Once
	cancelOnce sync.Once
}

func (h *Handle) Cancel(cause error) {
	if cause == nil {
		cause = context.Canceled
	}
	h.cancelOnce.Do(func() { h.cancel(cause) })
}

func (h *Handle) closeLogs() {
	h.closeOnce.Do(func() {
		if h.stdout != nil {
			_ = h.stdout.Close()
		}
		if h.stderr != nil {
			_ = h.stderr.Close()
		}
	})
}

type StartError struct {
	Result Result
}

func (e StartError) Error() string { return e.Result.ErrorString() }

func Start(ctx context.Context, cfg config.Config, route config.RouteConfig, job model.Job, issue model.IssueSnapshot, runnerInfo model.RunnerInfo) (*Handle, error) {
	paths := PreparePaths(cfg.Workdir.Path, job.ID)
	started := time.Now().UTC()
	ctxData := Context{
		Issue: issue,
		Job: JobContext{
			ID:          job.ID,
			Route:       job.RouteName,
			Kind:        job.Kind,
			Attempt:     currentAttempt(job.Attempts),
			MaxAttempts: route.Job.MaxAttempts,
		},
		Runner: runnerInfo,
	}
	if err := WriteContext(paths, ctxData); err != nil {
		return nil, StartError{Result: Result{ExitCode: -1, Error: err, Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}}
	}

	stdout, err := os.OpenFile(paths.StdoutPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, StartError{Result: Result{ExitCode: -1, Error: fmt.Errorf("open stdout log: %w", err), Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}}
	}
	stderr, err := os.OpenFile(paths.StderrPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		_ = stdout.Close()
		return nil, StartError{Result: Result{ExitCode: -1, Error: fmt.Errorf("open stderr log: %w", err), Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}}
	}

	if len(route.Job.Command) == 0 {
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, StartError{Result: Result{ExitCode: -1, Error: errors.New("job command is empty"), Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}}
	}
	procCtx, cancel := context.WithCancelCause(ctx)
	timeout := route.Job.Timeout.Duration
	if timeout <= 0 {
		timeout = config.DefaultLeaseDuration
	}
	var timeoutTimer *time.Timer
	timeoutTimer = time.AfterFunc(timeout, func() { cancel(context.DeadlineExceeded) })

	args := append([]string{}, route.Job.Command[1:]...)
	args = append(args, paths.ContextPath, paths.ResultPath)
	proc, err := currentProcessStarter().StartProcess(ProcessSpec{
		Command: route.Job.Command[0],
		Args:    args,
		Env:     BuildEnv(cfg, route, job, issue, paths),
		Stdout:  stdout,
		Stderr:  stderr,
	})
	if err != nil {
		timeoutTimer.Stop()
		cancel(context.Canceled)
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, StartError{Result: Result{ExitCode: -1, Error: fmt.Errorf("start subprocess: %w", err), Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}}
	}
	ready := make(chan struct{})
	handle := &Handle{
		Job:         job,
		Issue:       issue,
		Route:       route,
		Runner:      runnerInfo,
		Paths:       paths,
		PID:         proc.PID(),
		StartedAt:   started,
		Timeout:     timeout,
		ctx:         procCtx,
		cancel:      cancel,
		timer:       timeoutTimer,
		process:     proc,
		stdout:      stdout,
		stderr:      stderr,
		done:        ready,
		Done:        ready,
		ContextDone: procCtx.Done(),
		Ready:       ready,
	}
	go func() {
		err := proc.Wait()
		handle.waitErrMu.Lock()
		handle.waitErr = err
		handle.waitErrMu.Unlock()
		close(ready)
	}()
	return handle, nil
}

func Wait(handle *Handle) Result {
	select {
	case <-handle.Done:
	case <-handle.ctx.Done():
		killErr := handle.process.KillTree()
		<-handle.Done
		finished := time.Now().UTC()
		handle.timer.Stop()
		handle.closeLogs()
		cause := context.Cause(handle.ctx)
		if cause == nil {
			cause = context.Canceled
		}
		if errors.Is(cause, context.DeadlineExceeded) {
			if killErr != nil {
				return Result{ExitCode: -1, TimedOut: true, Error: fmt.Errorf("subprocess timed out after %s; kill process tree: %w", handle.Timeout, killErr), Paths: handle.Paths, StartedAt: handle.StartedAt, FinishedAt: finished, PID: handle.PID}
			}
			return Result{ExitCode: -1, TimedOut: true, Error: fmt.Errorf("subprocess timed out after %s", handle.Timeout), Paths: handle.Paths, StartedAt: handle.StartedAt, FinishedAt: finished, PID: handle.PID}
		}
		if killErr != nil {
			return Result{ExitCode: -1, Cancelled: true, Error: fmt.Errorf("subprocess cancelled: %v; kill process tree: %w", cause, killErr), Paths: handle.Paths, StartedAt: handle.StartedAt, FinishedAt: finished, PID: handle.PID}
		}
		return Result{ExitCode: -1, Cancelled: true, Error: cause, Paths: handle.Paths, StartedAt: handle.StartedAt, FinishedAt: finished, PID: handle.PID}
	}
	finished := time.Now().UTC()
	handle.timer.Stop()
	handle.closeLogs()
	handle.waitErrMu.Lock()
	err := handle.waitErr
	handle.waitErrMu.Unlock()
	if err != nil {
		exitCode := -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
		return Result{ExitCode: exitCode, Error: err, Paths: handle.Paths, StartedAt: handle.StartedAt, FinishedAt: finished, PID: handle.PID}
	}
	return Result{ExitCode: 0, Paths: handle.Paths, StartedAt: handle.StartedAt, FinishedAt: finished, PID: handle.PID}
}

func Run(ctx context.Context, cfg config.Config, route config.RouteConfig, job model.Job, issue model.IssueSnapshot, runnerInfo model.RunnerInfo) Result {
	handle, err := Start(ctx, cfg, route, job, issue, runnerInfo)
	if err != nil {
		var startErr StartError
		if errors.As(err, &startErr) {
			return startErr.Result
		}
		return Result{ExitCode: -1, Error: err, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC()}
	}
	return Wait(handle)
}

func errorsIsProcessDone(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, os.ErrProcessDone)
}

func BuildEnv(cfg config.Config, route config.RouteConfig, job model.Job, issue model.IssueSnapshot, paths Paths) []string {
	env := map[string]string{}
	if cfg.Runner.Env.Inherit {
		for _, pair := range os.Environ() {
			k, v, ok := strings.Cut(pair, "=")
			if ok {
				env[k] = v
			}
		}
	}
	for _, name := range append(append([]string{}, cfg.Runner.Env.Pass...), route.Job.Env.Pass...) {
		if name == cfg.GitHub.TokenEnv {
			continue
		}
		if value, ok := os.LookupEnv(name); ok {
			env[name] = value
		}
	}
	delete(env, cfg.GitHub.TokenEnv)

	env["ISSUEQ_JOB_ID"] = job.ID
	env["ISSUEQ_ROUTE"] = job.RouteName
	env["ISSUEQ_KIND"] = job.Kind
	env["ISSUEQ_ATTEMPT"] = strconv.Itoa(currentAttempt(job.Attempts))
	env["ISSUEQ_CONTEXT_PATH"] = paths.ContextPath
	env["ISSUEQ_RESULT_PATH"] = paths.ResultPath
	env["ISSUEQ_ISSUE_KEY"] = issue.IssueKey
	env["GITHUB_HOST"] = issue.Host
	env["GITHUB_OWNER"] = issue.Owner
	env["GITHUB_REPO"] = issue.Repo
	env["GITHUB_ISSUE_NUMBER"] = strconv.Itoa(issue.Number)

	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}
