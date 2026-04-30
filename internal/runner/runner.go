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

func Run(ctx context.Context, cfg config.Config, route config.RouteConfig, job model.Job, issue model.IssueSnapshot, runnerInfo model.RunnerInfo) Result {
	paths := PreparePaths(cfg.Workdir.Path, job.ID)
	started := time.Now().UTC()
	ctxData := Context{
		Issue: issue,
		Job: JobContext{
			ID:          job.ID,
			Route:       job.RouteName,
			Kind:        job.Kind,
			Attempt:     job.Attempts + 1,
			MaxAttempts: route.Job.MaxAttempts,
		},
		Runner: runnerInfo,
	}
	if err := WriteContext(paths, ctxData); err != nil {
		return Result{ExitCode: -1, Error: err, Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}
	}

	stdout, err := os.OpenFile(paths.StdoutPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return Result{ExitCode: -1, Error: fmt.Errorf("open stdout log: %w", err), Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}
	}
	defer stdout.Close()
	stderr, err := os.OpenFile(paths.StderrPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return Result{ExitCode: -1, Error: fmt.Errorf("open stderr log: %w", err), Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}
	}
	defer stderr.Close()

	if len(route.Job.Command) == 0 {
		return Result{ExitCode: -1, Error: errors.New("job command is empty"), Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, route.Job.Timeout.Duration)
	defer cancel()
	args := append([]string{}, route.Job.Command[1:]...)
	args = append(args, paths.ContextPath, paths.ResultPath)
	cmd := exec.CommandContext(timeoutCtx, route.Job.Command[0], args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = BuildEnv(cfg, route, job, issue, paths)

	if err := cmd.Start(); err != nil {
		return Result{ExitCode: -1, Error: fmt.Errorf("start subprocess: %w", err), Paths: paths, StartedAt: started, FinishedAt: time.Now().UTC()}
	}
	pid := cmd.Process.Pid
	err = cmd.Wait()
	finished := time.Now().UTC()
	if timeoutCtx.Err() == context.DeadlineExceeded {
		return Result{ExitCode: -1, TimedOut: true, Error: fmt.Errorf("subprocess timed out after %s", route.Job.Timeout.Duration), Paths: paths, StartedAt: started, FinishedAt: finished, PID: pid}
	}
	if err != nil {
		exitCode := -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
		return Result{ExitCode: exitCode, Error: err, Paths: paths, StartedAt: started, FinishedAt: finished, PID: pid}
	}
	return Result{ExitCode: 0, Paths: paths, StartedAt: started, FinishedAt: finished, PID: pid}
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
	env["ISSUEQ_ATTEMPT"] = strconv.Itoa(job.Attempts + 1)
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
