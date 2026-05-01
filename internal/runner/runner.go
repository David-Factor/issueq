// Package runner prepares job context, artifact paths, and environment values.
package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
