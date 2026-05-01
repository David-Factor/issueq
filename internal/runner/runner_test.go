package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
)

func TestBuildEnvAllowsOnlyExplicitPassAndMetadata(t *testing.T) {
	t.Setenv("PATH", "/bin")
	t.Setenv("HOME", "/home/test")
	t.Setenv("AGENT_TOKEN", "agent")
	t.Setenv("GITHUB_TOKEN", "secret")
	t.Setenv("UNLISTED_SECRET", "nope")
	cfg := testConfig(t, []string{"/bin/echo"})
	cfg.Runner.Env.Pass = []string{"PATH", "HOME", "GITHUB_TOKEN"}
	cfg.Routes[0].Job.Env.Pass = []string{"AGENT_TOKEN"}
	job := testJob()
	issue := testIssue("malicious; touch /tmp/pwned")
	paths := PreparePaths(cfg.Workdir.Path, job.ID)
	env := strings.Join(BuildEnv(cfg, cfg.Routes[0], job, issue, paths), "\n")
	for _, want := range []string{"PATH=/bin", "HOME=/home/test", "AGENT_TOKEN=agent", "ISSUEQ_JOB_ID=job_1", "ISSUEQ_ATTEMPT=1", "GITHUB_ISSUE_NUMBER=1"} {
		if !strings.Contains(env, want) {
			t.Fatalf("env missing %q:\n%s", want, env)
		}
	}
	if strings.Contains(env, "GITHUB_TOKEN=secret") || strings.Contains(env, "UNLISTED_SECRET") {
		t.Fatalf("env leaked secret:\n%s", env)
	}
}

func TestBuildEnvClampsZeroAttemptToFirstAttempt(t *testing.T) {
	cfg := testConfig(t, []string{"/bin/echo"})
	job := testJob()
	job.Attempts = 0
	paths := PreparePaths(cfg.Workdir.Path, job.ID)
	env := strings.Join(BuildEnv(cfg, cfg.Routes[0], job, testIssue("title"), paths), "\n")
	if !strings.Contains(env, "ISSUEQ_ATTEMPT=1") {
		t.Fatalf("env = %s, want ISSUEQ_ATTEMPT=1", env)
	}
}

func TestPreparePathsAndWriteContext(t *testing.T) {
	paths := PreparePaths(t.TempDir(), "job_1")
	ctx := Context{Issue: testIssue("title"), Job: JobContext{ID: "job_1", Route: "code", Kind: "code", Attempt: 1, MaxAttempts: 3}, Runner: model.RunnerInfo{ID: "runner-1"}}
	if err := WriteContext(paths, ctx); err != nil {
		t.Fatal(err)
	}
	var got Context
	data, err := os.ReadFile(paths.ContextPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Job.ID != "job_1" || got.Job.Attempt != 1 || got.Runner.ID != "runner-1" || got.Issue.Title != "title" {
		t.Fatalf("context = %#v", got)
	}
}

func testConfig(t *testing.T, command []string) config.Config {
	t.Helper()
	return config.Config{
		Runner:  config.RunnerConfig{Name: "runner", Env: config.EnvConfig{Pass: []string{"PATH", "HOME"}}},
		GitHub:  config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo", TokenEnv: "GITHUB_TOKEN"},
		Workdir: config.WorkdirConfig{Path: filepath.Join(t.TempDir(), ".issueq")},
		Routes:  []config.RouteConfig{{Name: "code", Job: config.JobConfig{Kind: "code", Command: command, Timeout: config.Duration{Duration: time.Second}, MaxAttempts: 3}}},
	}
}

func testJob() model.Job {
	return model.Job{ID: "job_1", IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Attempts: 1}
}

func testIssue(title string) model.IssueSnapshot {
	return model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: title, Labels: []string{"agent-ready"}, State: "open"}
}
