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
	for _, want := range []string{"PATH=/bin", "HOME=/home/test", "AGENT_TOKEN=agent", "ISSUEQ_JOB_ID=job_1", "GITHUB_ISSUE_NUMBER=1"} {
		if !strings.Contains(env, want) {
			t.Fatalf("env missing %q:\n%s", want, env)
		}
	}
	if strings.Contains(env, "GITHUB_TOKEN=secret") || strings.Contains(env, "UNLISTED_SECRET") {
		t.Fatalf("env leaked secret:\n%s", env)
	}
}

func TestRunSuccessWritesContextStdoutStderrAndResult(t *testing.T) {
	script := writeScript(t, `#!/bin/sh
echo stdout-line
echo stderr-line >&2
python3 - "$1" "$2" <<'PY'
import json, sys
ctx=json.load(open(sys.argv[1]))
open(sys.argv[2], 'w').write(json.dumps({'job': ctx['job']['id'], 'route': ctx['job']['route']}))
PY
`)
	cfg := testConfig(t, []string{script})
	res := Run(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{ID: "runner-1", Name: "runner"})
	if res.Error != nil || res.ExitCode != 0 {
		t.Fatalf("Run error=%v exit=%d", res.Error, res.ExitCode)
	}
	assertFileContains(t, res.Paths.StdoutPath, "stdout-line")
	assertFileContains(t, res.Paths.StderrPath, "stderr-line")
	assertFileContains(t, res.Paths.ResultPath, "job_1")
	var ctxData Context
	data, err := os.ReadFile(res.Paths.ContextPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &ctxData); err != nil {
		t.Fatal(err)
	}
	if ctxData.Issue.Title != "title" || ctxData.Job.ID != "job_1" || ctxData.Runner.ID != "runner-1" {
		t.Fatalf("context = %#v", ctxData)
	}
}

func TestRunFailureAndTimeout(t *testing.T) {
	fail := writeScript(t, "#!/bin/sh\necho bad >&2\nexit 1\n")
	cfg := testConfig(t, []string{fail})
	res := Run(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	if res.Error == nil || res.ExitCode != 1 || res.TimedOut {
		t.Fatalf("failure result = %#v", res)
	}
	assertFileContains(t, res.Paths.StderrPath, "bad")

	sleep := writeScript(t, "#!/bin/sh\nsleep 2\n")
	cfg = testConfig(t, []string{sleep})
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: 20 * time.Millisecond}
	res = Run(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	if !res.TimedOut || res.Error == nil || !strings.Contains(res.Error.Error(), "timed out") {
		t.Fatalf("timeout result = %#v", res)
	}
}

func TestIssueContentDoesNotAlterCommandArgv(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "pwned")
	script := writeScript(t, "#!/bin/sh\necho safe\n")
	cfg := testConfig(t, []string{script})
	issue := testIssue("bad; touch " + marker)
	res := Run(t.Context(), cfg, cfg.Routes[0], testJob(), issue, model.RunnerInfo{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("marker exists or stat error = %v", err)
	}
}

func writeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "task.sh")
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
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
	return model.Job{ID: "job_1", IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code"}
}

func testIssue(title string) model.IssueSnapshot {
	return model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: title, Labels: []string{"agent-ready"}, State: "open"}
}

func assertFileContains(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), want) {
		t.Fatalf("%s = %q, want substring %q", path, data, want)
	}
}
