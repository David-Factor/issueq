package dispatcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
)

func TestDispatchRunsLocalFixtureJobEndToEnd(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho hello\necho err >&2\nexit 0\n")
	defer store.Close()
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatalf("Dispatch error = %v", err)
	}
	if result.Claimed != 1 || result.Succeeded != 1 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job := jobs[0]
	if job.Status != model.JobStatusSucceeded || job.StdoutPath == "" || job.StderrPath == "" || job.ContextPath == "" {
		t.Fatalf("job = %#v", job)
	}
	assertContains(t, job.StdoutPath, "hello")
	assertContains(t, job.StderrPath, "err")
}

func TestDispatchMarksFailure(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho fail >&2\nexit 1\n")
	defer store.Close()
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusFailed || jobs[0].LastError == "" {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchTimeout(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\nsleep 2\n")
	defer store.Close()
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: 20 * time.Millisecond}
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusFailed || !strings.Contains(jobs[0].LastError, "timed out") {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func setupDispatch(t *testing.T, scriptBody string) (*sqlitestore.Store, config.Config) {
	t.Helper()
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "issueq.db"))
	if err != nil {
		t.Fatal(err)
	}
	issue := model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: issue.IssueKey, RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe"}); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "task.sh")
	if err := os.WriteFile(script, []byte(scriptBody), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Runner:  config.RunnerConfig{Name: "runner", Capabilities: []string{"code"}, Env: config.EnvConfig{Pass: []string{"PATH", "HOME"}}},
		Queue:   config.QueueConfig{MaxGlobalConcurrency: 1, LeaseDuration: config.Duration{Duration: time.Minute}},
		Workdir: config.WorkdirConfig{Path: filepath.Join(t.TempDir(), ".issueq")},
		GitHub:  config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo", TokenEnv: "GITHUB_TOKEN"},
		Routes:  []config.RouteConfig{{Name: "code", Job: config.JobConfig{Kind: "code", Command: []string{script}, Timeout: config.Duration{Duration: time.Second}, Concurrency: 1, MaxAttempts: 3}}},
	}
	return store, cfg
}

func assertContains(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), want) {
		t.Fatalf("%s = %q, want %q", path, data, want)
	}
}

type fakeGitHub struct {
	issue    model.IssueSnapshot
	calls    []string
	comments []string
	errOn    map[string]error
}

func (f *fakeGitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	return []model.IssueSnapshot{f.issue}, nil
}
func (f *fakeGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	f.calls = append(f.calls, "get")
	if err := f.errOn["get"]; err != nil {
		return model.IssueSnapshot{}, err
	}
	return f.issue, nil
}
func (f *fakeGitHub) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "add:"+strings.Join(labels, ","))
	if err := f.errOn["add"]; err != nil {
		return err
	}
	f.issue.Labels = append(f.issue.Labels, labels...)
	return nil
}
func (f *fakeGitHub) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "remove:"+strings.Join(labels, ","))
	if err := f.errOn["remove"]; err != nil {
		return err
	}
	blocked := map[string]bool{}
	for _, l := range labels {
		blocked[l] = true
	}
	out := f.issue.Labels[:0]
	for _, l := range f.issue.Labels {
		if !blocked[l] {
			out = append(out, l)
		}
	}
	f.issue.Labels = out
	return nil
}
func (f *fakeGitHub) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	f.calls = append(f.calls, "comment")
	if err := f.errOn["comment"]; err != nil {
		return err
	}
	f.comments = append(f.comments, body)
	return nil
}

func TestDispatchWithGitHubStaleJobSkipped(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\nexit 99\n")
	defer store.Close()
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"other"}, State: "open"}}
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Skipped != 1 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusSkipped {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchWithGitHubSuccessActionsAndResult(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, `#!/bin/sh
cat > "$2" <<'JSON'
{"comment":"PR #1","labels_add":["agent-review"],"labels_remove":["agent-running"]}
JSON
`)
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.OnStart = config.ActionConfig{LabelsRemove: []string{"agent-ready"}, LabelsAdd: []string{"agent-running"}}
	cfg.Routes[0].Job.OnSuccess = config.ActionConfig{LabelsRemove: []string{"agent-running"}, LabelsAdd: []string{"agent-review"}, Comment: "Implementation finished"}
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 1 {
		t.Fatalf("result = %#v", result)
	}
	callOrder := strings.Join(gh.calls, "|")
	if !strings.Contains(callOrder, "remove:agent-ready|add:agent-running") {
		t.Fatalf("on_start order/calls = %s", callOrder)
	}
	if len(gh.comments) != 1 || gh.comments[0] != "Implementation finished\n\nPR #1" {
		t.Fatalf("comments = %#v", gh.comments)
	}
	issues, _ := store.ListIssues(ctx)
	if strings.Join(issues[0].Labels, ",") != "agent-review" {
		t.Fatalf("local labels = %#v", issues[0].Labels)
	}
}

func TestDispatchWithGitHubFailureActionsAndBadResult(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, `#!/bin/sh
printf '{' > "$2"
exit 0
`)
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.OnFailure = config.ActionConfig{LabelsRemove: []string{"agent-running"}, LabelsAdd: []string{"agent-failed"}, Comment: "failed"}
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready", "agent-running"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if !strings.Contains(jobs[0].LastError, "parse result JSON") {
		t.Fatalf("job = %#v", jobs[0])
	}
	if len(gh.comments) != 1 || gh.comments[0] != "failed" {
		t.Fatalf("comments = %#v", gh.comments)
	}
}

func TestAttemptsWithinMaxSpawnsSubprocess(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, `#!/bin/sh
echo spawned
echo '{"comment":"attempt '$ISSUEQ_ATTEMPT'"}' > "$2"
`)
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.OnStart = config.ActionConfig{LabelsRemove: []string{"agent-ready"}, LabelsAdd: []string{"agent-running"}}
	cfg.Routes[0].Job.MaxAttempts = 1
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Attempts != 1 {
		t.Fatalf("job attempts = %d, want 1", jobs[0].Attempts)
	}
	assertContains(t, jobs[0].StdoutPath, "spawned")
	if len(gh.comments) != 1 || gh.comments[0] != "attempt 1" {
		t.Fatalf("comments = %#v", gh.comments)
	}
}

func TestAttemptsExceededDoesNotSpawnAndMarksDead(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho should-not-run\n")
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.MaxAttempts = 1
	cfg.Routes[0].Job.OnAttemptsExceeded = config.ActionConfig{LabelsAdd: []string{"agent-failed", "agent-needs-human"}, Comment: "too many"}
	_, err := store.IncrementAttempts(ctx, "github.com/example-org/example-repo#1", 0, "code")
	if err != nil {
		t.Fatal(err)
	}
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dead != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusDead || jobs[0].StdoutPath != "" {
		t.Fatalf("job = %#v", jobs[0])
	}
	if len(gh.comments) != 1 || gh.comments[0] != "too many" {
		t.Fatalf("comments = %#v", gh.comments)
	}
}

func TestPostClaimGitHubRefreshErrorFinalizesJob(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho should-not-run\n")
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	gh := &fakeGitHub{
		issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"},
		errOn: map[string]error{"get": errors.New("boom")},
	}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusFailed || !strings.Contains(jobs[0].LastError, "refresh issue") {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestPostClaimOnStartErrorFinalizesJob(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho should-not-run\n")
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.OnStart = config.ActionConfig{LabelsAdd: []string{"agent-running"}}
	gh := &fakeGitHub{
		issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"},
		errOn: map[string]error{"add": errors.New("add failed")},
	}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusFailed || !strings.Contains(jobs[0].LastError, "apply on_start actions") {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestTransitionLimitExceededAppliesTerminalAction(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\nexit 0\n")
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.OnStart = config.ActionConfig{LabelsRemove: []string{"agent-ready"}, LabelsAdd: []string{"agent-running"}}
	cfg.Workflow.MaxTransitionsPerIssue = -1
	cfg.Workflow.OnTransitionsExceeded = config.ActionConfig{LabelsAdd: []string{"agent-failed", "agent-needs-human"}, Comment: "terminal"}
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dead != 1 {
		t.Fatalf("result = %#v", result)
	}
	if len(gh.comments) == 0 || gh.comments[len(gh.comments)-1] != "terminal" {
		t.Fatalf("comments = %#v", gh.comments)
	}
}

func TestReviewLoopStopsAfterConfiguredAttempts(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho should-not-run\n")
	defer store.Close()
	cfg.Routes[0].Name = "review"
	cfg.Routes[0].Job.Kind = "code"
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-review"}}
	cfg.Routes[0].Job.MaxAttempts = 2
	jobs, _ := store.ListJobs(ctx)
	_ = store.FinalizeJob(ctx, jobs[0].ID, model.JobFinalize{Status: model.JobStatusCancelled})
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "review", Kind: "code", DedupeKey: "review"}); err != nil {
		t.Fatal(err)
	}
	_, _ = store.IncrementAttempts(ctx, "github.com/example-org/example-repo#1", 0, "review")
	_, _ = store.IncrementAttempts(ctx, "github.com/example-org/example-repo#1", 0, "review")
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-review"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Dead != 1 {
		t.Fatalf("result = %#v", result)
	}
}
