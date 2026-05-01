package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/runner"
	storepkg "issueq/internal/store"
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

func TestDispatchLocalMalformedResultFailsJob(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, `#!/bin/sh
printf '{' > "$2"
`)
	defer store.Close()
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 || result.Succeeded != 0 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusFailed || !strings.Contains(jobs[0].LastError, "parse result JSON") {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchLocalRunsJobsConcurrently(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	marker := filepath.Join(dir, "started")
	script := fmt.Sprintf(`#!/bin/sh
printf %%s $$ > %q.$$
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  set -- %q.*
  if [ -e "$1" ] && [ -e "$2" ]; then exit 0; fi
  sleep 0.05
done
exit 42
`, marker, marker)
	store, cfg := setupDispatch(t, script)
	defer store.Close()
	cfg.Queue.MaxGlobalConcurrency = 2
	cfg.Routes[0].Job.Concurrency = 2
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe-2"}); err != nil {
		t.Fatal(err)
	}
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 2 || result.Succeeded != 2 {
		t.Fatalf("result = %#v", result)
	}
	assertOverlappedStarts(t, marker)
}

func assertOverlappedStarts(t *testing.T, marker string) {
	t.Helper()
	matches, err := filepath.Glob(marker + ".*")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("started marker count = %d, want 2 (%#v)", len(matches), matches)
	}
}

func TestDispatchLocalGlobalConcurrencyOneIsSerial(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, `#!/bin/sh
sleep 0.12
`)
	defer store.Close()
	cfg.Queue.MaxGlobalConcurrency = 1
	cfg.Routes[0].Job.Concurrency = 2
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe-2"}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if result.Claimed != 2 || result.Succeeded != 2 {
		t.Fatalf("result = %#v", result)
	}
	if elapsed < 220*time.Millisecond {
		t.Fatalf("dispatch took %s, want serial jobs", elapsed)
	}
}

func TestDispatchLocalSameRouteConcurrencyOnePreventsOverlap(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, `#!/bin/sh
sleep 0.12
`)
	defer store.Close()
	cfg.Queue.MaxGlobalConcurrency = 2
	cfg.Routes[0].Job.Concurrency = 1
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe-2"}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if result.Claimed != 2 || result.Succeeded != 2 {
		t.Fatalf("result = %#v", result)
	}
	if elapsed < 220*time.Millisecond {
		t.Fatalf("dispatch took %s, want route-serial jobs", elapsed)
	}
}

func TestDispatchLocalHeartbeatsWhileJobsRun(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, `#!/bin/sh
sleep 0.12
`)
	defer store.Close()
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: time.Second}
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 1 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusSucceeded {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchLocalDrainsInitialBacklogButNotNewJobs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	script := filepath.Join(dir, "task.sh")
	marker := filepath.Join(dir, "marker")
	body := "#!/bin/sh\nif [ ! -f " + marker + " ]; then touch " + marker + "; fi\n"
	store, cfg := setupDispatch(t, body)
	defer store.Close()
	cfg.Queue.MaxGlobalConcurrency = 1
	cfg.Routes[0].Job.Concurrency = 1
	cfg.Routes[0].Job.Command = []string{script}
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "initial-2"}); err != nil {
		t.Fatal(err)
	}
	future, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 10, DedupeKey: "future", AvailableAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Dispatch(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 2 || result.Succeeded != 2 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	for _, job := range jobs {
		if job.ID == future.ID && job.Status != model.JobStatusPending {
			t.Fatalf("future job status = %s, want pending", job.Status)
		}
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
	issue     model.IssueSnapshot
	calls     []string
	comments  []string
	errOn     map[string]error
	errOnCall map[string]int
	afterCall func(name string)
}

func (f *fakeGitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	return []model.IssueSnapshot{f.issue}, nil
}
func (f *fakeGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	f.calls = append(f.calls, "get")
	if err := f.callError("get"); err != nil {
		return model.IssueSnapshot{}, err
	}
	return f.issue, nil
}
func (f *fakeGitHub) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "add:"+strings.Join(labels, ","))
	if err := f.callError("add"); err != nil {
		return err
	}
	f.issue.Labels = append(f.issue.Labels, labels...)
	return nil
}
func (f *fakeGitHub) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "remove:"+strings.Join(labels, ","))
	if err := f.callError("remove"); err != nil {
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
	if err := f.callError("comment"); err != nil {
		return err
	}
	f.comments = append(f.comments, body)
	return nil
}

func (f *fakeGitHub) callError(name string) error {
	defer func() {
		if f.afterCall != nil {
			f.afterCall(name)
		}
	}()
	if f.errOnCall != nil {
		f.errOnCall[name]--
		if f.errOnCall[name] == 0 {
			return f.errOn[name]
		}
		return nil
	}
	return f.errOn[name]
}

func TestDispatchWithGitHubRunsJobsConcurrently(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	marker := filepath.Join(dir, "started")
	script := fmt.Sprintf(`#!/bin/sh
printf %%s $$ > %q.$$
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  set -- %q.*
  if [ -e "$1" ] && [ -e "$2" ]; then exit 0; fi
  sleep 0.05
done
exit 42
`, marker, marker)
	store, cfg := setupDispatch(t, script)
	defer store.Close()
	cfg.Queue.MaxGlobalConcurrency = 2
	cfg.Routes[0].Job.Concurrency = 2
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe-2"}); err != nil {
		t.Fatal(err)
	}
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 2 || result.Succeeded != 2 {
		t.Fatalf("result = %#v", result)
	}
	assertOverlappedStarts(t, marker)
}

func TestDispatchWithGitHubLostOwnershipBeforeOnStartSkipsGitHubAction(t *testing.T) {
	ctx := context.Background()
	baseStore, cfg := setupDispatch(t, "#!/bin/sh\necho should-not-run\n")
	defer baseStore.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.OnStart = config.ActionConfig{LabelsAdd: []string{"agent-running"}, Comment: "starting"}
	wrapped := &renewInjectStore{Store: baseStore}
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	gh.afterCall = func(name string) {
		if name != "get" {
			return
		}
		jobs, _ := baseStore.ListJobs(ctx)
		if len(jobs) == 0 || jobs[0].RunnerInstanceID == "" {
			return
		}
		wrapped.renewErrs = []error{nil, nil, storepkg.ErrNotOwner}
		gh.afterCall = nil
	}
	result, err := DispatchWithGitHub(ctx, cfg, wrapped, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Succeeded != 0 || result.Failed != 0 || result.Skipped != 0 || result.Dead != 0 {
		t.Fatalf("result = %#v", result)
	}
	for _, call := range gh.calls {
		if strings.HasPrefix(call, "add:") || call == "comment" || strings.HasPrefix(call, "remove:") {
			t.Fatalf("unexpected GitHub side effect calls = %#v", gh.calls)
		}
	}
	jobs, _ := baseStore.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusRunning || jobs[0].Attempts != 1 {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchWithGitHubLostOwnershipBeforeResultActionSkipsGitHubAction(t *testing.T) {
	ctx := context.Background()
	baseStore, cfg := setupDispatch(t, `#!/bin/sh
cat > "$2" <<'JSON'
{"comment":"done","labels_add":["agent-review"]}
JSON
`)
	defer baseStore.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	wrapped := &renewInjectStore{Store: baseStore, renewErrs: []error{nil, nil, nil, nil, nil, storepkg.ErrLostLease}}
	proc := newDispatcherFakeProcess(3001)
	fakeStarter := &dispatcherFakeStarter{processes: []*dispatcherFakeProcess{proc}}
	restore := runner.SetProcessStarterForTest(fakeStarter)
	defer restore()
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	go func() { proc.waitCh <- nil }()
	result, err := DispatchWithGitHub(ctx, cfg, wrapped, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Succeeded != 0 || result.Failed != 0 || result.Dead != 0 {
		t.Fatalf("result = %#v", result)
	}
	for _, call := range gh.calls {
		if strings.HasPrefix(call, "add:agent-review") || call == "comment" {
			t.Fatalf("unexpected result GitHub side effect calls = %#v", gh.calls)
		}
	}
	jobs, _ := baseStore.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusRunning {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchWithGitHubLostOwnershipBeforeAttemptsPreventsAttemptMutation(t *testing.T) {
	ctx := context.Background()
	baseStore, cfg := setupDispatch(t, "#!/bin/sh\necho should-not-run\n")
	defer baseStore.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	wrapped := &renewInjectStore{Store: baseStore, incrementAttemptsErrs: []error{storepkg.ErrNotOwner}}
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, wrapped, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Succeeded != 0 || result.Failed != 0 || result.Dead != 0 || result.Skipped != 0 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := baseStore.ListJobs(ctx)
	if jobs[0].Attempts != 0 || jobs[0].Status != model.JobStatusRunning {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchWithGitHubStartFailureAppliesFailureAction(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\necho should-not-run\n")
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.Command = nil
	cfg.Routes[0].Job.OnFailure = config.ActionConfig{LabelsAdd: []string{"agent-failed"}, Comment: "failed"}
	gh := &fakeGitHub{issue: model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"}}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	if len(gh.comments) != 1 || gh.comments[0] != "failed" {
		t.Fatalf("comments = %#v calls = %#v", gh.comments, gh.calls)
	}
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

func TestTransitionLimitTerminalActionErrorFailsJob(t *testing.T) {
	ctx := context.Background()
	store, cfg := setupDispatch(t, "#!/bin/sh\nexit 0\n")
	defer store.Close()
	cfg.Routes[0].When = config.PredicateConfig{LabelsInclude: []string{"agent-ready"}}
	cfg.Routes[0].Job.OnStart = config.ActionConfig{LabelsRemove: []string{"agent-ready"}, LabelsAdd: []string{"agent-running"}}
	cfg.Workflow.MaxTransitionsPerIssue = -1
	cfg.Workflow.OnTransitionsExceeded = config.ActionConfig{LabelsAdd: []string{"agent-failed"}, Comment: "terminal"}
	gh := &fakeGitHub{
		issue:     model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: []string{"agent-ready"}, State: "open"},
		errOn:     map[string]error{"add": errors.New("terminal add failed")},
		errOnCall: map[string]int{"add": 2},
	}
	result, err := DispatchWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 || result.Dead != 0 {
		t.Fatalf("result = %#v", result)
	}
	jobs, _ := store.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusFailed || !strings.Contains(jobs[0].LastError, "apply transitions-exceeded actions") {
		t.Fatalf("job = %#v", jobs[0])
	}
	events, err := store.ListJobEvents(ctx, jobs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "terminal_action_failed") {
		t.Fatalf("events = %#v", events)
	}
}

type dispatcherFakeStarter struct {
	processes []*dispatcherFakeProcess
	started   int
}

func (s *dispatcherFakeStarter) StartProcess(spec runner.ProcessSpec) (runner.Process, error) {
	if s.started >= len(s.processes) {
		return nil, errors.New("unexpected process start")
	}
	proc := s.processes[s.started]
	s.started++
	return proc, nil
}

type dispatcherFakeProcess struct {
	pid      int
	waitCh   chan error
	killOnce sync.Once
	killCh   chan struct{}
}

func newDispatcherFakeProcess(pid int) *dispatcherFakeProcess {
	return &dispatcherFakeProcess{pid: pid, waitCh: make(chan error, 1), killCh: make(chan struct{})}
}

func (p *dispatcherFakeProcess) PID() int { return p.pid }
func (p *dispatcherFakeProcess) Wait() error {
	return <-p.waitCh
}
func (p *dispatcherFakeProcess) KillTree() error {
	p.killOnce.Do(func() {
		close(p.killCh)
		p.waitCh <- errors.New("killed")
	})
	return nil
}

type renewInjectStore struct {
	*sqlitestore.Store
	assertErrs                []error
	assertCalls               int
	incrementAttemptsErrs     []error
	incrementAttemptsCalls    int
	incrementTransitionsErrs  []error
	incrementTransitionsCalls int
	renewErrs                 []error
	renewCalls                int
	onRenew                   func(call int)
	artifactErrs              []error
	artifactCalls             int
	finalizeErrs              []error
	finalizeCalls             int
	finalizedJobIDs           []string
	artifactJobIDs            []string
}

func (s *renewInjectStore) AssertJobOwned(ctx context.Context, jobID, runnerInstanceID string) error {
	s.assertCalls++
	call := s.assertCalls
	if call <= len(s.assertErrs) && s.assertErrs[call-1] != nil {
		return s.assertErrs[call-1]
	}
	return s.Store.AssertJobOwned(ctx, jobID, runnerInstanceID)
}

func (s *renewInjectStore) IncrementAttemptsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string, generation int, routeName string) (int, error) {
	s.incrementAttemptsCalls++
	call := s.incrementAttemptsCalls
	if call <= len(s.incrementAttemptsErrs) && s.incrementAttemptsErrs[call-1] != nil {
		return 0, s.incrementAttemptsErrs[call-1]
	}
	return s.Store.IncrementAttemptsForJob(ctx, jobID, runnerInstanceID, issueKey, generation, routeName)
}

func (s *renewInjectStore) IncrementTransitionsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string) (int, error) {
	s.incrementTransitionsCalls++
	call := s.incrementTransitionsCalls
	if call <= len(s.incrementTransitionsErrs) && s.incrementTransitionsErrs[call-1] != nil {
		return 0, s.incrementTransitionsErrs[call-1]
	}
	return s.Store.IncrementTransitionsForJob(ctx, jobID, runnerInstanceID, issueKey)
}

func (s *renewInjectStore) RenewJobLease(ctx context.Context, jobID, runnerInstanceID string, leaseDuration time.Duration) error {
	s.renewCalls++
	call := s.renewCalls
	if s.onRenew != nil {
		s.onRenew(call)
	}
	if call <= len(s.renewErrs) && s.renewErrs[call-1] != nil {
		return s.renewErrs[call-1]
	}
	return s.Store.RenewJobLease(ctx, jobID, runnerInstanceID, leaseDuration)
}

func (s *renewInjectStore) UpdateJobArtifactsOwned(ctx context.Context, jobID, runnerInstanceID, contextPath, resultPath, stdoutPath, stderrPath string, pid int) error {
	s.artifactCalls++
	s.artifactJobIDs = append(s.artifactJobIDs, jobID)
	call := s.artifactCalls
	if call <= len(s.artifactErrs) && s.artifactErrs[call-1] != nil {
		return s.artifactErrs[call-1]
	}
	return s.Store.UpdateJobArtifactsOwned(ctx, jobID, runnerInstanceID, contextPath, resultPath, stdoutPath, stderrPath, pid)
}

func (s *renewInjectStore) FinalizeJobOwned(ctx context.Context, jobID string, runnerInstanceID string, result model.JobFinalize) error {
	s.finalizeCalls++
	s.finalizedJobIDs = append(s.finalizedJobIDs, jobID)
	call := s.finalizeCalls
	if call <= len(s.finalizeErrs) && s.finalizeErrs[call-1] != nil {
		return s.finalizeErrs[call-1]
	}
	return s.Store.FinalizeJobOwned(ctx, jobID, runnerInstanceID, result)
}

func TestDispatchLocalTransientRenewalErrorRetriesWithoutKillingJob(t *testing.T) {
	ctx := context.Background()
	baseStore, cfg := setupDispatch(t, "#!/bin/sh\nexit 0\n")
	defer baseStore.Close()
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: time.Second}
	proc := newDispatcherFakeProcess(2001)
	fakeStarter := &dispatcherFakeStarter{processes: []*dispatcherFakeProcess{proc}}
	restore := runner.SetProcessStarterForTest(fakeStarter)
	defer restore()
	wrapped := &renewInjectStore{
		Store:     baseStore,
		renewErrs: []error{errors.New("temporary renew failure"), nil},
	}
	wrapped.onRenew = func(call int) {
		if call == 2 {
			proc.waitCh <- nil
		}
	}
	result, err := Dispatch(ctx, cfg, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 1 || result.Failed != 0 || result.Claimed != 1 {
		t.Fatalf("result = %#v", result)
	}
	if wrapped.renewCalls < 2 {
		t.Fatalf("renew calls = %d, want retry", wrapped.renewCalls)
	}
	select {
	case <-proc.killCh:
		t.Fatal("process killed after transient renewal failure")
	default:
	}
	jobs, _ := baseStore.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusSucceeded {
		t.Fatalf("job = %#v", jobs[0])
	}
}

func TestDispatchLocalLostOwnershipCancelsAndSkipsFinalization(t *testing.T) {
	ctx := context.Background()
	baseStore, cfg := setupDispatch(t, "#!/bin/sh\nexit 0\n")
	defer baseStore.Close()
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: time.Second}
	proc := newDispatcherFakeProcess(2002)
	fakeStarter := &dispatcherFakeStarter{processes: []*dispatcherFakeProcess{proc}}
	restore := runner.SetProcessStarterForTest(fakeStarter)
	defer restore()
	wrapped := &renewInjectStore{Store: baseStore, renewErrs: []error{storepkg.ErrLostLease}}
	result, err := Dispatch(ctx, cfg, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 0 || result.Failed != 0 || result.Claimed != 1 {
		t.Fatalf("result = %#v", result)
	}
	select {
	case <-proc.killCh:
	case <-time.After(time.Second):
		t.Fatal("lost ownership did not kill process")
	}
	if wrapped.finalizeCalls != 0 {
		t.Fatalf("finalize calls = %d, want 0", wrapped.finalizeCalls)
	}
	if wrapped.artifactCalls != 1 {
		t.Fatalf("artifact calls = %d, want only initial update", wrapped.artifactCalls)
	}
	jobs, _ := baseStore.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusRunning {
		t.Fatalf("job status = %s, want running because stale owner did not finalize", jobs[0].Status)
	}
}

func TestDispatchLocalLostOwnershipCancelsOnlyAffectedJob(t *testing.T) {
	ctx := context.Background()
	baseStore, cfg := setupDispatch(t, "#!/bin/sh\nexit 0\n")
	defer baseStore.Close()
	cfg.Queue.MaxGlobalConcurrency = 2
	cfg.Queue.LeaseDuration = config.Duration{Duration: 500 * time.Millisecond}
	cfg.Routes[0].Job.Concurrency = 2
	cfg.Routes[0].Job.Timeout = config.Duration{Duration: time.Second}
	if _, _, err := baseStore.EnqueueJob(ctx, model.JobCreate{IssueKey: "github.com/example-org/example-repo#1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "dedupe-2"}); err != nil {
		t.Fatal(err)
	}
	proc1 := newDispatcherFakeProcess(2005)
	proc2 := newDispatcherFakeProcess(2006)
	fakeStarter := &dispatcherFakeStarter{processes: []*dispatcherFakeProcess{proc1, proc2}}
	restore := runner.SetProcessStarterForTest(fakeStarter)
	defer restore()
	wrapped := &renewInjectStore{Store: baseStore, renewErrs: []error{storepkg.ErrNotOwner}}
	wrapped.onRenew = func(call int) {
		if call == 2 {
			proc1.waitCh <- nil
			proc2.waitCh <- nil
		}
	}
	result, err := Dispatch(ctx, cfg, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 2 || result.Succeeded != 1 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
	killed := 0
	for _, proc := range []*dispatcherFakeProcess{proc1, proc2} {
		select {
		case <-proc.killCh:
			killed++
		default:
		}
	}
	if killed != 1 {
		t.Fatalf("killed processes = %d, want 1", killed)
	}
	jobs, _ := baseStore.ListJobs(ctx)
	succeeded := 0
	running := 0
	for _, job := range jobs {
		switch job.Status {
		case model.JobStatusSucceeded:
			succeeded++
		case model.JobStatusRunning:
			running++
		}
	}
	if succeeded != 1 || running != 1 {
		t.Fatalf("jobs = %#v, want one succeeded and one still running", jobs)
	}
}

func TestDispatchLocalOwnershipLossOnReapSkipsStaleWrites(t *testing.T) {
	ctx := context.Background()
	baseStore, cfg := setupDispatch(t, "#!/bin/sh\nexit 0\n")
	defer baseStore.Close()
	proc := newDispatcherFakeProcess(2003)
	fakeStarter := &dispatcherFakeStarter{processes: []*dispatcherFakeProcess{proc}}
	restore := runner.SetProcessStarterForTest(fakeStarter)
	defer restore()
	wrapped := &renewInjectStore{Store: baseStore, artifactErrs: []error{nil, storepkg.ErrNotOwner}}
	go func() { proc.waitCh <- nil }()
	result, err := Dispatch(ctx, cfg, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 0 || result.Failed != 0 || result.Claimed != 1 {
		t.Fatalf("result = %#v", result)
	}
	if wrapped.finalizeCalls != 0 {
		t.Fatalf("finalize calls = %d, want 0", wrapped.finalizeCalls)
	}
	jobs, _ := baseStore.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusRunning {
		t.Fatalf("job status = %s, want running because stale owner did not finalize", jobs[0].Status)
	}
}

func TestDispatchLocalFinalizeOwnershipLossSkipsResultCounting(t *testing.T) {
	ctx := context.Background()
	baseStore, cfg := setupDispatch(t, "#!/bin/sh\nexit 0\n")
	defer baseStore.Close()
	proc := newDispatcherFakeProcess(2004)
	fakeStarter := &dispatcherFakeStarter{processes: []*dispatcherFakeProcess{proc}}
	restore := runner.SetProcessStarterForTest(fakeStarter)
	defer restore()
	wrapped := &renewInjectStore{Store: baseStore, finalizeErrs: []error{storepkg.ErrLostLease}}
	go func() { proc.waitCh <- nil }()
	result, err := Dispatch(ctx, cfg, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 0 || result.Failed != 0 || result.Claimed != 1 {
		t.Fatalf("result = %#v", result)
	}
	if wrapped.finalizeCalls != 1 {
		t.Fatalf("finalize calls = %d, want 1", wrapped.finalizeCalls)
	}
	jobs, _ := baseStore.ListJobs(ctx)
	if jobs[0].Status != model.JobStatusRunning {
		t.Fatalf("job status = %s, want running because stale owner did not finalize", jobs[0].Status)
	}
}

func hasEvent(events []model.JobEvent, typ string) bool {
	for _, event := range events {
		if event.EventType == typ {
			return true
		}
	}
	return false
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
