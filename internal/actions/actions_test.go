package actions

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

func TestMergeConcatenatesComments(t *testing.T) {
	got := Merge(config.ActionConfig{Comment: "base"}, ResultAction{Comment: "result"})
	if got.Comment != "base\n\nresult" {
		t.Fatalf("comment = %q", got.Comment)
	}
}

func TestMergeResultFileLabelOpsWinConflicts(t *testing.T) {
	got := Merge(config.ActionConfig{LabelsAdd: []string{"agent-review"}, LabelsRemove: []string{"agent-running"}}, ResultAction{LabelsAdd: []string{"agent-running"}, LabelsRemove: []string{"agent-review"}})
	if strings.Join(got.LabelsAdd, ",") != "agent-running" {
		t.Fatalf("labels add = %#v", got.LabelsAdd)
	}
	if strings.Join(got.LabelsRemove, ",") != "agent-review" {
		t.Fatalf("labels remove = %#v", got.LabelsRemove)
	}
}

func TestApplyRefreshesAfterMutationsAndDetectsRealLabelChange(t *testing.T) {
	ctx := context.Background()
	queue, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "issueq.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	issue := testIssue([]string{"agent-ready"})
	final := testIssue([]string{"agent-running"})
	final.GitHubUpdatedAt = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	gh := &fakeClient{issues: []model.IssueSnapshot{issue, final}}

	result, err := Apply(ctx, testConfig(), gh, queue, issue, config.ActionConfig{LabelsRemove: []string{"agent-ready"}, LabelsAdd: []string{"agent-running"}, Comment: "started"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed {
		t.Fatal("Changed = false, want true")
	}
	if strings.Join(result.UpdatedIssue.Labels, ",") != "agent-running" || !result.UpdatedIssue.GitHubUpdatedAt.Equal(final.GitHubUpdatedAt) {
		t.Fatalf("updated issue = %#v", result.UpdatedIssue)
	}
	stored, err := queue.GetIssue(ctx, issue.IssueKey)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(stored.Labels, ",") != "agent-running" || !stored.GitHubUpdatedAt.Equal(final.GitHubUpdatedAt) {
		t.Fatalf("stored issue = %#v", stored)
	}
	if got := strings.Join(gh.calls, "|"); got != "get|set:agent-running|comment|get" {
		t.Fatalf("calls = %s", got)
	}
}

func TestApplySetsLabelsAtomically(t *testing.T) {
	ctx := context.Background()
	queue, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "issueq.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	issue := testIssue([]string{"agent-ready", "agent-route-pr-review", "agent-result"})
	final := testIssue([]string{"agent-ready", "agent-route-pr-fix", "agent-result"})
	gh := &fakeClient{issues: []model.IssueSnapshot{issue, final}}

	_, err = Apply(ctx, testConfig(), gh, queue, issue, config.ActionConfig{LabelsRemove: []string{"agent-route-pr-review"}, LabelsAdd: []string{"agent-route-pr-fix", "agent-ready"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(gh.calls, "|"); got != "get|set:agent-ready,agent-result,agent-route-pr-fix|get" {
		t.Fatalf("calls = %s", got)
	}
}

func TestApplyNoOpLabelsAndCommentsDoNotCountAsChanged(t *testing.T) {
	ctx := context.Background()
	queue, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "issueq.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	issue := testIssue([]string{"agent-ready"})
	gh := &fakeClient{issues: []model.IssueSnapshot{issue, issue}}

	result, err := Apply(ctx, testConfig(), gh, queue, issue, config.ActionConfig{LabelsRemove: []string{"missing"}, LabelsAdd: []string{"agent-ready"}, Comment: "note"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Changed {
		t.Fatal("Changed = true, want false")
	}
	if got := strings.Join(gh.calls, "|"); got != "get|set:agent-ready|comment|get" {
		t.Fatalf("calls = %s", got)
	}
}

func TestApplyWithHooksChecksBeforeEverySideEffect(t *testing.T) {
	ctx := context.Background()
	queue, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "issueq.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	issue := testIssue([]string{"agent-ready"})
	gh := &fakeClient{issues: []model.IssueSnapshot{issue}}
	checks := 0
	_, err = ApplyWithHooks(ctx, testConfig(), gh, queue, issue, config.ActionConfig{LabelsRemove: []string{"agent-ready"}, LabelsAdd: []string{"agent-running"}, Comment: "started"}, ApplyHooks{BeforeSideEffect: func() error {
		checks++
		if checks == 3 {
			return errors.New("lost")
		}
		return nil
	}})
	if err == nil || err.Error() != "lost" {
		t.Fatalf("err = %v, want lost", err)
	}
	if got := strings.Join(gh.calls, "|"); got != "get|set:agent-running" {
		t.Fatalf("calls = %s", got)
	}
}

func testConfig() config.Config {
	return config.Config{GitHub: config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo"}}
}

func testIssue(labels []string) model.IssueSnapshot {
	return model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Host: "github.com", Owner: "example-org", Repo: "example-repo", Number: 1, Title: "Issue", Labels: labels, State: "open"}
}

type fakeClient struct {
	issues []model.IssueSnapshot
	calls  []string
}

func (f *fakeClient) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	return nil, nil
}

func (f *fakeClient) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]model.IssueComment, error) {
	return nil, nil
}

func (f *fakeClient) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	f.calls = append(f.calls, "get")
	if len(f.issues) == 0 {
		return model.IssueSnapshot{}, nil
	}
	issue := f.issues[0]
	f.issues = f.issues[1:]
	return issue, nil
}

func (f *fakeClient) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "add:"+strings.Join(labels, ","))
	return nil
}

func (f *fakeClient) SetLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "set:"+strings.Join(labels, ","))
	return nil
}

func (f *fakeClient) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "remove:"+strings.Join(labels, ","))
	return nil
}

func (f *fakeClient) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	f.calls = append(f.calls, "comment")
	return nil
}

func (f *fakeClient) UpdateComment(ctx context.Context, owner, repo string, commentID string, body string) error {
	return nil
}

func TestParseResultFileErrors(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, found, err := ParseResultFile(bad)
	if !found || err == nil || !strings.Contains(err.Error(), "parse result JSON") {
		t.Fatalf("found=%v err=%v", found, err)
	}
	enqueue := filepath.Join(dir, "enqueue.json")
	if err := os.WriteFile(enqueue, []byte(`{"enqueue": []}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, found, err = ParseResultFile(enqueue)
	if !found || err == nil || !strings.Contains(err.Error(), `unsupported result JSON field "enqueue"`) {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func TestParseResultFileAcceptsWorkStarted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")
	if err := os.WriteFile(path, []byte(`{"comment":"blocked","work_started":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	result, found, err := ParseResultFile(path)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if result.WorkStarted == nil || *result.WorkStarted {
		t.Fatalf("WorkStarted = %#v, want false", result.WorkStarted)
	}
}

func TestParseWorkStartedFileIgnoresOtherFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")
	if err := os.WriteFile(path, []byte(`{"enqueue":[],"work_started":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	workStarted, found, err := ParseWorkStartedFile(path)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if workStarted == nil || *workStarted {
		t.Fatalf("workStarted = %#v, want false", workStarted)
	}
}
