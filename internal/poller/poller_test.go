package poller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
)

type fakeGitHub struct {
	issues   []model.IssueSnapshot
	comments map[int][]model.IssueComment
	err      error
}

func (f fakeGitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.issues, nil
}
func (f fakeGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]model.IssueComment, error) {
	return f.comments[number], nil
}
func (f fakeGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	return model.IssueSnapshot{}, nil
}
func (f fakeGitHub) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	return nil
}
func (f fakeGitHub) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	return nil
}
func (f fakeGitHub) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	return nil
}

func TestPollUpsertsFakeGitHubIssues(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	updated := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	client := fakeGitHub{issues: []model.IssueSnapshot{{
		IssueKey:        "github.com/example-org/example-repo#123",
		NodeID:          "node-123",
		Host:            "github.com",
		Owner:           "example-org",
		Repo:            "example-repo",
		Number:          123,
		Title:           "Add CSV export",
		Body:            "body text",
		Labels:          []string{"agent-ready", "bug"},
		State:           "open",
		GitHubUpdatedAt: updated,
	}}}

	result, err := Poll(ctx, testConfig(), client, store)
	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if result.IssuesFetched != 1 || result.IssuesUpserted != 1 {
		t.Fatalf("result = %#v", result)
	}
	issues, err := store.ListIssues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 {
		t.Fatalf("issues len = %d", len(issues))
	}
	got := issues[0]
	if got.IssueKey != "github.com/example-org/example-repo#123" || got.Title != "Add CSV export" || got.Body != "body text" || got.State != "open" {
		t.Fatalf("issue fields not preserved: %#v", got)
	}
	if len(got.Labels) != 2 || got.Labels[0] != "agent-ready" || got.Labels[1] != "bug" {
		t.Fatalf("labels = %#v", got.Labels)
	}
	if !got.GitHubUpdatedAt.Equal(updated) {
		t.Fatalf("updated = %s, want %s", got.GitHubUpdatedAt, updated)
	}
	if got.SyncedAt.IsZero() {
		t.Fatal("SyncedAt is zero")
	}
}

func TestPollFillsIssueKeyFormat(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	client := fakeGitHub{issues: []model.IssueSnapshot{{Number: 7, Title: "T", State: "open"}}}
	if _, err := Poll(ctx, testConfig(), client, store); err != nil {
		t.Fatal(err)
	}
	issues, err := store.ListIssues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := issues[0].IssueKey; got != "github.com/example-org/example-repo#7" {
		t.Fatalf("issue key = %q", got)
	}
}

func TestPollHandlesEmptyIssueList(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	result, err := Poll(ctx, testConfig(), fakeGitHub{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.IssuesFetched != 0 || result.IssuesUpserted != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestPollReportsGitHubErrorsClearly(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	_, err := Poll(ctx, testConfig(), fakeGitHub{err: errors.New("rate limited")}, store)
	if err == nil {
		t.Fatal("Poll() error = nil")
	}
	if !strings.Contains(err.Error(), "list GitHub issues: rate limited") {
		t.Fatalf("error = %v", err)
	}
}

func TestPollIngestsHandoffCommentsIdempotently(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	updated := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	commented := time.Date(2026, 1, 2, 4, 0, 0, 0, time.UTC)
	client := fakeGitHub{
		issues: []model.IssueSnapshot{{
			Host:            "github.com",
			Owner:           "example-org",
			Repo:            "example-repo",
			Number:          123,
			Title:           "Add CSV export",
			Labels:          []string{"agent-ready"},
			State:           "open",
			GitHubUpdatedAt: updated,
		}},
		comments: map[int][]model.IssueComment{123: {{Body: handoffCommentPayload(), CreatedAt: commented}}},
	}
	result, err := Poll(ctx, testConfig(), client, store)
	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if result.HandoffsFound != 1 || result.HandoffsInserted != 1 {
		t.Fatalf("handoff result = %#v", result)
	}
	again, err := Poll(ctx, testConfig(), client, store)
	if err != nil {
		t.Fatalf("second Poll() error = %v", err)
	}
	if again.HandoffsFound != 1 || again.HandoffsInserted != 0 {
		t.Fatalf("second handoff result = %#v", again)
	}
	handoffs, err := store.ListHandoffsForIssue(ctx, "github.com/example-org/example-repo#123")
	if err != nil {
		t.Fatalf("ListHandoffsForIssue error = %v", err)
	}
	if len(handoffs) != 1 || handoffs[0].RouteName != "triage" || handoffs[0].Decision != "accepted" || handoffs[0].NextRoute != "fix" {
		t.Fatalf("handoffs = %#v", handoffs)
	}
}

func handoffCommentPayload() string {
	return "```issueq-handoff\n" + `{
  "schema": "issueq-handoff/v1",
  "schema_version": "1",
  "route": "triage",
  "decision": "accepted",
  "next_route": "fix",
  "source": {"kind": "github_issue", "issue_number": 123, "body_sha256": "abc"},
  "target": {"kind": "github_issue", "issue_number": 123}
}` + "\n```"
}

func testConfig() config.Config {
	return config.Config{GitHub: config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo"}}
}

func openStore(t *testing.T, ctx context.Context) *sqlitestore.Store {
	t.Helper()
	store, err := sqlitestore.Open(ctx, t.TempDir()+"/issueq.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return store
}
