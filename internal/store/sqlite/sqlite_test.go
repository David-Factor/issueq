package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"issueq/internal/model"

	_ "modernc.org/sqlite"
)

func TestMigrationsCreateTablesAndAreIdempotent(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}
	for _, table := range []string{"issues", "jobs", "issue_state", "route_attempts", "job_events"} {
		var name string
		err := store.db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&name)
		if err != nil {
			t.Fatalf("table %s missing: %v", table, err)
		}
	}
}

func TestUpsertIssueInsertsThenUpdates(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()

	issue := sampleIssue()
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatalf("UpsertIssue insert error = %v", err)
	}
	issue.Title = "updated title"
	issue.Body = "updated body"
	issue.Labels = []string{"agent-ready", "bug"}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatalf("UpsertIssue update error = %v", err)
	}

	issues, err := store.ListIssues(ctx)
	if err != nil {
		t.Fatalf("ListIssues error = %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("len issues = %d, want 1", len(issues))
	}
	if issues[0].Title != "updated title" || issues[0].Body != "updated body" {
		t.Fatalf("issue not updated: %#v", issues[0])
	}
	if got := len(issues[0].Labels); got != 2 {
		t.Fatalf("labels len = %d", got)
	}

	var stateRows int
	if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM issue_state WHERE issue_key = ?", issue.IssueKey).Scan(&stateRows); err != nil {
		t.Fatal(err)
	}
	if stateRows != 1 {
		t.Fatalf("issue_state rows = %d, want 1", stateRows)
	}
}

func TestListRoutableIssuesOnlyOpen(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()

	open := sampleIssue()
	closed := sampleIssue()
	closed.IssueKey = "github.com/example-org/example-repo#2"
	closed.Number = 2
	closed.State = "closed"
	if err := store.UpsertIssue(ctx, open); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertIssue(ctx, closed); err != nil {
		t.Fatal(err)
	}

	issues, err := store.ListRoutableIssues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].IssueKey != open.IssueKey {
		t.Fatalf("routable issues = %#v", issues)
	}
}

func TestEnqueueJobDedupesByDedupeKey(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()

	create := model.JobCreate{IssueKey: sampleIssue().IssueKey, RouteName: "triage", Kind: "triage", Priority: 10, DedupeKey: "dedupe-1"}
	first, inserted, err := store.EnqueueJob(ctx, create)
	if err != nil {
		t.Fatalf("EnqueueJob first error = %v", err)
	}
	if !inserted {
		t.Fatal("first enqueue inserted = false")
	}
	second, inserted, err := store.EnqueueJob(ctx, create)
	if err != nil {
		t.Fatalf("EnqueueJob second error = %v", err)
	}
	if inserted {
		t.Fatal("second enqueue inserted = true")
	}
	if first.ID != second.ID {
		t.Fatalf("deduped job ID = %q, want %q", second.ID, first.ID)
	}

	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs len = %d, want 1", len(jobs))
	}
}

func TestJobsSortByPriorityDescThenCreatedAsc(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()

	_, inserted, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "low", Kind: "code", Priority: 1, DedupeKey: "low"})
	if err != nil || !inserted {
		t.Fatalf("enqueue low inserted=%v err=%v", inserted, err)
	}
	time.Sleep(time.Millisecond)
	_, inserted, err = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "high1", Kind: "code", Priority: 10, DedupeKey: "high1"})
	if err != nil || !inserted {
		t.Fatalf("enqueue high1 inserted=%v err=%v", inserted, err)
	}
	time.Sleep(time.Millisecond)
	_, inserted, err = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "high2", Kind: "code", Priority: 10, DedupeKey: "high2"})
	if err != nil || !inserted {
		t.Fatalf("enqueue high2 inserted=%v err=%v", inserted, err)
	}

	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{jobs[0].RouteName, jobs[1].RouteName, jobs[2].RouteName}
	want := []string{"high1", "high2", "low"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("job order = %#v, want %#v", got, want)
		}
	}
}

func TestJobEventsCanBeWrittenAndRead(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()

	event, err := store.InsertJobEvent(ctx, model.JobEvent{JobID: "job-1", IssueKey: "issue-1", EventType: "test", Message: "hello", DataJSON: `{"ok":true}`})
	if err != nil {
		t.Fatalf("InsertJobEvent error = %v", err)
	}
	if event.ID == "" {
		t.Fatal("event ID is empty")
	}
	events, err := store.ListJobEvents(ctx, "job-1")
	if err != nil {
		t.Fatalf("ListJobEvents error = %v", err)
	}
	if len(events) != 1 || events[0].Message != "hello" || events[0].DataJSON != `{"ok":true}` {
		t.Fatalf("events = %#v", events)
	}
}

func TestEmptyDBAutomaticallyInitialized(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "issueq.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer store.Close()
	if _, err := store.ListIssues(ctx); err != nil {
		t.Fatalf("ListIssues on empty DB error = %v", err)
	}
	if _, err := store.ListJobs(ctx); err != nil {
		t.Fatalf("ListJobs on empty DB error = %v", err)
	}
	info, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_ = info.Close()
}

func openTempStore(t *testing.T, ctx context.Context) *Store {
	t.Helper()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "issueq.db"))
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	return store
}

func sampleIssue() model.IssueSnapshot {
	return model.IssueSnapshot{
		IssueKey:        "github.com/example-org/example-repo#1",
		NodeID:          "node-1",
		Host:            "github.com",
		Owner:           "example-org",
		Repo:            "example-repo",
		Number:          1,
		Title:           "title",
		Body:            "body",
		Labels:          []string{"agent-triage"},
		State:           "open",
		GitHubUpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		SyncedAt:        time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
	}
}

func TestClaimPendingJobSetsLeaseFields(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, inserted, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "claim-1"})
	if err != nil || !inserted {
		t.Fatalf("enqueue inserted=%v err=%v", inserted, err)
	}
	job, err := store.ClaimNextJob(ctx, "runner-1", []string{"code"}, 1, map[string]int{"code": 1}, 30*time.Minute)
	if err != nil {
		t.Fatalf("ClaimNextJob error = %v", err)
	}
	if job == nil {
		t.Fatal("job = nil")
	}
	if job.Status != model.JobStatusRunning || job.LockedBy != "runner-1" || job.LeaseUntil == nil || job.StartedAt == nil {
		t.Fatalf("claimed job = %#v", job)
	}
}

func TestNonExpiredRunningJobNotClaimedTwice(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "claim-2"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimNextJob(ctx, "runner-1", []string{"code"}, 2, map[string]int{"code": 2}, 30*time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first claim job=%#v err=%v", first, err)
	}
	second, err := store.ClaimNextJob(ctx, "runner-2", []string{"code"}, 2, map[string]int{"code": 2}, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second != nil {
		t.Fatalf("second claim = %#v, want nil", second)
	}
}

func TestExpiredLeaseCanBeReleased(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "claim-3"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.ClaimNextJob(ctx, "runner-1", []string{"code"}, 1, map[string]int{"code": 1}, time.Millisecond)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	time.Sleep(2 * time.Millisecond)
	released, err := store.ReleaseExpiredLeases(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("released = %d, want 1", released)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].Status != model.JobStatusPending || jobs[0].LockedBy != "" || jobs[0].LeaseUntil != nil {
		t.Fatalf("released job = %#v", jobs[0])
	}
}

func TestClaimRespectsGlobalAndRouteConcurrency(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", Priority: 10, DedupeKey: "claim-4"})
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-2", RouteName: "code", Kind: "code", Priority: 9, DedupeKey: "claim-5"})
	first, err := store.ClaimNextJob(ctx, "runner-1", []string{"code"}, 1, map[string]int{"code": 2}, 30*time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first claim job=%#v err=%v", first, err)
	}
	blockedGlobal, err := store.ClaimNextJob(ctx, "runner-1", []string{"code"}, 1, map[string]int{"code": 2}, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if blockedGlobal != nil {
		t.Fatalf("blockedGlobal = %#v, want nil", blockedGlobal)
	}

	if err := store.FinalizeJob(ctx, first.ID, model.JobFinalize{Status: model.JobStatusSucceeded}); err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimNextJob(ctx, "runner-1", []string{"code"}, 2, map[string]int{"code": 1}, 30*time.Minute)
	if err != nil || second == nil {
		t.Fatalf("second claim job=%#v err=%v", second, err)
	}
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-3", RouteName: "code", Kind: "code", Priority: 8, DedupeKey: "claim-6"})
	blockedRoute, err := store.ClaimNextJob(ctx, "runner-1", []string{"code"}, 2, map[string]int{"code": 1}, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if blockedRoute != nil {
		t.Fatalf("blockedRoute = %#v, want nil", blockedRoute)
	}
}

func TestAttemptsScopedByIssueGenerationRouteAndIncrement(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	first, err := store.IncrementAttempts(ctx, "issue-1", 0, "code")
	if err != nil || first != 1 {
		t.Fatalf("first attempts=%d err=%v", first, err)
	}
	second, err := store.IncrementAttempts(ctx, "issue-1", 0, "code")
	if err != nil || second != 2 {
		t.Fatalf("second attempts=%d err=%v", second, err)
	}
	otherGen, err := store.IncrementAttempts(ctx, "issue-1", 1, "code")
	if err != nil || otherGen != 1 {
		t.Fatalf("otherGen attempts=%d err=%v", otherGen, err)
	}
	otherRoute, err := store.IncrementAttempts(ctx, "issue-1", 0, "review")
	if err != nil || otherRoute != 1 {
		t.Fatalf("otherRoute attempts=%d err=%v", otherRoute, err)
	}
}

func TestTransitionCountIncrements(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	issue := sampleIssue()
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	count, err := store.IncrementTransitions(ctx, issue.IssueKey)
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	count, err = store.IncrementTransitions(ctx, issue.IssueKey)
	if err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
