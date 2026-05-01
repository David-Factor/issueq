package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"issueq/internal/model"
	storepkg "issueq/internal/store"

	_ "modernc.org/sqlite"
)

func TestMigrationsCreateTablesAndAreIdempotent(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}
	for _, table := range []string{"issues", "jobs", "issue_state", "route_attempts", "runner_heartbeats", "job_events"} {
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

func identity(id string) model.RunnerIdentity {
	return model.RunnerIdentity{RunnerID: id, InstanceID: id + "-instance"}
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
	job, err := store.ClaimNextJob(ctx, identity("runner-1"), []string{"code"}, 1, map[string]int{"code": 1}, 30*time.Minute)
	if err != nil {
		t.Fatalf("ClaimNextJob error = %v", err)
	}
	if job == nil {
		t.Fatal("job = nil")
	}
	if job.Status != model.JobStatusRunning || job.LockedBy != "runner-1" || job.RunnerInstanceID != "runner-1-instance" || job.LeaseUntil == nil || job.StartedAt == nil {
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
	first, err := store.ClaimNextJob(ctx, identity("runner-1"), []string{"code"}, 2, map[string]int{"code": 2}, 30*time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first claim job=%#v err=%v", first, err)
	}
	second, err := store.ClaimNextJob(ctx, identity("runner-2"), []string{"code"}, 2, map[string]int{"code": 2}, 30*time.Minute)
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
	job, err := store.ClaimNextJob(ctx, identity("runner-1"), []string{"code"}, 1, map[string]int{"code": 1}, time.Millisecond)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	time.Sleep(2 * time.Millisecond)
	released, err := store.ReleaseExpiredLeases(ctx, time.Now().UTC(), time.Now().UTC(), "", nil)
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
	first, err := store.ClaimNextJob(ctx, identity("runner-1"), []string{"code"}, 1, map[string]int{"code": 2}, 30*time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first claim job=%#v err=%v", first, err)
	}
	blockedGlobal, err := store.ClaimNextJob(ctx, identity("runner-1"), []string{"code"}, 1, map[string]int{"code": 2}, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if blockedGlobal != nil {
		t.Fatalf("blockedGlobal = %#v, want nil", blockedGlobal)
	}

	if err := store.FinalizeJobOwned(ctx, first.ID, identity("runner-1").InstanceID, model.JobFinalize{Status: model.JobStatusSucceeded}); err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimNextJob(ctx, identity("runner-1"), []string{"code"}, 2, map[string]int{"code": 1}, 30*time.Minute)
	if err != nil || second == nil {
		t.Fatalf("second claim job=%#v err=%v", second, err)
	}
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-3", RouteName: "code", Kind: "code", Priority: 8, DedupeKey: "claim-6"})
	blockedRoute, err := store.ClaimNextJob(ctx, identity("runner-1"), []string{"code"}, 2, map[string]int{"code": 1}, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if blockedRoute != nil {
		t.Fatalf("blockedRoute = %#v, want nil", blockedRoute)
	}
}

func TestFormattedTimesSortLexicographically(t *testing.T) {
	early := time.Date(2026, 1, 1, 0, 0, 0, 9, time.UTC)
	late := time.Date(2026, 1, 1, 0, 0, 0, 10, time.UTC)
	if !(formatTime(early) < formatTime(late)) {
		t.Fatalf("formatted times do not sort: %q >= %q", formatTime(early), formatTime(late))
	}
	if got := parseTime(formatTime(late)); !got.Equal(late) {
		t.Fatalf("parse formatted time = %s, want %s", got, late)
	}
	legacy := "2026-01-01T00:00:00.9Z"
	if got := parseTime(legacy); got.IsZero() {
		t.Fatalf("legacy RFC3339Nano time did not parse")
	}
}

func TestHeartbeatRunnerInsertUpdateDeleteAndPrune(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()

	id := identity("runner-hb")
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.HeartbeatRunner(ctx, id, 123, now); err != nil {
		t.Fatalf("HeartbeatRunner insert error = %v", err)
	}
	if err := store.HeartbeatRunner(ctx, id, 456, now.Add(time.Minute)); err != nil {
		t.Fatalf("HeartbeatRunner update error = %v", err)
	}
	var pid int
	var heartbeatAt string
	if err := store.db.QueryRowContext(ctx, `SELECT pid, heartbeat_at FROM runner_heartbeats WHERE runner_instance_id = ?`, id.InstanceID).Scan(&pid, &heartbeatAt); err != nil {
		t.Fatal(err)
	}
	if pid != 456 || parseTime(heartbeatAt) != now.Add(time.Minute) {
		t.Fatalf("heartbeat pid=%d at=%s", pid, heartbeatAt)
	}
	pruned, err := store.PruneStaleRunnerHeartbeats(ctx, now.Add(2*time.Minute))
	if err != nil || pruned != 1 {
		t.Fatalf("PruneStaleRunnerHeartbeats pruned=%d err=%v", pruned, err)
	}
	if err := store.HeartbeatRunner(ctx, id, 789, now); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteRunnerHeartbeat(ctx, id.InstanceID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM runner_heartbeats`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("heartbeats count = %d, want 0", count)
	}
}

func TestOwnedJobMutationsRequireRunnerInstance(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "owned-1"})
	if err != nil {
		t.Fatal(err)
	}
	owner := identity("runner-owned")
	job, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"code": 1}, 30*time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	if err := store.AssertJobOwned(ctx, job.ID, owner.InstanceID); err != nil {
		t.Fatalf("AssertJobOwned owner error = %v", err)
	}
	if err := store.AssertJobOwned(ctx, job.ID, "other-instance"); !errors.Is(err, storepkg.ErrNotOwner) {
		t.Fatalf("AssertJobOwned wrong error = %v, want ErrNotOwner", err)
	}
	if err := store.RenewJobLease(ctx, job.ID, "other-instance", time.Minute); !errors.Is(err, storepkg.ErrNotOwner) {
		t.Fatalf("RenewJobLease wrong error = %v, want ErrNotOwner", err)
	}
	if err := store.RenewJobLease(ctx, job.ID, owner.InstanceID, time.Minute); err != nil {
		t.Fatalf("RenewJobLease owner error = %v", err)
	}
	if err := store.FinalizeJobOwned(ctx, job.ID, "other-instance", model.JobFinalize{Status: model.JobStatusSucceeded}); !errors.Is(err, storepkg.ErrNotOwner) {
		t.Fatalf("FinalizeJobOwned wrong error = %v, want ErrNotOwner", err)
	}
	if err := store.FinalizeJobOwned(ctx, job.ID, owner.InstanceID, model.JobFinalize{Status: model.JobStatusSucceeded}); err != nil {
		t.Fatalf("FinalizeJobOwned owner error = %v", err)
	}
}

func TestOwnedJobMutationFailsAfterLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "owned-expired"})
	if err != nil {
		t.Fatal(err)
	}
	owner := identity("runner-expired")
	job, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"code": 1}, time.Millisecond)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := store.AssertJobOwned(ctx, job.ID, owner.InstanceID); !errors.Is(err, storepkg.ErrLostLease) {
		t.Fatalf("AssertJobOwned expired error = %v, want ErrLostLease", err)
	}
	if err := store.RenewJobLease(ctx, job.ID, owner.InstanceID, time.Minute); !errors.Is(err, storepkg.ErrLostLease) {
		t.Fatalf("RenewJobLease expired error = %v, want ErrLostLease", err)
	}
}

func TestIncrementAttemptsForJobIsAtomicWithOwnership(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "owned-attempt"})
	if err != nil {
		t.Fatal(err)
	}
	owner := identity("runner-attempt")
	job, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"code": 1}, 30*time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	if attempts, err := store.IncrementAttemptsForJob(ctx, job.ID, "other-instance", "issue-1", 0, "code"); !errors.Is(err, storepkg.ErrNotOwner) || attempts != 0 {
		t.Fatalf("wrong owner attempts=%d err=%v", attempts, err)
	}
	var routeRows int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM route_attempts`).Scan(&routeRows); err != nil {
		t.Fatal(err)
	}
	if routeRows != 0 {
		t.Fatalf("route_attempts rows = %d, want 0", routeRows)
	}
	attempts, err := store.IncrementAttemptsForJob(ctx, job.ID, owner.InstanceID, "issue-1", 0, "code")
	if err != nil || attempts != 1 {
		t.Fatalf("owner attempts=%d err=%v", attempts, err)
	}
}

func TestIncrementTransitionsForJobIsOwnershipGuarded(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	issue := model.IssueSnapshot{IssueKey: "issue-1", Host: "github.com", Owner: "o", Repo: "r", Number: 1, State: "open"}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	job, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: issue.IssueKey, RouteName: "code", Kind: "code", DedupeKey: "transition"})
	if err != nil {
		t.Fatal(err)
	}
	owner := identity("owner")
	claimed, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"code": 1}, time.Minute)
	if err != nil || claimed.ID != job.ID {
		t.Fatalf("claimed=%#v err=%v", claimed, err)
	}
	if transitions, err := store.IncrementTransitionsForJob(ctx, job.ID, "other", issue.IssueKey); !errors.Is(err, storepkg.ErrNotOwner) || transitions != 0 {
		t.Fatalf("wrong owner transitions=%d err=%v", transitions, err)
	}
	_, transitions, err := store.GetIssueState(ctx, issue.IssueKey)
	if err != nil {
		t.Fatal(err)
	}
	if transitions != 0 {
		t.Fatalf("transitions = %d, want 0 after denied increment", transitions)
	}
	transitions, err = store.IncrementTransitionsForJob(ctx, job.ID, owner.InstanceID, issue.IssueKey)
	if err != nil || transitions != 1 {
		t.Fatalf("owner transitions=%d err=%v", transitions, err)
	}
}

func TestHeartbeatAwareExpiredLeaseRecovery(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	now := time.Now().UTC()

	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "recover-stale"})
	stale := identity("stale")
	staleJob, _ := store.ClaimNextJob(ctx, stale, []string{"code"}, 10, map[string]int{"code": 10}, time.Minute)
	_, _ = store.db.ExecContext(ctx, `UPDATE jobs SET lease_until = ? WHERE id = ?`, formatTime(now.Add(-time.Millisecond)), staleJob.ID)
	_ = store.HeartbeatRunner(ctx, stale, 111, now.Add(-time.Hour))

	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-2", RouteName: "code", Kind: "code", DedupeKey: "recover-fresh"})
	fresh := identity("fresh")
	freshJob, _ := store.ClaimNextJob(ctx, fresh, []string{"code"}, 10, map[string]int{"code": 10}, time.Minute)
	_, _ = store.db.ExecContext(ctx, `UPDATE jobs SET lease_until = ? WHERE id = ?`, formatTime(now.Add(-time.Millisecond)), freshJob.ID)
	_ = store.HeartbeatRunner(ctx, fresh, 222, now)

	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-3", RouteName: "code", Kind: "code", DedupeKey: "recover-active"})
	active := identity("active")
	activeJob, _ := store.ClaimNextJob(ctx, active, []string{"code"}, 10, map[string]int{"code": 10}, time.Minute)
	_, _ = store.db.ExecContext(ctx, `UPDATE jobs SET lease_until = ? WHERE id = ?`, formatTime(now.Add(-time.Millisecond)), activeJob.ID)
	_ = store.HeartbeatRunner(ctx, active, 333, now.Add(-time.Hour))

	// Backward compatibility: old running row with no runner_instance_id is recoverable.
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-4", RouteName: "code", Kind: "code", DedupeKey: "recover-legacy"})
	legacy := identity("legacy")
	legacyJob, _ := store.ClaimNextJob(ctx, legacy, []string{"code"}, 10, map[string]int{"code": 10}, time.Minute)
	_, _ = store.db.ExecContext(ctx, `UPDATE jobs SET runner_instance_id = NULL, lease_until = ? WHERE id = ?`, formatTime(now.Add(-time.Millisecond)), legacyJob.ID)

	released, err := store.ReleaseExpiredLeases(ctx, time.Now().UTC(), now.Add(-time.Minute), active.InstanceID, []string{activeJob.ID})
	if err != nil {
		t.Fatal(err)
	}
	if released != 2 {
		t.Fatalf("released = %d, want 2", released)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]model.Job{}
	for _, job := range jobs {
		byID[job.ID] = job
	}
	if got := byID[staleJob.ID]; got.Status != model.JobStatusPending || got.LockedBy != "" || got.RunnerInstanceID != "" || got.LeaseUntil != nil || got.PID != 0 || got.Attempts != 0 {
		t.Fatalf("stale recovered job = %#v", got)
	}
	if got := byID[legacyJob.ID]; got.Status != model.JobStatusPending || got.RunnerInstanceID != "" {
		t.Fatalf("legacy recovered job = %#v", got)
	}
	if got := byID[freshJob.ID]; got.Status != model.JobStatusRunning || got.RunnerInstanceID != fresh.InstanceID {
		t.Fatalf("fresh job = %#v", got)
	}
	if got := byID[activeJob.ID]; got.Status != model.JobStatusRunning || got.RunnerInstanceID != active.InstanceID {
		t.Fatalf("active job = %#v", got)
	}
}

func TestTwoStoreInstancesDoNotOverclaimGlobalCapacity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "issueq.db")
	storeA, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer storeA.Close()
	storeB, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer storeB.Close()
	_, _, _ = storeA.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", Priority: 10, DedupeKey: "overclaim-1"})
	_, _, _ = storeA.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-2", RouteName: "code", Kind: "code", Priority: 9, DedupeKey: "overclaim-2"})

	type claimResult struct {
		job *model.Job
		err error
	}
	results := make(chan claimResult, 2)
	go func() {
		job, err := storeA.ClaimNextJob(ctx, identity("runner-overclaim-a"), []string{"code"}, 1, map[string]int{"code": 2}, time.Minute)
		results <- claimResult{job: job, err: err}
	}()
	go func() {
		job, err := storeB.ClaimNextJob(ctx, identity("runner-overclaim-b"), []string{"code"}, 1, map[string]int{"code": 2}, time.Minute)
		results <- claimResult{job: job, err: err}
	}()

	claimed := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("claim error = %v", result.err)
		}
		if result.job != nil {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed = %d, want 1", claimed)
	}
}

func TestDurableLaunchStateTransitionsAndScans(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "launch"})
	if err != nil {
		t.Fatal(err)
	}
	owner := identity("runner-launch")
	job, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"code": 1}, time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	if job.LaunchState != model.LaunchStatePreparing {
		t.Fatalf("claimed launch state = %q", job.LaunchState)
	}
	timeoutAt := time.Now().UTC().Add(time.Minute)
	spec := model.LaunchSpecRecord{SupervisorKind: "wrapper", LaunchToken: "tok", LaunchSpecPath: "spec.json", ContextPath: "context.json", ResultPath: "result.json", StdoutPath: "stdout.log", StderrPath: "stderr.log", RunMetadataPath: "run.json", TimeoutAt: timeoutAt}
	if err := store.PersistLaunchSpecOwned(ctx, job.ID, owner.InstanceID, spec); err != nil {
		t.Fatalf("PersistLaunchSpecOwned error = %v", err)
	}
	if err := store.MarkJobLaunchingOwned(ctx, job.ID, owner.InstanceID, "tok"); err != nil {
		t.Fatalf("MarkJobLaunchingOwned error = %v", err)
	}
	started := time.Now().UTC()
	record := model.LaunchRecord{SupervisorKind: "wrapper", SupervisorID: "pid-123", LaunchToken: "tok", PID: 123, PGID: 123, ProcessStartedAt: started, TimeoutAt: timeoutAt, RunMetadataPath: "run.json"}
	if err := store.PersistLaunchRecordOwned(ctx, job.ID, owner.InstanceID, record); err != nil {
		t.Fatalf("PersistLaunchRecordOwned error = %v", err)
	}
	jobs, err := store.ListOwnedRunningJobs(ctx, owner.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].LaunchState != model.LaunchStateRunning || jobs[0].SupervisorKind != "wrapper" || jobs[0].SupervisorID != "pid-123" || jobs[0].PID != 123 || jobs[0].PGID != 123 || jobs[0].TimeoutAt == nil || jobs[0].ProcessStartedAt == nil {
		t.Fatalf("owned running jobs = %#v", jobs)
	}
	count, err := store.CountRunningJobs(ctx)
	if err != nil || count != 1 {
		t.Fatalf("CountRunningJobs count=%d err=%v", count, err)
	}
	routeCount, err := store.CountRunningJobsByRoute(ctx, "code")
	if err != nil || routeCount != 1 {
		t.Fatalf("CountRunningJobsByRoute count=%d err=%v", routeCount, err)
	}
}

func TestDurableLaunchValidationAndOwnershipFailures(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "launch-failures"})
	if err != nil {
		t.Fatal(err)
	}
	owner := identity("runner-failures")
	job, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"code": 1}, time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	if err := store.PersistLaunchSpecOwned(ctx, job.ID, owner.InstanceID, model.LaunchSpecRecord{}); err == nil {
		t.Fatal("PersistLaunchSpecOwned invalid error = nil")
	}
	valid := model.LaunchSpecRecord{SupervisorKind: "wrapper", LaunchToken: "tok", LaunchSpecPath: "spec", RunMetadataPath: "run", TimeoutAt: time.Now().Add(time.Minute)}
	if err := store.PersistLaunchSpecOwned(ctx, job.ID, "other", valid); !errors.Is(err, storepkg.ErrNotOwner) {
		t.Fatalf("PersistLaunchSpecOwned wrong owner err = %v", err)
	}
	if err := store.PersistLaunchSpecOwned(ctx, job.ID, owner.InstanceID, valid); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobLaunchingOwned(ctx, job.ID, owner.InstanceID, "wrong"); !errors.Is(err, storepkg.ErrNotOwner) {
		t.Fatalf("MarkJobLaunchingOwned wrong token err = %v", err)
	}
	if err := store.MarkJobLaunchingOwned(ctx, job.ID, owner.InstanceID, "tok"); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistLaunchRecordOwned(ctx, job.ID, owner.InstanceID, model.LaunchRecord{SupervisorKind: "wrapper", LaunchToken: "tok"}); err == nil {
		t.Fatal("PersistLaunchRecordOwned invalid error = nil")
	}
}

func TestDurableLaunchOwnershipGuardsAndStaleRecovery(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	now := time.Now().UTC()
	owner := identity("stale-durable")
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "durable"})
	durable, _ := store.ClaimNextJob(ctx, owner, []string{"code"}, 10, map[string]int{"code": 10}, time.Minute)
	if err := store.PersistLaunchSpecOwned(ctx, durable.ID, owner.InstanceID, model.LaunchSpecRecord{SupervisorKind: "wrapper", LaunchToken: "tok", LaunchSpecPath: "spec", RunMetadataPath: "run", TimeoutAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobLaunchingOwned(ctx, durable.ID, owner.InstanceID, "tok"); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistLaunchRecordOwned(ctx, durable.ID, owner.InstanceID, model.LaunchRecord{SupervisorKind: "wrapper", SupervisorID: "sid", LaunchToken: "tok", PID: 1}); err != nil {
		t.Fatal(err)
	}
	_, _ = store.db.ExecContext(ctx, `UPDATE jobs SET lease_until = ? WHERE id = ?`, formatTime(now.Add(-time.Minute)), durable.ID)
	_ = store.HeartbeatRunner(ctx, owner, 1, now.Add(-time.Hour))

	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-2", RouteName: "code", Kind: "code", DedupeKey: "preparing"})
	preparing, _ := store.ClaimNextJob(ctx, owner, []string{"code"}, 10, map[string]int{"code": 10}, time.Minute)
	_, _ = store.db.ExecContext(ctx, `UPDATE jobs SET lease_until = ? WHERE id = ?`, formatTime(now.Add(-time.Minute)), preparing.ID)

	released, err := store.ReleaseExpiredLeases(ctx, now, now.Add(-time.Minute), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("released = %d, want only non-durable preparing row", released)
	}
	stale, err := store.ListStaleDurableRunningJobs(ctx, now, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].ID != durable.ID {
		t.Fatalf("stale durable = %#v", stale)
	}
	if err := store.MarkStaleRunningJobUnknown(ctx, durable.ID, owner.InstanceID, now, now.Add(-time.Minute)); err != nil {
		t.Fatalf("MarkStaleRunningJobUnknown error = %v", err)
	}
	if _, err := store.AdoptStaleRunningJob(ctx, durable.ID, owner.InstanceID, model.RunnerIdentity{}, time.Minute, now, now.Add(-time.Minute)); err == nil {
		t.Fatal("AdoptStaleRunningJob invalid identity error = nil")
	}
	jobs, _ := store.ListJobs(ctx)
	byID := map[string]model.Job{}
	for _, job := range jobs {
		byID[job.ID] = job
	}
	if got := byID[durable.ID]; got.Status != model.JobStatusRunning || got.RunnerInstanceID != owner.InstanceID || got.LaunchState != model.LaunchStateUnknown {
		t.Fatalf("durable after unknown = %#v", got)
	}
	newOwner := identity("new-owner")
	if _, err := store.AdoptStaleRunningJob(ctx, durable.ID, owner.InstanceID, newOwner, time.Minute, now, now.Add(-time.Minute)); !errors.Is(err, storepkg.ErrNotOwner) {
		t.Fatalf("AdoptStaleRunningJob unknown err = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE jobs SET launch_state = ? WHERE id = ?`, model.LaunchStateRunning, durable.ID); err != nil {
		t.Fatal(err)
	}
	adopted, err := store.AdoptStaleRunningJob(ctx, durable.ID, owner.InstanceID, newOwner, time.Minute, now, now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("AdoptStaleRunningJob error = %v", err)
	}
	if adopted.RunnerInstanceID != newOwner.InstanceID || adopted.LockedBy != newOwner.RunnerID || adopted.LeaseUntil == nil {
		t.Fatalf("adopted = %#v", adopted)
	}
	if got := byID[preparing.ID]; got.Status != model.JobStatusPending || got.LaunchToken != "" {
		t.Fatalf("preparing after release = %#v", got)
	}
}

func TestClaimUsesRouteNameConcurrencyNotKindFallback(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code-a", Kind: "code", Priority: 10, DedupeKey: "route-name-1"})
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-2", RouteName: "code-b", Kind: "code", Priority: 9, DedupeKey: "route-name-2"})
	first, err := store.ClaimNextJob(ctx, identity("runner-route-1"), []string{"code"}, 2, map[string]int{"code-a": 1, "code-b": 1, "code": 1}, 30*time.Minute)
	if err != nil || first == nil || first.RouteName != "code-a" {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	second, err := store.ClaimNextJob(ctx, identity("runner-route-2"), []string{"code"}, 2, map[string]int{"code-a": 1, "code-b": 1, "code": 1}, 30*time.Minute)
	if err != nil || second == nil || second.RouteName != "code-b" {
		t.Fatalf("second=%#v err=%v", second, err)
	}
}

func TestClaimFrontierRestrictsEligibleJobs(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	first, _, _ := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", Priority: 1, DedupeKey: "frontier-1"})
	second, _, _ := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-2", RouteName: "code", Kind: "code", Priority: 10, DedupeKey: "frontier-2"})
	ids, err := store.ListEligibleJobIDs(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != second.ID || ids[1] != first.ID {
		t.Fatalf("eligible ids = %#v", ids)
	}
	job, err := store.ClaimNextJobInFrontier(ctx, identity("runner-frontier"), []string{"code"}, 1, map[string]int{"code": 1}, time.Minute, []string{first.ID})
	if err != nil || job == nil || job.ID != first.ID {
		t.Fatalf("frontier claim job=%#v err=%v", job, err)
	}
}

func TestStaleDurableUnknownRowsAreNotRelistedOrAdopted(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t, ctx)
	defer store.Close()
	now := time.Now().UTC()
	owner := identity("unknown-durable")
	_, _, _ = store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "code", Kind: "code", DedupeKey: "unknown-durable"})
	job, _ := store.ClaimNextJob(ctx, owner, []string{"code"}, 10, map[string]int{"code": 10}, time.Minute)
	if err := store.PersistLaunchSpecOwned(ctx, job.ID, owner.InstanceID, model.LaunchSpecRecord{SupervisorKind: "wrapper", LaunchToken: "tok", LaunchSpecPath: "spec", RunMetadataPath: "run", TimeoutAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobLaunchingOwned(ctx, job.ID, owner.InstanceID, "tok"); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistLaunchRecordOwned(ctx, job.ID, owner.InstanceID, model.LaunchRecord{SupervisorKind: "wrapper", SupervisorID: "sid", LaunchToken: "tok", PID: 1, RunMetadataPath: "run", TimeoutAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	_, _ = store.db.ExecContext(ctx, `UPDATE jobs SET lease_until = ?, launch_state = ? WHERE id = ?`, formatTime(now.Add(-time.Minute)), model.LaunchStateUnknown, job.ID)
	_ = store.HeartbeatRunner(ctx, owner, 1, now.Add(-time.Hour))
	stale, err := store.ListStaleDurableRunningJobs(ctx, now, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 {
		t.Fatalf("stale = %#v, want none", stale)
	}
	if _, err := store.AdoptStaleRunningJob(ctx, job.ID, owner.InstanceID, identity("new"), time.Minute, now, now.Add(-time.Minute)); !errors.Is(err, storepkg.ErrNotOwner) {
		t.Fatalf("AdoptStaleRunningJob err = %v", err)
	}
	if err := store.MarkStaleRunningJobUnknown(ctx, job.ID, owner.InstanceID, now, now.Add(-time.Minute)); !errors.Is(err, storepkg.ErrNotOwner) {
		t.Fatalf("MarkStaleRunningJobUnknown err = %v", err)
	}
}
