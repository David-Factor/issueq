package workflow

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
	"issueq/internal/supervisor"
)

type fakeLifecycleGitHub struct {
	issue model.IssueSnapshot
}

func (f *fakeLifecycleGitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	return []model.IssueSnapshot{f.issue}, nil
}
func (f *fakeLifecycleGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]model.IssueComment, error) {
	return nil, nil
}
func (f *fakeLifecycleGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	return f.issue, nil
}
func (f *fakeLifecycleGitHub) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.issue.Labels = append(f.issue.Labels, labels...)
	return nil
}
func (f *fakeLifecycleGitHub) SetLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.issue.Labels = append([]string(nil), labels...)
	return nil
}

func (f *fakeLifecycleGitHub) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	return nil
}
func (f *fakeLifecycleGitHub) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	return nil
}

func (f *fakeLifecycleGitHub) UpdateComment(ctx context.Context, owner, repo string, commentID string, body string) error {
	return nil
}

func TestPrepareClaimedWrapperLaunchUsesHandoffScopedAttempts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := sqlitestore.Open(ctx, filepath.Join(dir, "issueq.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	issue := model.IssueSnapshot{
		IssueKey:        "github.com/example-org/example-repo#1",
		Host:            "github.com",
		Owner:           "example-org",
		Repo:            "example-repo",
		Number:          1,
		Title:           "Issue",
		Body:            "current body",
		Labels:          []string{"agent-ready"},
		State:           "open",
		GitHubUpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertHandoff(ctx, lifecycleHandoff("handoff-1", issue)); err != nil {
		t.Fatal(err)
	}
	route := config.RouteConfig{
		Name: "fix",
		When: config.PredicateConfig{LabelsInclude: []string{"agent-ready"}},
		Gate: config.GateConfig{Handoff: config.HandoffGateConfig{
			Required:  true,
			From:      []string{"triage"},
			Decisions: []string{"accepted"},
			NextRoute: config.HandoffNextRouteConfig{Mode: config.HandoffNextRouteCurrent},
			Freshness: config.HandoffFreshnessSourceUnchanged,
		}},
		Job: config.JobConfig{Kind: "code", MaxAttempts: 1, AttemptScope: config.AttemptScopeHandoff},
	}
	cfg := config.Config{GitHub: config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo"}, Runner: config.RunnerConfig{Capabilities: []string{"code"}}, Routes: []config.RouteConfig{route}}
	identity := model.RunnerIdentity{RunnerID: "runner", InstanceID: "runner-1"}
	gh := &fakeLifecycleGitHub{issue: issue}

	first := prepareScopedJob(t, ctx, store, cfg, gh, identity, route, "first")
	if first.Outcome != PrepareLaunch || first.Finalized {
		t.Fatalf("first result = %#v", first)
	}
	jobs, err := store.ListOwnedRunningJobs(ctx, identity.InstanceID)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("running jobs=%#v err=%v", jobs, err)
	}
	if jobs[0].Attempts != 1 {
		t.Fatalf("first job attempts = %d, want 1", jobs[0].Attempts)
	}
	if err := store.FinalizeJobOwned(ctx, jobs[0].ID, identity.InstanceID, model.JobFinalize{Status: model.JobStatusSucceeded}); err != nil {
		t.Fatal(err)
	}

	second := prepareScopedJob(t, ctx, store, cfg, gh, identity, route, "second")
	if second.Outcome != PrepareDone || second.Status != model.JobStatusDead || !second.Finalized {
		t.Fatalf("second result = %#v, want finalized dead", second)
	}
}

func TestPrepareClaimedWrapperLaunchSkipsBlockedHandoffScopeWithoutAttempt(t *testing.T) {
	tests := []struct {
		name          string
		attemptScope  string
		handoff       *model.Handoff
		latestBody    string
		wantLastError string
	}{
		{name: "handoff missing", attemptScope: config.AttemptScopeHandoff, latestBody: "current body", wantLastError: model.GateBlockReasonMissingHandoff},
		{name: "handoff decision disallowed", attemptScope: config.AttemptScopeHandoff, handoff: ptrHandoff(func(issue model.IssueSnapshot) model.Handoff {
			h := lifecycleHandoff("handoff-rejected", issue)
			h.Decision = "rejected"
			return h
		}), latestBody: "current body", wantLastError: model.GateBlockReasonDecisionNotAllowed},
		{name: "handoff source stale", attemptScope: config.AttemptScopeHandoff, handoff: ptrHandoff(func(issue model.IssueSnapshot) model.Handoff {
			return lifecycleHandoff("handoff-stale", issue)
		}), latestBody: "changed body", wantLastError: model.GateBlockReasonSourceStale},
		{name: "pr head missing", attemptScope: config.AttemptScopePRHead, latestBody: "current body", wantLastError: model.GateBlockReasonMissingHandoff},
		{name: "pr head source stale", attemptScope: config.AttemptScopePRHead, handoff: ptrHandoff(func(issue model.IssueSnapshot) model.Handoff {
			return lifecycleHandoff("handoff-pr-stale", issue)
		}), latestBody: "changed body", wantLastError: model.GateBlockReasonSourceStale},
		{name: "ci head decision disallowed", attemptScope: config.AttemptScopeCIHead, handoff: ptrHandoff(func(issue model.IssueSnapshot) model.Handoff {
			h := lifecycleHandoff("handoff-ci-rejected", issue)
			h.Decision = "rejected"
			return h
		}), latestBody: "current body", wantLastError: model.GateBlockReasonDecisionNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "issueq.db")
			store, err := sqlitestore.Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			issue := model.IssueSnapshot{
				IssueKey:        "github.com/example-org/example-repo#1",
				Host:            "github.com",
				Owner:           "example-org",
				Repo:            "example-repo",
				Number:          1,
				Title:           "Issue",
				Body:            "current body",
				Labels:          []string{"agent-ready"},
				State:           "open",
				GitHubUpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			if err := store.UpsertIssue(ctx, issue); err != nil {
				t.Fatal(err)
			}
			if tt.handoff != nil {
				if _, err := store.UpsertHandoff(ctx, *tt.handoff); err != nil {
					t.Fatal(err)
				}
			}
			route := config.RouteConfig{
				Name: "fix",
				When: config.PredicateConfig{LabelsInclude: []string{"agent-ready"}},
				Gate: config.GateConfig{Handoff: config.HandoffGateConfig{
					Required:  true,
					From:      []string{"triage"},
					Decisions: []string{"accepted"},
					NextRoute: config.HandoffNextRouteConfig{Mode: config.HandoffNextRouteCurrent},
					Freshness: config.HandoffFreshnessSourceUnchanged,
				}},
				Job: config.JobConfig{Kind: "code", MaxAttempts: 1, AttemptScope: tt.attemptScope},
			}
			cfg := config.Config{GitHub: config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo"}, Runner: config.RunnerConfig{Capabilities: []string{"code"}}, Routes: []config.RouteConfig{route}}
			identity := model.RunnerIdentity{RunnerID: "runner", InstanceID: "runner-1"}
			latest := issue
			latest.Body = tt.latestBody
			gh := &fakeLifecycleGitHub{issue: latest}

			result := prepareScopedJob(t, ctx, store, cfg, gh, identity, route, tt.name)
			if result.Outcome != PrepareDone || result.Status != model.JobStatusSkipped || !result.Finalized {
				t.Fatalf("result = %#v, want finalized skipped", result)
			}
			jobs, err := store.ListJobs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != 1 {
				t.Fatalf("jobs = %#v, want one job", jobs)
			}
			if jobs[0].Attempts != 0 {
				t.Fatalf("job attempts = %d, want 0", jobs[0].Attempts)
			}
			if jobs[0].Status != model.JobStatusSkipped || !strings.Contains(jobs[0].LastError, tt.wantLastError) {
				t.Fatalf("job status=%q last_error=%q, want skipped containing %q", jobs[0].Status, jobs[0].LastError, tt.wantLastError)
			}
		})
	}
}

func TestPrepareClaimedWrapperLaunchKeepsReservedScopeLegacyFallbackWhenGateAllowed(t *testing.T) {
	for _, attemptScope := range []string{config.AttemptScopePRHead, config.AttemptScopeCIHead} {
		t.Run(attemptScope, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "issueq.db")
			store, err := sqlitestore.Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			issue := model.IssueSnapshot{
				IssueKey:        "github.com/example-org/example-repo#1",
				Host:            "github.com",
				Owner:           "example-org",
				Repo:            "example-repo",
				Number:          1,
				Title:           "Issue",
				Body:            "current body",
				Labels:          []string{"agent-ready"},
				State:           "open",
				GitHubUpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			if err := store.UpsertIssue(ctx, issue); err != nil {
				t.Fatal(err)
			}
			if _, err := store.UpsertHandoff(ctx, lifecycleHandoff("handoff-allowed", issue)); err != nil {
				t.Fatal(err)
			}
			route := config.RouteConfig{
				Name: "fix",
				When: config.PredicateConfig{LabelsInclude: []string{"agent-ready"}},
				Gate: config.GateConfig{Handoff: config.HandoffGateConfig{
					Required:  true,
					From:      []string{"triage"},
					Decisions: []string{"accepted"},
					NextRoute: config.HandoffNextRouteConfig{Mode: config.HandoffNextRouteCurrent},
					Freshness: config.HandoffFreshnessSourceUnchanged,
				}},
				Job: config.JobConfig{Kind: "code", MaxAttempts: 1, AttemptScope: attemptScope},
			}
			cfg := config.Config{GitHub: config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo"}, Runner: config.RunnerConfig{Capabilities: []string{"code"}}, Routes: []config.RouteConfig{route}}
			identity := model.RunnerIdentity{RunnerID: "runner", InstanceID: "runner-1"}
			gh := &fakeLifecycleGitHub{issue: issue}

			result := prepareScopedJob(t, ctx, store, cfg, gh, identity, route, attemptScope)
			if result.Outcome != PrepareLaunch || result.Finalized {
				t.Fatalf("result = %#v, want launch", result)
			}
			jobs, err := store.ListOwnedRunningJobs(ctx, identity.InstanceID)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != 1 || jobs[0].Attempts != 1 {
				t.Fatalf("running jobs = %#v, want one job with one attempt", jobs)
			}
		})
	}
}

func TestFinalizeOwnedObservationWorkStartedFallbackAccounting(t *testing.T) {
	tests := []struct {
		name              string
		resultJSON        string
		wantRouteAttempts int
		wantJobAttempts   int
	}{
		{name: "omitted defaults started", resultJSON: `{}`, wantRouteAttempts: 1, wantJobAttempts: 1},
		{name: "true counts", resultJSON: `{"work_started":true}`, wantRouteAttempts: 1, wantJobAttempts: 1},
		{name: "false reverses", resultJSON: `{"enqueue":[],"work_started":false}`, wantRouteAttempts: 0, wantJobAttempts: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "issueq.db")
			store, err := sqlitestore.Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			owner := model.RunnerIdentity{RunnerID: "runner", InstanceID: "runner-1"}
			if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "fix", Kind: "code", DedupeKey: tt.name}); err != nil {
				t.Fatal(err)
			}
			job, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"fix": 1}, time.Minute)
			if err != nil || job == nil {
				t.Fatalf("claim job=%#v err=%v", job, err)
			}
			if attempts, err := store.IncrementAttemptsForJob(ctx, job.ID, owner.InstanceID, "issue-1", 2, "fix", "handoff-a"); err != nil || attempts != 1 {
				t.Fatalf("increment attempts=%d err=%v", attempts, err)
			}
			resultPath := filepath.Join(dir, tt.name+"-result.json")
			if err := os.WriteFile(resultPath, []byte(tt.resultJSON), 0o600); err != nil {
				t.Fatal(err)
			}
			finalized, err := FinalizeOwnedObservationWithLifecycle(ctx, FinalizeObservationLifecycleInput{
				Config:   config.Config{},
				Queue:    store,
				Identity: owner,
				Job:      *job,
				Obs:      supervisorObservation(resultPath),
				Lease:    time.Minute,
				Now:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !finalized.Finalized || finalized.Status != model.JobStatusSucceeded {
				t.Fatalf("finalized = %#v", finalized)
			}
			var routeAttempts int
			db, err := sql.Open("sqlite", dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.QueryRowContext(ctx, `SELECT attempts FROM route_attempts WHERE issue_key = 'issue-1' AND generation = 2 AND route_name = 'fix' AND scope_hash = 'handoff-a'`).Scan(&routeAttempts); err != nil {
				t.Fatal(err)
			}
			if routeAttempts != tt.wantRouteAttempts {
				t.Fatalf("route attempts = %d, want %d", routeAttempts, tt.wantRouteAttempts)
			}
			jobs, err := store.ListJobs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != 1 || jobs[0].Attempts != tt.wantJobAttempts {
				t.Fatalf("jobs = %#v, want attempts %d", jobs, tt.wantJobAttempts)
			}
		})
	}
}

func TestFinalizeOwnedObservationCleanupPathWorkStartedFalseReversesAttemptOnce(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "issueq.db")
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	owner := model.RunnerIdentity{RunnerID: "runner", InstanceID: "runner-1"}
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: "issue-1", RouteName: "fix", Kind: "code", DedupeKey: "cleanup-path"}); err != nil {
		t.Fatal(err)
	}
	job, err := store.ClaimNextJob(ctx, owner, []string{"code"}, 1, map[string]int{"fix": 1}, time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	if attempts, err := store.IncrementAttemptsForJob(ctx, job.ID, owner.InstanceID, "issue-1", 2, "fix", "handoff-a"); err != nil || attempts != 1 {
		t.Fatalf("increment attempts=%d err=%v", attempts, err)
	}

	resultPath := filepath.Join(dir, "result.json")
	if err := os.WriteFile(resultPath, []byte(`{"work_started":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := model.LaunchSpecRecord{
		SupervisorKind:  supervisor.KindWrapper,
		LaunchToken:     "tok",
		LaunchSpecPath:  filepath.Join(dir, "spec.json"),
		ResultPath:      resultPath,
		RunMetadataPath: filepath.Join(dir, "run.json"),
		TimeoutAt:       time.Now().Add(time.Minute),
	}
	if err := store.PersistLaunchSpecOwned(ctx, job.ID, owner.InstanceID, spec); err != nil {
		t.Fatalf("PersistLaunchSpecOwned error = %v", err)
	}
	if err := store.MarkJobLaunchingOwned(ctx, job.ID, owner.InstanceID, "tok"); err != nil {
		t.Fatalf("MarkJobLaunchingOwned error = %v", err)
	}
	if err := store.PersistLaunchRecordOwned(ctx, job.ID, owner.InstanceID, model.LaunchRecord{SupervisorKind: supervisor.KindWrapper, SupervisorID: "pid-123", LaunchToken: "tok", PID: 123, RunMetadataPath: spec.RunMetadataPath, ResultPath: resultPath, TimeoutAt: spec.TimeoutAt}); err != nil {
		t.Fatalf("PersistLaunchRecordOwned error = %v", err)
	}
	running, err := store.ListOwnedRunningJobs(ctx, owner.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 1 {
		t.Fatalf("running jobs = %#v, want one", running)
	}

	obs := supervisorObservation(resultPath)
	finalized, err := FinalizeOwnedObservation(ctx, store, owner, running[0], obs, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !finalized.Finalized || finalized.Status != model.JobStatusSucceeded {
		t.Fatalf("finalized = %#v", finalized)
	}
	retry, err := FinalizeOwnedObservation(ctx, store, owner, running[0], obs, time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !retry.OwnershipLost || retry.Finalized {
		t.Fatalf("retry finalization = %#v, want ownership loss without finalization", retry)
	}

	var routeAttempts int
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRowContext(ctx, `SELECT attempts FROM route_attempts WHERE issue_key = 'issue-1' AND generation = 2 AND route_name = 'fix' AND scope_hash = 'handoff-a'`).Scan(&routeAttempts); err != nil {
		t.Fatal(err)
	}
	if routeAttempts != 0 {
		t.Fatalf("route attempts = %d, want 0", routeAttempts)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Attempts != 0 {
		t.Fatalf("jobs = %#v, want one job with zero attempts", jobs)
	}
}

func supervisorObservation(resultPath string) supervisor.Observation {
	return supervisor.Observation{
		State:       supervisor.RunExited,
		ExitCode:    0,
		HasExitCode: true,
		ResultPath:  resultPath,
		FinishedAt:  time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
	}
}

func prepareScopedJob(t *testing.T, ctx context.Context, store *sqlitestore.Store, cfg config.Config, gh *fakeLifecycleGitHub, identity model.RunnerIdentity, route config.RouteConfig, dedupe string) PrepareClaimedWrapperResult {
	t.Helper()
	if _, _, err := store.EnqueueJob(ctx, model.JobCreate{IssueKey: gh.issue.IssueKey, RouteName: route.Name, Kind: route.Job.Kind, DedupeKey: dedupe}); err != nil {
		t.Fatal(err)
	}
	job, err := store.ClaimNextJob(ctx, identity, []string{route.Job.Kind}, 1, map[string]int{route.Name: 1}, time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim job=%#v err=%v", job, err)
	}
	result, err := PrepareClaimedWrapperLaunch(ctx, PrepareClaimedWrapperInput{Config: cfg, Queue: store, GitHub: gh, Identity: identity, Job: job, Route: route, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func ptrHandoff(build func(model.IssueSnapshot) model.Handoff) *model.Handoff {
	issue := model.IssueSnapshot{IssueKey: "github.com/example-org/example-repo#1", Number: 1, Body: "current body"}
	handoff := build(issue)
	return &handoff
}

func lifecycleHandoff(id string, issue model.IssueSnapshot) model.Handoff {
	h := sha256.Sum256([]byte(issue.Body))
	return model.Handoff{
		ID:                id,
		IssueKey:          issue.IssueKey,
		RouteName:         "triage",
		Decision:          "accepted",
		NextRoute:         "fix",
		SourceKind:        "github_issue",
		SourceKey:         "#1",
		SourceFingerprint: hex.EncodeToString(h[:]),
		TargetKind:        "github_issue",
		TargetKey:         "#1",
		PayloadJSON:       `{"schema":"issueq-handoff/v1"}`,
		CreatedAt:         time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
	}
}
