package router

import (
	"context"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
)

func TestMatchesIncludesAllRequiredLabels(t *testing.T) {
	cfg := testConfig()
	route := cfg.Routes[0]
	issue := testIssue([]string{"agent-ready", "bug"}, "open")
	if !Matches(cfg, route, issue) {
		t.Fatal("Matches() = false, want true")
	}
	issue.Labels = []string{"bug"}
	if Matches(cfg, route, issue) {
		t.Fatal("Matches() = true without required label")
	}
}

func TestMatchesExcludeLabelsBlockRoute(t *testing.T) {
	cfg := testConfig()
	route := cfg.Routes[0]
	issue := testIssue([]string{"agent-ready", "agent-running"}, "open")
	if Matches(cfg, route, issue) {
		t.Fatal("Matches() = true with excluded label")
	}
}

func TestMatchesClosedIssueFalse(t *testing.T) {
	cfg := testConfig()
	if Matches(cfg, cfg.Routes[0], testIssue([]string{"agent-ready"}, "closed")) {
		t.Fatal("Matches() = true for closed issue")
	}
}

func TestTerminalLabelsBlockWhenExcluded(t *testing.T) {
	cfg := testConfig()
	if Matches(cfg, cfg.Routes[0], testIssue([]string{"agent-ready", "agent-done"}, "open")) {
		t.Fatal("Matches() = true with terminal excluded label")
	}
}

func TestCapabilitiesBlockUnsupportedKinds(t *testing.T) {
	cfg := testConfig()
	cfg.Runner.Capabilities = []string{"triage"}
	if Matches(cfg, cfg.Routes[0], testIssue([]string{"agent-ready"}, "open")) {
		t.Fatal("Matches() = true for unsupported route kind")
	}
	cfg.Runner.Capabilities = nil
	if !Matches(cfg, cfg.Routes[0], testIssue([]string{"agent-ready"}, "open")) {
		t.Fatal("Matches() = false with empty capabilities; want true")
	}
}

func TestLabelHashStableRegardlessOfOrder(t *testing.T) {
	first := LabelHash([]string{"bug", "agent-ready", "p1"})
	second := LabelHash([]string{"p1", "bug", "agent-ready"})
	if first != second {
		t.Fatalf("hashes differ: %s != %s", first, second)
	}
}

func TestDedupeKeyChangesWithLabelsOrUpdatedAt(t *testing.T) {
	issue := testIssue([]string{"agent-ready"}, "open")
	base := DedupeKey(issue, "code")
	changedLabels := issue
	changedLabels.Labels = []string{"agent-ready", "bug"}
	if got := DedupeKey(changedLabels, "code"); got == base {
		t.Fatal("dedupe key did not change when labels changed")
	}
	changedTime := issue
	changedTime.GitHubUpdatedAt = changedTime.GitHubUpdatedAt.Add(time.Minute)
	if got := DedupeKey(changedTime, "code"); got == base {
		t.Fatal("dedupe key did not change when GitHub updated timestamp changed")
	}
}

func TestRouteCreatesPendingJobForMatchingIssue(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := testConfig()
	if err := store.UpsertIssue(ctx, testIssue([]string{"agent-ready"}, "open")); err != nil {
		t.Fatal(err)
	}
	result, err := Route(ctx, cfg, store)
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	if result.JobsCreated != 1 || result.JobsExisting != 0 {
		t.Fatalf("result = %#v", result)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.JobStatusPending || jobs[0].RouteName != "code" {
		t.Fatalf("jobs = %#v", jobs)
	}
}

func TestRepeatedRouteCreatesNoDuplicateJobs(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := testConfig()
	if err := store.UpsertIssue(ctx, testIssue([]string{"agent-ready"}, "open")); err != nil {
		t.Fatal(err)
	}
	if _, err := Route(ctx, cfg, store); err != nil {
		t.Fatal(err)
	}
	result, err := Route(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.JobsCreated != 0 || result.JobsExisting != 1 {
		t.Fatalf("second result = %#v", result)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs len = %d, want 1", len(jobs))
	}
}

func TestTwoMatchingRoutesCreateTwoJobs(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := testConfig()
	cfg.Routes = append(cfg.Routes, config.RouteConfig{
		Name: "audit",
		When: config.PredicateConfig{LabelsInclude: []string{"agent-ready"}},
		Job:  config.JobConfig{Kind: "code", Priority: 5},
	})
	if err := store.UpsertIssue(ctx, testIssue([]string{"agent-ready"}, "open")); err != nil {
		t.Fatal(err)
	}
	result, err := Route(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.JobsCreated != 2 {
		t.Fatalf("result = %#v", result)
	}
}

func TestNonMatchingIssueCreatesNoJobs(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := testConfig()
	if err := store.UpsertIssue(ctx, testIssue([]string{"bug"}, "open")); err != nil {
		t.Fatal(err)
	}
	result, err := Route(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if result.JobsCreated != 0 || result.RoutesMatched != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func testConfig() config.Config {
	return config.Config{
		Runner: config.RunnerConfig{Capabilities: []string{"code", "triage"}},
		Routes: []config.RouteConfig{{
			Name: "code",
			When: config.PredicateConfig{
				LabelsInclude: []string{"agent-ready"},
				LabelsExclude: []string{"agent-running", "agent-done", "agent-failed", "agent-needs-human", "manual-only"},
			},
			Job: config.JobConfig{Kind: "code", Priority: 20},
		}},
	}
}

func testIssue(labels []string, state string) model.IssueSnapshot {
	return model.IssueSnapshot{
		IssueKey:        "github.com/example-org/example-repo#1",
		Host:            "github.com",
		Owner:           "example-org",
		Repo:            "example-repo",
		Number:          1,
		Title:           "Issue",
		Labels:          labels,
		State:           state,
		GitHubUpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func openStore(t *testing.T, ctx context.Context) *sqlitestore.Store {
	t.Helper()
	store, err := sqlitestore.Open(ctx, t.TempDir()+"/issueq.db")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return store
}
