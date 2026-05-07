package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
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

type fakeRouterGitHub struct {
	issue                 model.IssueSnapshot
	calls                 []string
	comments              []string
	failCreateCommentOnce bool
}

func (f *fakeRouterGitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	return []model.IssueSnapshot{f.issue}, nil
}
func (f *fakeRouterGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]model.IssueComment, error) {
	return nil, nil
}
func (f *fakeRouterGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	f.calls = append(f.calls, "get")
	return f.issue, nil
}
func (f *fakeRouterGitHub) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "add:"+strings.Join(labels, ","))
	f.issue.Labels = append(f.issue.Labels, labels...)
	return nil
}
func (f *fakeRouterGitHub) SetLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "set:"+strings.Join(labels, ","))
	f.issue.Labels = append([]string(nil), labels...)
	return nil
}

func (f *fakeRouterGitHub) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.calls = append(f.calls, "remove:"+strings.Join(labels, ","))
	blocked := map[string]struct{}{}
	for _, label := range labels {
		blocked[label] = struct{}{}
	}
	out := f.issue.Labels[:0]
	for _, label := range f.issue.Labels {
		if _, ok := blocked[label]; !ok {
			out = append(out, label)
		}
	}
	f.issue.Labels = out
	return nil
}
func (f *fakeRouterGitHub) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	f.calls = append(f.calls, "comment")
	if f.failCreateCommentOnce {
		f.failCreateCommentOnce = false
		return errors.New("temporary comment failure")
	}
	f.comments = append(f.comments, body)
	return nil
}

func (f *fakeRouterGitHub) UpdateComment(ctx context.Context, owner, repo string, commentID string, body string) error {
	return nil
}

func TestGateMissingHandoffBlocksAndBlockLabelStopsLaterMatch(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := gatedTestConfig()
	issue := testIssue([]string{"agent-ready"}, "open")
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	gh := &fakeRouterGitHub{issue: issue}
	result, err := RouteWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatalf("RouteWithGitHub() error = %v", err)
	}
	if result.GateBlocked != 1 || result.GateBlocksRecorded != 1 || result.JobsCreated != 0 {
		t.Fatalf("result = %#v", result)
	}
	if strings.Join(gh.comments, "|") != "blocked: missing_handoff" || !containsString(gh.calls, "set:agent-ready,agent-needs-human") {
		t.Fatalf("calls=%#v comments=%#v", gh.calls, gh.comments)
	}
	if jobs, _ := store.ListJobs(ctx); len(jobs) != 0 {
		t.Fatalf("jobs = %#v, want none", jobs)
	}

	gh.calls = nil
	gh.comments = nil
	result, err = RouteWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatalf("second RouteWithGitHub() error = %v", err)
	}
	if result.RoutesMatched != 0 || result.GateBlocked != 0 || result.GateBlocksRecorded != 0 || result.GateBlocksExisting != 0 || len(gh.comments) != 0 {
		t.Fatalf("second result=%#v calls=%#v comments=%#v", result, gh.calls, gh.comments)
	}
}

func TestDuplicateGateBlockDedupesActionWhileStillMatching(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := gatedTestConfig()
	cfg.Routes[0].Gate.OnBlock.LabelsAdd = nil
	issue := testIssue([]string{"agent-ready"}, "open")
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	gh := &fakeRouterGitHub{issue: issue}
	result, err := RouteWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatalf("RouteWithGitHub() error = %v", err)
	}
	if result.GateBlocked != 1 || result.GateBlocksRecorded != 1 || strings.Join(gh.comments, "|") != "blocked: missing_handoff" {
		t.Fatalf("first result=%#v comments=%#v", result, gh.comments)
	}

	gh.calls = nil
	gh.comments = nil
	result, err = RouteWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatalf("second RouteWithGitHub() error = %v", err)
	}
	if result.GateBlocked != 1 || result.GateBlocksRecorded != 0 || result.GateBlocksExisting != 1 || len(gh.comments) != 0 {
		t.Fatalf("second result=%#v calls=%#v comments=%#v", result, gh.calls, gh.comments)
	}
}

func TestGateBlockActionFailureRetriesUntilAppliedOnce(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := gatedTestConfig()
	cfg.Routes[0].Gate.OnBlock.LabelsAdd = nil
	issue := testIssue([]string{"agent-ready"}, "open")
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	gh := &fakeRouterGitHub{issue: issue, failCreateCommentOnce: true}

	_, err := RouteWithGitHub(ctx, cfg, store, gh)
	if err == nil {
		t.Fatal("first RouteWithGitHub() error = nil, want transient action error")
	}
	if len(gh.comments) != 0 {
		t.Fatalf("comments after failed attempt = %#v, want none", gh.comments)
	}

	gh.calls = nil
	result, err := RouteWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatalf("retry RouteWithGitHub() error = %v", err)
	}
	if result.GateBlocked != 1 || result.GateBlocksRecorded != 0 || result.GateBlocksExisting != 1 {
		t.Fatalf("retry result=%#v", result)
	}
	if strings.Join(gh.comments, "|") != "blocked: missing_handoff" {
		t.Fatalf("comments after retry = %#v, want exactly one block comment", gh.comments)
	}

	gh.calls = nil
	result, err = RouteWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatalf("third RouteWithGitHub() error = %v", err)
	}
	if result.GateBlocked != 1 || result.GateBlocksExisting != 1 || len(gh.comments) != 1 {
		t.Fatalf("third result=%#v comments=%#v", result, gh.comments)
	}
}

func TestGateAcceptedFreshHandoffAllowsJob(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := gatedTestConfig()
	issue := testIssue([]string{"agent-ready"}, "open")
	issue.Body = "current body"
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertHandoff(ctx, gateHandoff("h1", "accepted", "code", bodySHA(issue.Body))); err != nil {
		t.Fatal(err)
	}
	result, err := RouteWithGitHub(ctx, cfg, store, &fakeRouterGitHub{issue: issue})
	if err != nil {
		t.Fatalf("RouteWithGitHub() error = %v", err)
	}
	if result.JobsCreated != 1 || result.GateBlocked != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestGateAcceptedEmptyBodyFingerprintAllowsJob(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := gatedTestConfig()
	issue := testIssue([]string{"agent-ready"}, "open")
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertHandoff(ctx, gateHandoff("h1", "accepted", "code", bodySHA(""))); err != nil {
		t.Fatal(err)
	}
	result, err := RouteWithGitHub(ctx, cfg, store, &fakeRouterGitHub{issue: issue})
	if err != nil {
		t.Fatalf("RouteWithGitHub() error = %v", err)
	}
	if result.JobsCreated != 1 || result.GateBlocked != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestAttemptScopeHashScopedRoutesFailClosedWhenGateBlocked(t *testing.T) {
	tests := []string{
		config.AttemptScopeHandoff,
		config.AttemptScopeIssue,
		config.AttemptScopePRHead,
		config.AttemptScopeCIHead,
	}
	for _, attemptScope := range tests {
		t.Run(attemptScope, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t, ctx)
			defer store.Close()
			cfg := gatedTestConfig()
			cfg.Routes[0].Job.AttemptScope = attemptScope
			issue := testIssue([]string{"agent-ready"}, "open")
			if err := store.UpsertIssue(ctx, issue); err != nil {
				t.Fatal(err)
			}
			_, err := AttemptScopeHash(ctx, store, cfg.Routes[0], issue)
			if !errors.Is(err, ErrAttemptScopeBlocked) {
				t.Fatalf("AttemptScopeHash() error = %v, want ErrAttemptScopeBlocked", err)
			}
		})
	}
}

func TestAttemptScopeHashUsesAcceptedHandoff(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := gatedTestConfig()
	cfg.Routes[0].Job.AttemptScope = config.AttemptScopeHandoff
	issue := testIssue([]string{"agent-ready"}, "open")
	issue.Body = "current body"
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertHandoff(ctx, gateHandoff("h1", "accepted", "code", bodySHA(issue.Body))); err != nil {
		t.Fatal(err)
	}
	first, err := AttemptScopeHash(ctx, store, cfg.Routes[0], issue)
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || first == config.AttemptScopeLegacy {
		t.Fatalf("first scope = %q, want non-legacy", first)
	}
	secondHandoff := gateHandoff("h2", "accepted", "code", bodySHA(issue.Body))
	secondHandoff.CreatedAt = secondHandoff.CreatedAt.Add(time.Minute)
	if _, err := store.UpsertHandoff(ctx, secondHandoff); err != nil {
		t.Fatal(err)
	}
	second, err := AttemptScopeHash(ctx, store, cfg.Routes[0], issue)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("second scope = first scope %q, want different scope for different handoff", second)
	}
}

func TestAttemptScopeHashReservedTargetsFallbackToLegacyWhenGateAllowed(t *testing.T) {
	tests := []struct {
		name         string
		attemptScope string
		handoff      model.Handoff
	}{
		{
			name:         "pr head unsupported target kind",
			attemptScope: config.AttemptScopePRHead,
			handoff:      gateHandoff("h1", "accepted", "code", bodySHA("current body")),
		},
		{
			name:         "ci head missing target key",
			attemptScope: config.AttemptScopeCIHead,
			handoff: func() model.Handoff {
				h := gateHandoff("h1", "accepted", "code", bodySHA("current body"))
				h.TargetKind = "github_ci"
				h.TargetKey = ""
				return h
			}(),
		},
		{
			name:         "pr head missing target kind",
			attemptScope: config.AttemptScopePRHead,
			handoff: func() model.Handoff {
				h := gateHandoff("h1", "accepted", "code", bodySHA("current body"))
				h.TargetKind = ""
				h.TargetKey = "pulls/1/head"
				return h
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t, ctx)
			defer store.Close()
			cfg := gatedTestConfig()
			cfg.Routes[0].Job.AttemptScope = tt.attemptScope
			issue := testIssue([]string{"agent-ready"}, "open")
			issue.Body = "current body"
			if err := store.UpsertIssue(ctx, issue); err != nil {
				t.Fatal(err)
			}
			if _, err := store.UpsertHandoff(ctx, tt.handoff); err != nil {
				t.Fatal(err)
			}
			scope, err := AttemptScopeHash(ctx, store, cfg.Routes[0], issue)
			if err != nil {
				t.Fatal(err)
			}
			if scope != config.AttemptScopeLegacy {
				t.Fatalf("scope = %q, want legacy fallback", scope)
			}
		})
	}
}

func TestAttemptScopeHashReservedTargetsHashTrimmedMetadata(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := gatedTestConfig()
	cfg.Routes[0].Job.AttemptScope = config.AttemptScopePRHead
	issue := testIssue([]string{"agent-ready"}, "open")
	issue.Body = "current body"
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	handoff := gateHandoff("h1", "accepted", "code", bodySHA(issue.Body))
	handoff.TargetKind = " github_pr "
	handoff.TargetKey = " pulls/1/head "
	if _, err := store.UpsertHandoff(ctx, handoff); err != nil {
		t.Fatal(err)
	}
	scope, err := AttemptScopeHash(ctx, store, cfg.Routes[0], issue)
	if err != nil {
		t.Fatal(err)
	}
	want := hashScope(config.AttemptScopePRHead, "github_pr", "pulls/1/head")
	if scope != want {
		t.Fatalf("scope = %q, want trimmed metadata hash %q", scope, want)
	}
}

func TestRouteWithoutGateIgnoresStoredHandoff(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := testConfig()
	issue := testIssue([]string{"agent-ready"}, "open")
	issue.Body = "body"
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertHandoff(ctx, gateHandoff("h1", "rejected", "other", "old")); err != nil {
		t.Fatal(err)
	}
	result, err := Route(ctx, cfg, store)
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	if result.JobsCreated != 1 || result.GateBlocked != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestGateBlocksDecisionNextRouteAndStaleSource(t *testing.T) {
	tests := []struct {
		name    string
		handoff model.Handoff
		want    string
	}{
		{name: "decision", handoff: gateHandoff("h1", "rejected", "code", bodySHA("body")), want: model.GateBlockReasonDecisionNotAllowed},
		{name: "next route", handoff: gateHandoff("h2", "accepted", "other", bodySHA("body")), want: model.GateBlockReasonNextRouteMismatch},
		{name: "source stale", handoff: gateHandoff("h3", "accepted", "code", "old"), want: model.GateBlockReasonSourceStale},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t, ctx)
			defer store.Close()
			cfg := gatedTestConfig()
			issue := testIssue([]string{"agent-ready"}, "open")
			issue.Body = "body"
			if err := store.UpsertIssue(ctx, issue); err != nil {
				t.Fatal(err)
			}
			if _, err := store.UpsertHandoff(ctx, tt.handoff); err != nil {
				t.Fatal(err)
			}
			gh := &fakeRouterGitHub{issue: issue}
			result, err := RouteWithGitHub(ctx, cfg, store, gh)
			if err != nil {
				t.Fatalf("RouteWithGitHub() error = %v", err)
			}
			if result.GateBlocked != 1 || result.JobsCreated != 0 {
				t.Fatalf("result = %#v", result)
			}
			if strings.Join(gh.comments, "|") != "blocked: "+tt.want {
				t.Fatalf("comments = %#v, want reason %q", gh.comments, tt.want)
			}
		})
	}
}

func TestGateTargetHeadUnchangedReservedBehaviorBlocks(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx)
	defer store.Close()
	cfg := gatedTestConfig()
	cfg.Routes[0].Gate.Handoff.Freshness = config.HandoffFreshnessTargetHeadUnchanged
	issue := testIssue([]string{"agent-ready"}, "open")
	issue.Body = "body"
	if err := store.UpsertIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertHandoff(ctx, gateHandoff("h1", "accepted", "code", bodySHA(issue.Body))); err != nil {
		t.Fatal(err)
	}
	gh := &fakeRouterGitHub{issue: issue}
	result, err := RouteWithGitHub(ctx, cfg, store, gh)
	if err != nil {
		t.Fatalf("RouteWithGitHub() error = %v", err)
	}
	if result.GateBlocked != 1 || result.JobsCreated != 0 {
		t.Fatalf("result = %#v", result)
	}
	if strings.Join(gh.comments, "|") != "blocked: "+model.GateBlockReasonTargetStale {
		t.Fatalf("comments = %#v", gh.comments)
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
		GitHub: config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo"},
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

func gatedTestConfig() config.Config {
	cfg := testConfig()
	cfg.Routes[0].Gate = config.GateConfig{
		Handoff: config.HandoffGateConfig{Required: true, From: []string{"triage"}, Decisions: []string{"accepted"}, NextRoute: config.HandoffNextRouteConfig{Mode: config.HandoffNextRouteCurrent}, Freshness: config.HandoffFreshnessSourceUnchanged},
		OnBlock: config.ActionConfig{LabelsAdd: []string{"agent-needs-human"}, Comment: "blocked: {{ gate.reason }}"},
	}
	return cfg
}

func gateHandoff(id, decision, nextRoute, fingerprint string) model.Handoff {
	return model.Handoff{ID: id, IssueKey: "github.com/example-org/example-repo#1", RouteName: "triage", Decision: decision, NextRoute: nextRoute, SourceKind: "github_issue", SourceKey: "#1", SourceFingerprint: fingerprint, TargetKind: "github_issue", TargetKey: "#1", PayloadJSON: `{"schema":"issueq-handoff/v1"}`, CreatedAt: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
}

func bodySHA(body string) string {
	h := sha256.Sum256([]byte(body))
	return hex.EncodeToString(h[:])
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
