package eventcore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/store"
	sqlitestore "issueq/internal/store/sqlite"
)

func testConfig(dir string) config.Config {
	return config.Config{Runner: config.RunnerConfig{Name: "runner"}, Queue: config.QueueConfig{LeaseDuration: config.Duration{Duration: time.Second}, MaxGlobalConcurrency: 1}, Workdir: config.WorkdirConfig{Path: dir}, GitHub: config.GitHubConfig{TokenEnv: "GITHUB_TOKEN"}, Routes: []config.RouteConfig{
		{Name: "pr-review", EventKind: "pr-review", Job: config.JobConfig{Kind: "event", Command: []string{"/bin/true"}, Timeout: config.Duration{Duration: time.Second}, Concurrency: 1, MaxAttempts: 1, FollowUps: []config.FollowUpConfig{{Decision: "fix_candidate", Kind: "pr-fix", Route: "pr-fix"}}}},
		{Name: "pr-fix", EventKind: "pr-fix", Requires: config.RequiresConfig{Handoff: config.EventHandoffGateConfig{From: "pr-review", Decisions: []string{"fix_candidate"}, SameTarget: true, ExpectedNext: true}}, Job: config.JobConfig{Kind: "event", Command: []string{"/bin/true"}, Timeout: config.Duration{Duration: time.Second}, Concurrency: 1, MaxAttempts: 1}},
	}}
}

func sampleUpsert(kind string) EventUpsert {
	return EventUpsert{Schema: EventSchema, Kind: kind, Repo: RepoInput{Host: "h", Owner: "o", Name: "r"}, Target: TargetInput{Kind: "pull_request", Key: "pr-1", Fingerprint: "head-abcdef"}, Payload: json.RawMessage(`{"x":1}`)}
}

func TestValidateUpsertCanonicalAndRouteSpoof(t *testing.T) {
	cfg := testConfig(t.TempDir())
	ev, err := ValidateUpsert(cfg, sampleUpsert("pr-review"))
	if err != nil {
		t.Fatal(err)
	}
	want := "pr-review:h/o/r:pr-1:head-abcdef"
	if ev.EventKey != want {
		t.Fatalf("key=%s", ev.EventKey)
	}
	in := sampleUpsert("pr-review")
	in.RouteName = "pr-fix"
	if _, err := ValidateUpsert(cfg, in); err == nil {
		t.Fatal("expected spoof rejection")
	}
}

func TestTerminalUpsertProtection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := sqlitestore.Open(ctx, filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := testConfig(dir)
	ev, _, _, err := Upsert(ctx, cfg, st, sampleUpsert("pr-review"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := st.ClaimAutomationEvent(ctx, store.EventClaimOptions{RouteName: "pr-review", LeaseOwner: "one", LeaseDuration: time.Minute, MaxAttempts: 1, Now: time.Now()})
	if err != nil || claimed == nil {
		t.Fatalf("claim %#v %v", claimed, err)
	}
	ok, err := st.FinalizeAutomationEvent(ctx, ev.EventKey, store.EventFinalize{Status: model.AutomationEventStatusSucceeded, ResultJSON: `{"ok":true}`, LeaseOwner: "one", Now: time.Now()})
	if err != nil || !ok {
		t.Fatalf("finalize %v %v", ok, err)
	}
	_, inserted, protected, err := Upsert(ctx, cfg, st, sampleUpsert("pr-review"))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || !protected {
		t.Fatalf("inserted=%v protected=%v", inserted, protected)
	}
	got, _ := st.GetAutomationEvent(ctx, ev.EventKey)
	if got.Status != model.AutomationEventStatusSucceeded || got.ResultJSON == "" {
		t.Fatalf("terminal reset: %#v", got)
	}
}

func TestLeaseLateFinalizerNoopAndAttemptExhaustion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := sqlitestore.Open(ctx, filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := testConfig(dir)
	ev, _, _, _ := Upsert(ctx, cfg, st, sampleUpsert("pr-review"))
	claimed, err := st.ClaimAutomationEvent(ctx, store.EventClaimOptions{RouteName: "pr-review", LeaseOwner: "old", LeaseDuration: time.Nanosecond, MaxAttempts: 1, Now: time.Now().Add(-time.Hour)})
	if err != nil || claimed == nil {
		t.Fatalf("claim %#v %v", claimed, err)
	}
	n, err := st.ReleaseExpiredAutomationEvents(ctx, time.Now(), map[string]int{"pr-review": 1})
	if err != nil || n != 1 {
		t.Fatalf("release n=%d err=%v", n, err)
	}
	ok, err := st.FinalizeAutomationEvent(ctx, ev.EventKey, store.EventFinalize{Status: model.AutomationEventStatusSucceeded, ResultJSON: `{}`, LeaseOwner: "old", Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("late finalizer should no-op")
	}
	got, _ := st.GetAutomationEvent(ctx, ev.EventKey)
	if got.Status != model.AutomationEventStatusFailed {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestHandoffGateAndFollowUp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := sqlitestore.Open(ctx, filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := testConfig(dir)
	fix := sampleUpsert("pr-fix")
	if _, _, _, err := Upsert(ctx, cfg, st, fix); err != nil {
		t.Fatal(err)
	}
	ev, _ := st.GetAutomationEvent(ctx, "pr-fix:h/o/r:pr-1:head-abcdef")
	if _, err := CheckHandoffGate(ctx, st, cfg.Routes[1], ev); err == nil {
		t.Fatal("expected missing handoff")
	}
	_, _ = st.UpsertEventHandoff(ctx, model.EventHandoff{ID: "h1", ProducerEventKey: "p", ProducerRoute: "pr-review", Decision: "fix_candidate", NextRoute: "pr-fix", TargetKind: "pull_request", TargetKey: "pr-1", TargetFingerprint: "head-abcdef", PayloadJSON: `{}`})
	if _, err := CheckHandoffGate(ctx, st, cfg.Routes[1], ev); err != nil {
		t.Fatalf("gate: %v", err)
	}
	rev, _ := st.GetAutomationEvent(ctx, "pr-review:h/o/r:pr-1:head-abcdef")
	if rev.EventKey == "" {
		rev, _, _, _ = Upsert(ctx, cfg, st, sampleUpsert("pr-review"))
	}
	claimed, _ := st.ClaimAutomationEvent(ctx, store.EventClaimOptions{RouteName: "pr-review", LeaseOwner: "r", LeaseDuration: time.Minute, MaxAttempts: 1, Now: time.Now()})
	if claimed == nil {
		t.Fatal("claim review")
	}
	res := `{"schema":"issueq-agent-result/v1","event_key":"` + claimed.EventKey + `","route":"pr-review","status":"succeeded","decision":"fix_candidate","summary_markdown":"s","next_event":{"kind":"pr-fix","payload_patch":{"y":2}}}`
	p := filepath.Join(dir, "result.json")
	_ = os.WriteFile(p, []byte(res), 0600)
	ok, err := FinalizeFromResult(ctx, cfg, st, *claimed, cfg.Routes[0], "r", p)
	if err != nil || !ok {
		t.Fatalf("finalize %v %v", ok, err)
	}
	got, _ := st.GetAutomationEvent(ctx, "pr-fix:h/o/r:pr-1:head-abcdef")
	if got.EventKey == "" {
		t.Fatal("follow-up not created")
	}
}
