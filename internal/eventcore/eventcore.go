package eventcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/store"
	sqlitestore "issueq/internal/store/sqlite"
)

const (
	EventSchema      = "issueq-event/v1"
	RunContextSchema = "issueq-run-context/v1"
	ResultSchema     = "issueq-agent-result/v1"
	HandoffSchema    = "issueq-handoff/v1"
)

type EventUpsert struct {
	Schema    string          `json:"schema"`
	Kind      string          `json:"kind"`
	EventKey  string          `json:"event_key"`
	Priority  int             `json:"priority"`
	Repo      RepoInput       `json:"repo"`
	Source    SourceInput     `json:"source"`
	Target    TargetInput     `json:"target"`
	Subscope  string          `json:"subscope"`
	Payload   json.RawMessage `json:"payload"`
	RouteName string          `json:"route_name"`
}

type RepoInput struct{ Host, Owner, Name string }
type SourceInput struct{ Kind, Key, URL string }
type TargetInput struct{ Kind, Key, Fingerprint string }

type AgentResult struct {
	Schema          string                `json:"schema"`
	EventKey        string                `json:"event_key"`
	Route           string                `json:"route"`
	Status          string                `json:"status"`
	Decision        string                `json:"decision"`
	SummaryMarkdown string                `json:"summary_markdown"`
	WorkStarted     *bool                 `json:"work_started"`
	Handoff         *AgentResultHandoff   `json:"handoff"`
	NextEvent       *AgentResultNextEvent `json:"next_event"`
	Projection      map[string]any        `json:"projection"`
	Safety          map[string]any        `json:"safety"`
}

type AgentResultHandoff struct {
	Schema   string `json:"schema"`
	Producer struct {
		EventKey string `json:"event_key"`
		Route    string `json:"route"`
		Decision string `json:"decision"`
	} `json:"producer"`
	Target struct {
		Kind        string `json:"kind"`
		Key         string `json:"key"`
		Fingerprint string `json:"fingerprint"`
		Subscope    string `json:"subscope"`
	} `json:"target"`
	NextEvent struct {
		Kind  string `json:"kind"`
		Route string `json:"route"`
	} `json:"next_event"`
	Payload   map[string]any `json:"payload"`
	CreatedAt string         `json:"created_at"`
}

type AgentResultNextEvent struct {
	Kind         string         `json:"kind"`
	PayloadPatch map[string]any `json:"payload_patch"`
}

type ApprovalInput struct {
	Decision     string         `json:"decision"`
	NextKind     string         `json:"next_kind"`
	PayloadPatch map[string]any `json:"payload_patch"`
}

type ApprovalResult struct {
	Handoff model.EventHandoff    `json:"handoff"`
	Event   model.AutomationEvent `json:"event"`
	Policy  config.FollowUpConfig `json:"policy"`
}

type EventContext struct {
	Schema  string              `json:"schema"`
	Event   map[string]any      `json:"event"`
	Repo    map[string]any      `json:"repo"`
	Source  map[string]any      `json:"source"`
	Target  map[string]any      `json:"target"`
	Payload json.RawMessage     `json:"payload"`
	Handoff *model.EventHandoff `json:"handoff"`
	Job     map[string]any      `json:"job"`
	Runner  model.RunnerInfo    `json:"runner"`
	Paths   map[string]string   `json:"paths"`
}

func RouteForKind(cfg config.Config, kind string) (config.RouteConfig, bool) {
	for _, r := range cfg.Routes {
		if r.EventKind == kind {
			return r, true
		}
	}
	return config.RouteConfig{}, false
}

func MaxAttemptsByRoute(cfg config.Config) map[string]int {
	m := map[string]int{}
	for _, r := range cfg.Routes {
		if r.Name != "" {
			m[r.Name] = r.Job.MaxAttempts
		}
	}
	return m
}

func ValidateUpsert(cfg config.Config, in EventUpsert) (model.AutomationEvent, error) {
	if in.Schema != EventSchema {
		return model.AutomationEvent{}, fmt.Errorf("schema must be %s", EventSchema)
	}
	if strings.TrimSpace(in.Kind) == "" {
		return model.AutomationEvent{}, errors.New("kind is required")
	}
	route, ok := RouteForKind(cfg, in.Kind)
	if !ok {
		return model.AutomationEvent{}, fmt.Errorf("event kind %q is not configured", in.Kind)
	}
	if in.RouteName != "" && in.RouteName != route.Name {
		return model.AutomationEvent{}, fmt.Errorf("route_name %q does not match configured route %q", in.RouteName, route.Name)
	}
	if strings.TrimSpace(in.Repo.Host) == "" || strings.TrimSpace(in.Repo.Owner) == "" || strings.TrimSpace(in.Repo.Name) == "" {
		return model.AutomationEvent{}, errors.New("repo host owner and name are required")
	}
	if strings.TrimSpace(in.Target.Kind) == "" || strings.TrimSpace(in.Target.Key) == "" || strings.TrimSpace(in.Target.Fingerprint) == "" {
		return model.AutomationEvent{}, errors.New("target kind key and fingerprint are required")
	}
	payload := strings.TrimSpace(string(in.Payload))
	if payload == "" || payload == "null" {
		payload = "{}"
	}
	var tmp any
	if err := json.Unmarshal([]byte(payload), &tmp); err != nil {
		return model.AutomationEvent{}, fmt.Errorf("payload must be valid JSON: %w", err)
	}
	repo := model.EventRepoRef{Host: in.Repo.Host, Owner: in.Repo.Owner, Name: in.Repo.Name}
	target := model.EventTargetRef{Kind: in.Target.Kind, Key: in.Target.Key, Fingerprint: in.Target.Fingerprint}
	canonical := model.CanonicalEventKey(in.Kind, repo, target, in.Subscope)
	if in.EventKey != "" && in.EventKey != canonical {
		return model.AutomationEvent{}, fmt.Errorf("event_key %q does not match canonical %q", in.EventKey, canonical)
	}
	return model.AutomationEvent{EventKey: canonical, Kind: in.Kind, RouteName: route.Name, Status: model.AutomationEventStatusReady, Priority: in.Priority, RepoHost: in.Repo.Host, Owner: in.Repo.Owner, Repo: in.Repo.Name, SourceKind: in.Source.Kind, SourceKey: in.Source.Key, SourceURL: in.Source.URL, TargetKind: in.Target.Kind, TargetKey: in.Target.Key, TargetFingerprint: in.Target.Fingerprint, Subscope: in.Subscope, PayloadJSON: payload}, nil
}

func Upsert(ctx context.Context, cfg config.Config, st *sqlitestore.Store, in EventUpsert) (model.AutomationEvent, bool, bool, error) {
	ev, err := ValidateUpsert(cfg, in)
	if err != nil {
		return model.AutomationEvent{}, false, false, err
	}
	return st.UpsertAutomationEvent(ctx, ev)
}

func Approve(ctx context.Context, cfg config.Config, st *sqlitestore.Store, eventKey string, in ApprovalInput) (ApprovalResult, error) {
	if strings.TrimSpace(in.Decision) == "" {
		return ApprovalResult{}, errors.New("approval decision is required")
	}
	if strings.TrimSpace(in.NextKind) == "" {
		return ApprovalResult{}, errors.New("approval next_kind is required")
	}
	ev, err := st.GetAutomationEvent(ctx, eventKey)
	if err != nil {
		return ApprovalResult{}, err
	}
	route, ok := RouteForKind(cfg, ev.Kind)
	if !ok || route.Name != ev.RouteName {
		return ApprovalResult{}, fmt.Errorf("event route %q for kind %q is not configured", ev.RouteName, ev.Kind)
	}
	var policy config.FollowUpConfig
	for _, f := range route.Job.FollowUps {
		if f.Decision == in.Decision && f.Kind == in.NextKind {
			policy = f
			break
		}
	}
	if policy.Kind == "" {
		return ApprovalResult{}, fmt.Errorf("approval decision %q next_kind %q is not allowed by route %q policy", in.Decision, in.NextKind, route.Name)
	}
	now := time.Now().UTC()
	handoffPayload, _ := json.Marshal(map[string]any{"trusted_approval": true, "payload_patch": in.PayloadPatch})
	h := model.EventHandoff{ID: ev.EventKey + ":approval:" + in.Decision + ":" + in.NextKind, ProducerEventKey: ev.EventKey, ProducerRoute: route.Name, Decision: in.Decision, NextEventKind: policy.Kind, NextRoute: policy.Route, TargetKind: ev.TargetKind, TargetKey: ev.TargetKey, TargetFingerprint: ev.TargetFingerprint, Subscope: ev.Subscope, PayloadJSON: string(handoffPayload), CreatedAt: now}
	if _, err := st.UpsertEventHandoff(ctx, h); err != nil {
		return ApprovalResult{}, err
	}
	nextPayload := mergePayload(ev.PayloadJSON, in.PayloadPatch)
	next := model.AutomationEvent{EventKey: model.CanonicalEventKey(policy.Kind, model.EventRepoRef{Host: ev.RepoHost, Owner: ev.Owner, Name: ev.Repo}, model.EventTargetRef{Kind: ev.TargetKind, Key: ev.TargetKey, Fingerprint: ev.TargetFingerprint}, ev.Subscope), Kind: policy.Kind, RouteName: policy.Route, Status: model.AutomationEventStatusReady, Priority: ev.Priority, RepoHost: ev.RepoHost, Owner: ev.Owner, Repo: ev.Repo, SourceKind: ev.SourceKind, SourceKey: ev.SourceKey, SourceURL: ev.SourceURL, TargetKind: ev.TargetKind, TargetKey: ev.TargetKey, TargetFingerprint: ev.TargetFingerprint, Subscope: ev.Subscope, PayloadJSON: nextPayload}
	stored, _, _, err := st.UpsertAutomationEvent(ctx, next)
	if err != nil {
		return ApprovalResult{}, err
	}
	return ApprovalResult{Handoff: h, Event: stored, Policy: policy}, nil
}

type HandoffGateError struct {
	Reason store.EventBlockReason
}

func (e HandoffGateError) Error() string {
	if e.Reason.Message != "" {
		return e.Reason.Message
	}
	return e.Reason.Code
}

func CheckHandoffGate(ctx context.Context, st *sqlitestore.Store, route config.RouteConfig, ev model.AutomationEvent) (*model.EventHandoff, error) {
	req := route.Requires.Handoff
	if req.From == "" {
		return nil, nil
	}
	nextRoute := ""
	if req.ExpectedNext {
		nextRoute = route.Name
	}
	h, err := st.LatestMatchingEventHandoff(ctx, req.From, req.Decisions, nextRoute, ev.TargetKind, ev.TargetKey, ev.TargetFingerprint, ev.Subscope)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, HandoffGateError{Reason: store.EventBlockReason{Code: "required_handoff_not_satisfied", Message: fmt.Sprintf("required handoff from route %q for target %s/%s@%s is missing or mismatched", req.From, ev.TargetKind, ev.TargetKey, ev.TargetFingerprint)}}
	}
	return h, nil
}

func ClaimOne(ctx context.Context, cfg config.Config, st *sqlitestore.Store, leaseOwner string, lease time.Duration, workdir string, runner model.RunnerInfo) (*model.AutomationEvent, *config.RouteConfig, *model.EventHandoff, error) {
	_, _ = st.ReleaseExpiredAutomationEvents(ctx, time.Now().UTC(), MaxAttemptsByRoute(cfg))
	for _, route := range cfg.Routes {
		if route.EventKind == "" {
			continue
		}
		ev, err := st.ClaimAutomationEvent(ctx, store.EventClaimOptions{RouteName: route.Name, LeaseOwner: leaseOwner, LeaseDuration: lease, MaxAttempts: route.Job.MaxAttempts, Now: time.Now().UTC()})
		if err != nil {
			return nil, nil, nil, err
		}
		if ev == nil {
			continue
		}
		h, err := CheckHandoffGate(ctx, st, route, *ev)
		if err == nil {
			return ev, &route, h, nil
		}
		var gateErr HandoffGateError
		if errors.As(err, &gateErr) {
			_ = st.BlockAutomationEvent(ctx, ev.EventKey, gateErr.Reason)
			return nil, nil, nil, nil
		}
		return nil, nil, nil, err
	}
	return nil, nil, nil, nil
}

func Paths(workdir, eventKey string) map[string]string {
	safe := strings.NewReplacer("/", "_", ":", "_", "#", "_").Replace(eventKey)
	dir := filepath.Join(workdir, "events", safe)
	return map[string]string{"dir": dir, "context": filepath.Join(dir, "context.json"), "result": filepath.Join(dir, "result.json"), "stdout": filepath.Join(dir, "stdout.log"), "stderr": filepath.Join(dir, "stderr.log")}
}

func WriteContext(workdir string, ev model.AutomationEvent, route config.RouteConfig, handoff *model.EventHandoff, runner model.RunnerInfo, leaseOwner string) (map[string]string, error) {
	p := Paths(workdir, ev.EventKey)
	if err := os.MkdirAll(p["dir"], 0700); err != nil {
		return nil, err
	}
	var payload json.RawMessage = []byte(ev.PayloadJSON)
	ctx := EventContext{Schema: RunContextSchema, Event: map[string]any{"key": ev.EventKey, "kind": ev.Kind, "route": ev.RouteName, "status": ev.Status, "priority": ev.Priority, "attempt": ev.AttemptCount, "max_attempts": route.Job.MaxAttempts, "target_fingerprint": ev.TargetFingerprint, "subscope": ev.Subscope}, Repo: map[string]any{"host": ev.RepoHost, "owner": ev.Owner, "name": ev.Repo, "slug": ev.Owner + "/" + ev.Repo}, Source: map[string]any{"kind": ev.SourceKind, "key": ev.SourceKey, "url": ev.SourceURL}, Target: map[string]any{"kind": ev.TargetKind, "key": ev.TargetKey, "fingerprint": ev.TargetFingerprint}, Payload: payload, Handoff: handoff, Job: map[string]any{"id": ev.EventKey, "attempt": ev.AttemptCount, "max_attempts": route.Job.MaxAttempts, "lease_owner": leaseOwner}, Runner: runner, Paths: p}
	b, err := json.MarshalIndent(ctx, "", "  ")
	if err != nil {
		return nil, err
	}
	return p, os.WriteFile(p["context"], b, 0600)
}

func EnvFor(cfg config.Config, route config.RouteConfig, ev model.AutomationEvent, p map[string]string) []string {
	env := map[string]string{"ISSUEQ_CONTEXT_PATH": p["context"], "ISSUEQ_RESULT_PATH": p["result"], "ISSUEQ_EVENT_KEY": ev.EventKey, "ISSUEQ_EVENT_KIND": ev.Kind, "ISSUEQ_ROUTE": ev.RouteName, "ISSUEQ_ATTEMPT": fmt.Sprint(ev.AttemptCount), "GITHUB_HOST": ev.RepoHost, "GITHUB_OWNER": ev.Owner, "GITHUB_REPO": ev.Repo}
	for _, pair := range os.Environ() {
		k, v, ok := strings.Cut(pair, "=")
		if ok {
			env[k] = v
		}
	}
	delete(env, cfg.GitHub.TokenEnv)
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}

func ValidateResult(ev model.AutomationEvent, route config.RouteConfig, b []byte) (AgentResult, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return AgentResult{}, fmt.Errorf("parse result: %w", err)
	}
	allowed := map[string]struct{}{"schema": {}, "event_key": {}, "route": {}, "status": {}, "decision": {}, "summary_markdown": {}, "work_started": {}, "handoff": {}, "next_event": {}, "projection": {}, "safety": {}}
	for key := range raw {
		if _, ok := allowed[key]; !ok {
			return AgentResult{}, fmt.Errorf("unknown top-level result field %q", key)
		}
	}
	var res AgentResult
	if err := json.Unmarshal(b, &res); err != nil {
		return res, fmt.Errorf("parse result: %w", err)
	}
	if res.Schema != ResultSchema {
		return res, fmt.Errorf("result schema must be %s", ResultSchema)
	}
	if res.EventKey != ev.EventKey {
		return res, errors.New("result event_key mismatch")
	}
	if res.Route != route.Name {
		return res, errors.New("result route mismatch")
	}
	switch res.Status {
	case model.AutomationEventStatusSucceeded, model.AutomationEventStatusFailed, model.AutomationEventStatusStale, model.AutomationEventStatusNeedsHuman:
	default:
		return res, fmt.Errorf("invalid result status %q", res.Status)
	}
	if res.Decision == "" {
		return res, errors.New("decision is required")
	}
	if res.SummaryMarkdown == "" {
		return res, errors.New("summary_markdown is required")
	}
	if res.WorkStarted == nil {
		return res, errors.New("work_started is required")
	}
	if err := validateProjection(res.Projection); err != nil {
		return res, err
	}
	if res.Handoff != nil {
		if res.Handoff.Schema != HandoffSchema {
			return res, fmt.Errorf("handoff schema must be %s", HandoffSchema)
		}
		if res.Handoff.Producer.EventKey != ev.EventKey {
			return res, errors.New("handoff producer event_key mismatch")
		}
		if res.Handoff.Producer.Route != route.Name {
			return res, errors.New("handoff producer route mismatch")
		}
		if res.Handoff.Producer.Decision != res.Decision {
			return res, errors.New("handoff producer decision mismatch")
		}
		if res.Handoff.Target.Kind != ev.TargetKind || res.Handoff.Target.Key != ev.TargetKey || res.Handoff.Target.Fingerprint != ev.TargetFingerprint || normalizeSubscope(res.Handoff.Target.Subscope) != normalizeSubscope(ev.Subscope) {
			return res, errors.New("handoff target mismatch")
		}
	}
	if res.NextEvent != nil {
		if strings.TrimSpace(res.NextEvent.Kind) == "" {
			return res, errors.New("next_event.kind is required")
		}
		if res.Handoff == nil {
			return res, errors.New("next_event requires handoff")
		}
		if res.Handoff.NextEvent.Kind != res.NextEvent.Kind {
			return res, errors.New("handoff next_event kind mismatch")
		}
		if res.Handoff.NextEvent.Route == "" {
			return res, errors.New("handoff next_event route is required")
		}
	}
	return res, nil
}

func validateProjection(projection map[string]any) error {
	if projection == nil {
		return nil
	}
	allowedLabels := map[string]struct{}{
		"agent-active":      {},
		"agent-merge-ready": {},
		"agent-needs-human": {},
		"agent-stale":       {},
		"agent-failed":      {},
	}
	for key, value := range projection {
		switch key {
		case "comment", "summary", "target_url":
			if _, ok := value.(string); !ok {
				return fmt.Errorf("projection.%s must be a string", key)
			}
		case "labels":
			items, ok := value.([]any)
			if !ok {
				return errors.New("projection.labels must be an array")
			}
			for _, item := range items {
				label, ok := item.(string)
				if !ok {
					return errors.New("projection.labels must contain strings")
				}
				if _, ok := allowedLabels[label]; !ok {
					return fmt.Errorf("projection label %q is not UI-only allowlisted", label)
				}
			}
		default:
			return fmt.Errorf("unknown projection field %q", key)
		}
	}
	return nil
}

func normalizeSubscope(value string) string {
	return strings.TrimSpace(value)
}

func FinalizeFromResult(ctx context.Context, cfg config.Config, st *sqlitestore.Store, ev model.AutomationEvent, route config.RouteConfig, leaseOwner string, resultPath string) (bool, error) {
	b, err := os.ReadFile(resultPath)
	if err != nil {
		b = []byte(fmt.Sprintf(`{"schema":"%s","event_key":%q,"route":%q,"status":"failed","decision":"result_missing","summary_markdown":%q,"work_started":false}`, ResultSchema, ev.EventKey, route.Name, err.Error()))
	}
	res, err := ValidateResult(ev, route, b)
	if err != nil {
		workStarted := false
		res = AgentResult{Schema: ResultSchema, EventKey: ev.EventKey, Route: route.Name, Status: model.AutomationEventStatusFailed, Decision: "invalid_result", SummaryMarkdown: err.Error(), WorkStarted: &workStarted}
		b, _ = json.Marshal(res)
	}
	ok, err := st.FinalizeAutomationEvent(ctx, ev.EventKey, store.EventFinalize{Status: res.Status, ResultJSON: string(b), LeaseOwner: leaseOwner, Now: time.Now().UTC()})
	if err != nil || !ok {
		return ok, err
	}
	if res.Handoff != nil {
		hp, _ := json.Marshal(res.Handoff.Payload)
		created := time.Now().UTC()
		if res.Handoff.CreatedAt != "" {
			if t, e := time.Parse(time.RFC3339, res.Handoff.CreatedAt); e == nil {
				created = t
			}
		}
		_, _ = st.UpsertEventHandoff(ctx, model.EventHandoff{ID: ev.EventKey + ":" + res.Decision, ProducerEventKey: ev.EventKey, ProducerRoute: route.Name, Decision: res.Decision, NextEventKind: res.Handoff.NextEvent.Kind, NextRoute: res.Handoff.NextEvent.Route, TargetKind: res.Handoff.Target.Kind, TargetKey: res.Handoff.Target.Key, TargetFingerprint: res.Handoff.Target.Fingerprint, Subscope: normalizeSubscope(res.Handoff.Target.Subscope), PayloadJSON: string(hp), CreatedAt: created})
	}
	if res.NextEvent != nil {
		for _, f := range route.Job.FollowUps {
			if f.Decision == res.Decision && f.Kind == res.NextEvent.Kind {
				payload := mergePayload(ev.PayloadJSON, res.NextEvent.PayloadPatch)
				next := model.AutomationEvent{EventKey: model.CanonicalEventKey(f.Kind, model.EventRepoRef{Host: ev.RepoHost, Owner: ev.Owner, Name: ev.Repo}, model.EventTargetRef{Kind: ev.TargetKind, Key: ev.TargetKey, Fingerprint: ev.TargetFingerprint}, ev.Subscope), Kind: f.Kind, RouteName: f.Route, Status: model.AutomationEventStatusReady, Priority: ev.Priority, RepoHost: ev.RepoHost, Owner: ev.Owner, Repo: ev.Repo, SourceKind: ev.SourceKind, SourceKey: ev.SourceKey, SourceURL: ev.SourceURL, TargetKind: ev.TargetKind, TargetKey: ev.TargetKey, TargetFingerprint: ev.TargetFingerprint, Subscope: ev.Subscope, PayloadJSON: payload}
				_, _, _, _ = st.UpsertAutomationEvent(ctx, next)
			}
		}
	}
	return true, nil
}

func mergePayload(base string, patch map[string]any) string {
	var m map[string]any
	_ = json.Unmarshal([]byte(base), &m)
	if m == nil {
		m = map[string]any{}
	}
	for k, v := range patch {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return string(b)
}

type RunResult struct {
	Claimed   int
	Finalized int
}

type RunOptions struct {
	LeaseOwner string
	Lease      time.Duration
	Workdir    string
	Runner     model.RunnerInfo
}

func RunOnce(ctx context.Context, cfg config.Config, st *sqlitestore.Store, opts RunOptions) (RunResult, string, error) {
	if opts.LeaseOwner == "" {
		opts.LeaseOwner = fmt.Sprintf("issueq-%d", os.Getpid())
	}
	if opts.Lease <= 0 {
		opts.Lease = cfg.Queue.LeaseDuration.Duration
	}
	if opts.Lease <= 0 {
		opts.Lease = 30 * time.Minute
	}
	if opts.Workdir == "" {
		opts.Workdir = cfg.Workdir.Path
	}
	if opts.Runner.ID == "" {
		opts.Runner.ID = opts.LeaseOwner
	}
	if opts.Runner.Name == "" {
		opts.Runner.Name = cfg.Runner.Name
	}
	ev, route, handoff, err := ClaimOne(ctx, cfg, st, opts.LeaseOwner, opts.Lease, opts.Workdir, opts.Runner)
	if err != nil {
		return RunResult{}, "", err
	}
	if ev == nil {
		return RunResult{}, "", nil
	}
	if len(route.Job.Command) == 0 {
		_, _ = st.FinalizeAutomationEvent(ctx, ev.EventKey, store.EventFinalize{Status: model.AutomationEventStatusFailed, ResultJSON: `{"error":"route command is empty"}`, LeaseOwner: opts.LeaseOwner, Now: time.Now().UTC()})
		return RunResult{Claimed: 1, Finalized: 1}, ev.EventKey, nil
	}
	paths, err := WriteContext(opts.Workdir, *ev, *route, handoff, opts.Runner, opts.LeaseOwner)
	if err != nil {
		return RunResult{Claimed: 1}, ev.EventKey, err
	}
	command := append([]string(nil), route.Job.Command...)
	command = append(command, paths["context"], paths["result"])
	out, err := os.Create(paths["stdout"])
	if err != nil {
		return RunResult{Claimed: 1}, ev.EventKey, err
	}
	defer out.Close()
	errOut, err := os.Create(paths["stderr"])
	if err != nil {
		return RunResult{Claimed: 1}, ev.EventKey, err
	}
	defer errOut.Close()
	runCtx := ctx
	var cancel context.CancelFunc
	if route.Job.Timeout.Duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, route.Job.Timeout.Duration)
		defer cancel()
	}
	if err := runCommand(runCtx, command, EnvFor(cfg, *route, *ev, paths), out, errOut, 5*time.Second); err != nil {
		fallback := fmt.Sprintf(`{"schema":"%s","event_key":%q,"route":%q,"status":"failed","decision":"command_failed","summary_markdown":%q,"work_started":true}`, ResultSchema, ev.EventKey, route.Name, err.Error())
		_ = os.WriteFile(paths["result"], []byte(fallback), 0600)
	}
	ok, err := FinalizeFromResult(context.WithoutCancel(ctx), cfg, st, *ev, *route, opts.LeaseOwner, paths["result"])
	if err != nil {
		return RunResult{Claimed: 1}, ev.EventKey, err
	}
	result := RunResult{Claimed: 1}
	if ok {
		result.Finalized = 1
	}
	return result, ev.EventKey, nil
}

func RunLoop(ctx context.Context, cfg config.Config, st *sqlitestore.Store, logger *slog.Logger, opts RunOptions, idleInterval time.Duration) error {
	if logger == nil {
		logger = slog.Default()
	}
	if idleInterval <= 0 {
		idleInterval = cfg.Polling.Interval.Duration
	}
	if idleInterval <= 0 {
		idleInterval = 3 * time.Minute
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		result, key, err := RunOnce(ctx, cfg, st, opts)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("event runner failed", "error", err)
		} else if result.Claimed > 0 {
			logger.Info("event runner completed", "event_key", key, "finalized", result.Finalized)
			continue
		}
		timer := time.NewTimer(idleInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
