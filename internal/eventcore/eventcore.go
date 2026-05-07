package eventcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/store"
	sqlitestore "issueq/internal/store/sqlite"
)

const EventSchema = "issueq-event/v1"
const ResultSchema = "issueq-agent-result/v1"

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
		return nil, store.ErrEventNotClaimable
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
		_, _ = st.FinalizeAutomationEvent(ctx, ev.EventKey, store.EventFinalize{Status: model.AutomationEventStatusNeedsHuman, ResultJSON: `{"error":"handoff gate not satisfied"}`, LeaseOwner: leaseOwner, Now: time.Now().UTC()})
		return nil, nil, nil, nil
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
	ctx := EventContext{Schema: "issueq-run-context/v1", Event: map[string]any{"key": ev.EventKey, "kind": ev.Kind, "route": ev.RouteName, "status": ev.Status, "priority": ev.Priority, "attempt": ev.AttemptCount, "max_attempts": route.Job.MaxAttempts, "target_fingerprint": ev.TargetFingerprint, "subscope": ev.Subscope}, Repo: map[string]any{"host": ev.RepoHost, "owner": ev.Owner, "name": ev.Repo, "slug": ev.Owner + "/" + ev.Repo}, Source: map[string]any{"kind": ev.SourceKind, "key": ev.SourceKey, "url": ev.SourceURL}, Target: map[string]any{"kind": ev.TargetKind, "key": ev.TargetKey, "fingerprint": ev.TargetFingerprint}, Payload: payload, Handoff: handoff, Job: map[string]any{"id": ev.EventKey, "attempt": ev.AttemptCount, "max_attempts": route.Job.MaxAttempts, "lease_owner": leaseOwner}, Runner: runner, Paths: p}
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
	return res, nil
}

func FinalizeFromResult(ctx context.Context, cfg config.Config, st *sqlitestore.Store, ev model.AutomationEvent, route config.RouteConfig, leaseOwner string, resultPath string) (bool, error) {
	b, err := os.ReadFile(resultPath)
	if err != nil {
		b = []byte(fmt.Sprintf(`{"schema":"%s","event_key":%q,"route":%q,"status":"failed","decision":"result_missing","summary_markdown":%q}`, ResultSchema, ev.EventKey, route.Name, err.Error()))
	}
	res, err := ValidateResult(ev, route, b)
	if err != nil {
		res = AgentResult{Schema: ResultSchema, EventKey: ev.EventKey, Route: route.Name, Status: model.AutomationEventStatusFailed, Decision: "invalid_result", SummaryMarkdown: err.Error()}
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
		_, _ = st.UpsertEventHandoff(ctx, model.EventHandoff{ID: ev.EventKey + ":" + res.Decision, ProducerEventKey: ev.EventKey, ProducerRoute: route.Name, Decision: res.Decision, NextEventKind: res.Handoff.NextEvent.Kind, NextRoute: res.Handoff.NextEvent.Route, TargetKind: ev.TargetKind, TargetKey: ev.TargetKey, TargetFingerprint: ev.TargetFingerprint, Subscope: ev.Subscope, PayloadJSON: string(hp), CreatedAt: created})
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
	proc := exec.CommandContext(ctx, command[0], command[1:]...)
	proc.Env = EnvFor(cfg, *route, *ev, paths)
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
	proc.Stdout = out
	proc.Stderr = errOut
	if err := proc.Run(); err != nil {
		fallback := fmt.Sprintf(`{"schema":"%s","event_key":%q,"route":%q,"status":"failed","decision":"command_failed","summary_markdown":%q}`, ResultSchema, ev.EventKey, route.Name, err.Error())
		_ = os.WriteFile(paths["result"], []byte(fallback), 0600)
	}
	ok, err := FinalizeFromResult(ctx, cfg, st, *ev, *route, opts.LeaseOwner, paths["result"])
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
