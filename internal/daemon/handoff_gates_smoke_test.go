package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	sqlitestore "issueq/internal/store/sqlite"
	"issueq/internal/supervisor"
)

const smokeIssueKey = "github.com/example-org/example-repo#191"

func TestLocalHandoffGatesSmoke(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "issueq.db")
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	countPath := filepath.Join(dir, "bug-fix-count")
	command := writeSmokeCommand(t, dir, "bug-fix.sh", `#!/bin/sh
set -eu
count=0
if [ -f "$ISSUEQ_SMOKE_COUNT_FILE" ]; then
  count=$(cat "$ISSUEQ_SMOKE_COUNT_FILE")
fi
count=$((count + 1))
printf '%s' "$count" > "$ISSUEQ_SMOKE_COUNT_FILE"
printf '{"comment":"fixture bug-fix ran","labels_add":["agent-done"],"labels_remove":["agent-running"],"work_started":true}\n' > "$2"
`)
	cfg := smokeConfig(dir, command)
	cfg.Runner.Env.Pass = []string{"PATH", "ISSUEQ_SMOKE_COUNT_FILE"}
	t.Setenv("ISSUEQ_SMOKE_COUNT_FILE", countPath)

	issue := smokeIssue([]string{"agent-ready", "agent-route-bug-fix-pr", "agent-write-approved"})
	gh := newSmokeGitHub(issue)
	backend := newLocalExecSupervisor()

	first, err := onceWithSupervisor(ctx, cfg, store, gh, backend)
	if err != nil {
		t.Fatalf("first onceWithSupervisor() error = %v", err)
	}
	if first.Route.GateBlocked != 1 || first.Route.GateBlocksRecorded != 1 || first.Route.JobsCreated != 0 || first.Dispatch.Claimed != 0 {
		t.Fatalf("first result = %#v", first)
	}
	if got := queryInt(t, ctx, dbPath, `SELECT count(*) FROM gate_blocks WHERE issue_key = ? AND route_name = ? AND reason = ?`, smokeIssueKey, "bug-fix-pr", model.GateBlockReasonMissingHandoff); got != 1 {
		t.Fatalf("gate block rows = %d, want 1", got)
	}
	if got := queryInt(t, ctx, dbPath, `SELECT count(*) FROM route_attempts WHERE issue_key = ? AND route_name = ?`, smokeIssueKey, "bug-fix-pr"); got != 0 {
		t.Fatalf("route attempt rows after block = %d, want 0", got)
	}
	if got := queryInt(t, ctx, dbPath, `SELECT count(*) FROM jobs`); got != 0 {
		t.Fatalf("jobs after block = %d, want 0", got)
	}
	if !gh.hasComment("issueq route blocked: missing_handoff") || !gh.hasLabel("agent-needs-human") {
		t.Fatalf("fake GitHub block state labels=%#v comments=%#v", gh.issue.Labels, gh.createdComments())
	}

	gh.addHandoffComment(smokeHandoffComment(issue))
	gh.setLabels("agent-ready", "agent-route-bug-fix-pr", "agent-write-approved")
	second, err := onceWithSupervisor(ctx, cfg, store, gh, backend)
	if err != nil {
		t.Fatalf("second onceWithSupervisor() error = %v", err)
	}
	if second.Poll.HandoffsInserted != 1 || second.Route.JobsCreated != 1 || second.Dispatch.Claimed != 1 || second.Dispatch.Succeeded != 1 {
		t.Fatalf("second result = %#v", second)
	}
	if backend.launchCount() != 1 {
		t.Fatalf("fixture command launches = %d, want 1", backend.launchCount())
	}
	if got := readFile(t, countPath); got != "1" {
		t.Fatalf("fixture command count = %q, want 1", got)
	}
	if got := queryInt(t, ctx, dbPath, `SELECT count(*) FROM handoffs WHERE issue_key = ? AND route_name = ?`, smokeIssueKey, "bug-triage"); got != 1 {
		t.Fatalf("handoffs = %d, want 1", got)
	}
	if got := queryInt(t, ctx, dbPath, `SELECT attempts FROM route_attempts WHERE issue_key = ? AND route_name = ?`, smokeIssueKey, "bug-fix-pr"); got != 1 {
		t.Fatalf("route attempts after accepted handoff = %d, want 1", got)
	}
	if got := queryInt(t, ctx, dbPath, `SELECT count(*) FROM jobs WHERE route_name = ? AND status = ?`, "bug-fix-pr", model.JobStatusSucceeded); got != 1 {
		t.Fatalf("succeeded bug-fix jobs = %d, want 1", got)
	}

	gh.setLabels("agent-ready", "agent-route-bug-fix-pr", "agent-write-approved")
	third, err := onceWithSupervisor(ctx, cfg, store, gh, backend)
	if err != nil {
		t.Fatalf("third onceWithSupervisor() error = %v", err)
	}
	if third.Route.JobsCreated != 1 || third.Dispatch.Claimed != 1 || third.Dispatch.Dead != 1 {
		t.Fatalf("third result = %#v", third)
	}
	if backend.launchCount() != 1 {
		t.Fatalf("fixture command launches after capped rerun = %d, want 1", backend.launchCount())
	}
	if got := queryInt(t, ctx, dbPath, `SELECT count(*) FROM jobs WHERE route_name = ? AND status = ?`, "bug-fix-pr", model.JobStatusDead); got != 1 {
		t.Fatalf("dead bug-fix jobs = %d, want 1", got)
	}
	if !gh.hasComment("fixture max attempts exceeded") || !gh.hasLabel("agent-needs-human") {
		t.Fatalf("fake GitHub exceeded state labels=%#v comments=%#v", gh.issue.Labels, gh.createdComments())
	}
}

func TestLocalWorkStartedFallbackSmoke(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "issueq.db")
	store, err := sqlitestore.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	countPath := filepath.Join(dir, "fallback-count")
	command := writeSmokeCommand(t, dir, "fallback.sh", `#!/bin/sh
set -eu
printf '1' > "$ISSUEQ_SMOKE_COUNT_FILE"
printf '{"comment":"fixture stopped before work","work_started":false}\n' > "$2"
`)
	cfg := smokeConfig(dir, command)
	cfg.Runner.Env.Pass = []string{"PATH", "ISSUEQ_SMOKE_COUNT_FILE"}
	t.Setenv("ISSUEQ_SMOKE_COUNT_FILE", countPath)

	issue := smokeIssue([]string{"agent-ready", "agent-route-bug-fix-pr", "agent-write-approved"})
	gh := newSmokeGitHub(issue)
	gh.addHandoffComment(smokeHandoffComment(issue))
	backend := newLocalExecSupervisor()

	result, err := onceWithSupervisor(ctx, cfg, store, gh, backend)
	if err != nil {
		t.Fatalf("onceWithSupervisor() error = %v", err)
	}
	if result.Route.JobsCreated != 1 || result.Dispatch.Succeeded != 1 || backend.launchCount() != 1 {
		t.Fatalf("result=%#v launches=%d", result, backend.launchCount())
	}
	if got := readFile(t, countPath); got != "1" {
		t.Fatalf("fixture command count = %q, want 1", got)
	}
	if got := queryInt(t, ctx, dbPath, `SELECT attempts FROM route_attempts WHERE issue_key = ? AND route_name = ?`, smokeIssueKey, "bug-fix-pr"); got != 0 {
		t.Fatalf("route attempts after work_started:false = %d, want 0", got)
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.JobStatusSucceeded || jobs[0].Attempts != 0 {
		t.Fatalf("jobs = %#v, want one succeeded zero-attempt fallback job", jobs)
	}
}

func smokeConfig(dir, command string) config.Config {
	return config.Config{
		GitHub:  config.GitHubConfig{Host: "github.com", Owner: "example-org", Repo: "example-repo"},
		Runner:  config.RunnerConfig{Name: "smoke-runner", Capabilities: []string{"bug-fix-pr"}},
		Queue:   config.QueueConfig{MaxGlobalConcurrency: 1, LeaseDuration: config.Duration{Duration: time.Second}},
		Workdir: config.WorkdirConfig{Path: filepath.Join(dir, ".issueq")},
		Routes: []config.RouteConfig{{
			Name: "bug-fix-pr",
			When: config.PredicateConfig{
				LabelsInclude: []string{"agent-ready", "agent-route-bug-fix-pr", "agent-write-approved"},
				LabelsExclude: []string{"agent-running", "agent-done", "agent-failed", "agent-needs-human", "manual-only"},
			},
			Gate: config.GateConfig{
				Handoff: config.HandoffGateConfig{
					Required:  true,
					From:      []string{"bug-triage"},
					Decisions: []string{"bug_fix_candidate"},
					NextRoute: config.HandoffNextRouteConfig{Mode: config.HandoffNextRouteCurrent},
					Freshness: config.HandoffFreshnessSourceUnchanged,
				},
				OnBlock: config.ActionConfig{
					LabelsRemove: []string{"agent-ready", "agent-running"},
					LabelsAdd:    []string{"agent-needs-human"},
					Comment:      "issueq route blocked: {{ gate.reason }}",
				},
			},
			Job: config.JobConfig{
				Kind:         "bug-fix-pr",
				Command:      config.Command{command},
				Timeout:      config.Duration{Duration: time.Second},
				Concurrency:  1,
				MaxAttempts:  1,
				AttemptScope: config.AttemptScopeHandoff,
				OnStart: config.ActionConfig{
					LabelsRemove: []string{"agent-ready"},
					LabelsAdd:    []string{"agent-running"},
				},
				OnAttemptsExceeded: config.ActionConfig{
					LabelsRemove: []string{"agent-ready", "agent-running"},
					LabelsAdd:    []string{"agent-needs-human"},
					Comment:      "fixture max attempts exceeded",
				},
			},
		}},
	}
}

func smokeIssue(labels []string) model.IssueSnapshot {
	return model.IssueSnapshot{
		IssueKey:        smokeIssueKey,
		Host:            "github.com",
		Owner:           "example-org",
		Repo:            "example-repo",
		Number:          191,
		Title:           "Smoke issue",
		Body:            "current smoke body",
		Labels:          append([]string(nil), labels...),
		State:           "open",
		GitHubUpdatedAt: time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC),
	}
}

func smokeHandoffComment(issue model.IssueSnapshot) string {
	return fmt.Sprintf("```issueq-handoff\n{\"schema\":\"issueq-handoff/v1\",\"schema_version\":\"1\",\"route\":\"bug-triage\",\"decision\":\"bug_fix_candidate\",\"next_route\":\"bug-fix-pr\",\"source\":{\"kind\":\"github_issue\",\"issue_number\":191,\"body_sha256\":\"%s\"},\"target\":{\"kind\":\"github_issue\",\"issue_number\":191}}\n```\n\ntriage handoff", bodySHA256(issue.Body))
}

func bodySHA256(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func writeSmokeCommand(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func queryInt(t *testing.T, ctx context.Context, dbPath, query string, args ...any) int {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatal(err)
	}
	return got
}

type smokeGitHub struct {
	mu       sync.Mutex
	issue    model.IssueSnapshot
	comments []model.IssueComment
	clock    time.Time
	nextID   int
}

func newSmokeGitHub(issue model.IssueSnapshot) *smokeGitHub {
	return &smokeGitHub{issue: issue, clock: issue.GitHubUpdatedAt, nextID: 1}
}

func (f *smokeGitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.issue.State != "open" {
		return nil, nil
	}
	return []model.IssueSnapshot{cloneIssue(f.issue)}, nil
}

func (f *smokeGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]model.IssueComment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.IssueComment(nil), f.comments...), nil
}

func (f *smokeGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneIssue(f.issue), nil
}

func (f *smokeGitHub) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]struct{}{}
	for _, label := range f.issue.Labels {
		seen[label] = struct{}{}
	}
	for _, label := range labels {
		if _, ok := seen[label]; ok {
			continue
		}
		f.issue.Labels = append(f.issue.Labels, label)
		seen[label] = struct{}{}
	}
	f.touch()
	return nil
}

func (f *smokeGitHub) SetLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.issue.Labels = append([]string(nil), labels...)
	f.touch()
	return nil
}

func (f *smokeGitHub) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.touch()
	return nil
}

func (f *smokeGitHub) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.comments = append(f.comments, model.IssueComment{ID: fmt.Sprintf("comment-%d", f.nextID), IssueKey: smokeIssueKey, Body: body, CreatedAt: f.clock, UpdatedAt: f.clock})
	f.nextID++
	return nil
}

func (f *smokeGitHub) addHandoffComment(body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.comments = append(f.comments, model.IssueComment{ID: fmt.Sprintf("handoff-%d", f.nextID), IssueKey: smokeIssueKey, Body: body, CreatedAt: f.clock.Add(time.Second), UpdatedAt: f.clock.Add(time.Second)})
	f.nextID++
}

func (f *smokeGitHub) setLabels(labels ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.issue.Labels = append([]string(nil), labels...)
	f.touch()
}

func (f *smokeGitHub) hasLabel(label string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.issue.Labels {
		if existing == label {
			return true
		}
	}
	return false
}

func (f *smokeGitHub) hasComment(part string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, comment := range f.comments {
		if strings.Contains(comment.Body, part) {
			return true
		}
	}
	return false
}

func (f *smokeGitHub) createdComments() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.comments))
	for _, comment := range f.comments {
		out = append(out, comment.Body)
	}
	return out
}

func (f *smokeGitHub) touch() {
	f.clock = f.clock.Add(time.Second)
	f.issue.GitHubUpdatedAt = f.clock
}

func cloneIssue(issue model.IssueSnapshot) model.IssueSnapshot {
	issue.Labels = append([]string(nil), issue.Labels...)
	sort.Strings(issue.Labels)
	return issue
}

type localExecSupervisor struct {
	mu           sync.Mutex
	launches     []supervisor.LaunchSpec
	observations map[string]supervisor.Observation
}

func newLocalExecSupervisor() *localExecSupervisor {
	return &localExecSupervisor{observations: map[string]supervisor.Observation{}}
}

func (s *localExecSupervisor) Launch(ctx context.Context, spec supervisor.LaunchSpec) (supervisor.LaunchRecord, error) {
	started := time.Now().UTC()
	cmd := exec.CommandContext(ctx, spec.Command[0], spec.Command[1:]...)
	cmd.Env = spec.Env
	cmd.Dir = spec.Workdir
	if spec.StdoutPath != "" {
		stdout, err := os.Create(spec.StdoutPath)
		if err != nil {
			return supervisor.LaunchRecord{}, err
		}
		defer stdout.Close()
		cmd.Stdout = stdout
	}
	if spec.StderrPath != "" {
		stderr, err := os.Create(spec.StderrPath)
		if err != nil {
			return supervisor.LaunchRecord{}, err
		}
		defer stderr.Close()
		cmd.Stderr = stderr
	}
	err := cmd.Run()
	finished := time.Now().UTC()
	obs := supervisor.Observation{State: supervisor.RunExited, HasExitCode: true, ExitCode: 0, StartedAt: started, FinishedAt: finished, ResultPath: spec.ResultPath, StdoutPath: spec.StdoutPath, StderrPath: spec.StderrPath}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			obs.ExitCode = exitErr.ExitCode()
		} else {
			obs.State = supervisor.RunFailed
			obs.Error = err.Error()
		}
	}
	s.mu.Lock()
	s.launches = append(s.launches, spec)
	s.observations[spec.JobID] = obs
	s.mu.Unlock()
	return supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: spec.JobID + "-local", JobID: spec.JobID, LaunchToken: spec.LaunchToken, StartedAt: started, TimeoutAt: started.Add(spec.Timeout), MetadataPath: spec.MetadataPath}, nil
}

func (s *localExecSupervisor) Inspect(ctx context.Context, record supervisor.LaunchRecord) (supervisor.Observation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obs, ok := s.observations[record.JobID]
	if !ok {
		return supervisor.Observation{State: supervisor.RunUnknown, Error: "missing local smoke observation"}, nil
	}
	return obs, nil
}

func (s *localExecSupervisor) Cancel(ctx context.Context, record supervisor.LaunchRecord, reason supervisor.CancelReason) error {
	return nil
}

func (s *localExecSupervisor) launchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.launches)
}
