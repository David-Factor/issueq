// Package router evaluates configured routes and enqueues matching jobs.
package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"issueq/internal/actions"
	"issueq/internal/config"
	issuegithub "issueq/internal/github"
	"issueq/internal/model"
	"issueq/internal/store"
)

type Result struct {
	IssuesEvaluated    int
	RoutesMatched      int
	JobsCreated        int
	JobsExisting       int
	GateBlocked        int
	GateBlocksRecorded int
	GateBlocksExisting int
}

type gateDecision struct {
	Blocked bool
	Reason  string
	Handoff *model.Handoff
}

var ErrAttemptScopeBlocked = errors.New("attempt scope blocked by handoff gate")

func Route(ctx context.Context, cfg config.Config, queue store.QueueStore) (Result, error) {
	return RouteWithGitHub(ctx, cfg, queue, nil)
}

func RouteWithGitHub(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client) (Result, error) {
	issues, err := queue.ListRoutableIssues(ctx)
	if err != nil {
		return Result{}, err
	}

	var result Result
	result.IssuesEvaluated = len(issues)
	for _, issue := range issues {
		current := issue
		for _, route := range cfg.Routes {
			if !Matches(cfg, route, current) {
				continue
			}
			result.RoutesMatched++
			decision, err := EvaluateGate(ctx, queue, route, current)
			if err != nil {
				return Result{}, fmt.Errorf("evaluate gate for route %q issue %s: %w", route.Name, current.IssueKey, err)
			}
			if decision.Blocked {
				blocked, err := recordGateBlock(ctx, queue, route, current, decision)
				if err != nil {
					return Result{}, fmt.Errorf("record gate block for route %q issue %s: %w", route.Name, current.IssueKey, err)
				}
				result.GateBlocked++
				if blocked.Inserted {
					result.GateBlocksRecorded++
				} else {
					result.GateBlocksExisting++
				}
				if !blocked.ActionApplied && gh != nil {
					applied, didApply, err := applyGateBlockAction(ctx, cfg, queue, gh, current, route.Gate.OnBlock, decision.Reason)
					if err != nil {
						return Result{}, fmt.Errorf("apply gate block action for route %q issue %s: %w", route.Name, current.IssueKey, err)
					}
					if didApply {
						if err := queue.MarkGateBlockActionApplied(ctx, blocked.Block); err != nil {
							return Result{}, fmt.Errorf("mark gate block action applied for route %q issue %s: %w", route.Name, current.IssueKey, err)
						}
					}
					current = applied
				}
				continue
			}
			job, inserted, err := queue.EnqueueJob(ctx, JobCreate(route, current))
			if err != nil {
				return Result{}, fmt.Errorf("enqueue route %q for issue %s: %w", route.Name, current.IssueKey, err)
			}
			_ = job
			if inserted {
				result.JobsCreated++
			} else {
				result.JobsExisting++
			}
		}
	}
	return result, nil
}

func Matches(cfg config.Config, route config.RouteConfig, issue model.IssueSnapshot) bool {
	if issue.State != "open" {
		return false
	}
	if !capabilityAllows(cfg.Runner.Capabilities, route.Job.Kind) {
		return false
	}
	labels := labelSet(issue.Labels)
	for _, want := range route.When.LabelsInclude {
		if _, ok := labels[want]; !ok {
			return false
		}
	}
	for _, blocked := range route.When.LabelsExclude {
		if _, ok := labels[blocked]; ok {
			return false
		}
	}
	return true
}

func EvaluateGate(ctx context.Context, queue store.QueueStore, route config.RouteConfig, issue model.IssueSnapshot) (gateDecision, error) {
	gate := route.Gate.Handoff
	if !handoffGateEnabled(gate) {
		return gateDecision{}, nil
	}
	handoff, err := queue.LatestMatchingHandoff(ctx, model.HandoffQuery{IssueKey: issue.IssueKey, RouteNames: gate.From})
	if err != nil {
		return gateDecision{}, err
	}
	if handoff == nil {
		if gate.Required {
			return gateDecision{Blocked: true, Reason: model.GateBlockReasonMissingHandoff}, nil
		}
		return gateDecision{}, nil
	}
	if len(gate.Decisions) > 0 && !containsString(gate.Decisions, handoff.Decision) {
		return gateDecision{Blocked: true, Reason: model.GateBlockReasonDecisionNotAllowed, Handoff: handoff}, nil
	}
	if nextRouteWanted(gate.NextRoute, route.Name) != "" && handoff.NextRoute != nextRouteWanted(gate.NextRoute, route.Name) {
		return gateDecision{Blocked: true, Reason: model.GateBlockReasonNextRouteMismatch, Handoff: handoff}, nil
	}
	switch strings.TrimSpace(gate.Freshness) {
	case "", config.HandoffFreshnessNone:
		return gateDecision{Handoff: handoff}, nil
	case config.HandoffFreshnessSourceUnchanged:
		if !sourceFresh(*handoff, issue) {
			return gateDecision{Blocked: true, Reason: model.GateBlockReasonSourceStale, Handoff: handoff}, nil
		}
		return gateDecision{Handoff: handoff}, nil
	case config.HandoffFreshnessTargetHeadUnchanged:
		return gateDecision{Blocked: true, Reason: model.GateBlockReasonTargetStale, Handoff: handoff}, nil
	default:
		return gateDecision{Handoff: handoff}, nil
	}
}

type recordedGateBlock struct {
	Block         model.GateBlock
	Inserted      bool
	Count         int
	ActionApplied bool
}

func recordGateBlock(ctx context.Context, queue store.QueueStore, route config.RouteConfig, issue model.IssueSnapshot, decision gateDecision) (recordedGateBlock, error) {
	generation, _, err := queue.GetIssueState(ctx, issue.IssueKey)
	if err != nil {
		return recordedGateBlock{}, err
	}
	block := model.GateBlock{IssueKey: issue.IssueKey, Generation: generation, RouteName: route.Name, Reason: decision.Reason, ScopeHash: gateScopeHash(issue, decision), CreatedAt: time.Now().UTC()}
	inserted, count, actionApplied, err := queue.RecordGateBlock(ctx, block)
	return recordedGateBlock{Block: block, Inserted: inserted, Count: count, ActionApplied: actionApplied}, err
}

func applyGateBlockAction(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, issue model.IssueSnapshot, action config.ActionConfig, reason string) (model.IssueSnapshot, bool, error) {
	action.Comment = strings.ReplaceAll(action.Comment, "{{ gate.reason }}", reason)
	action.Comment = strings.ReplaceAll(action.Comment, "{{gate.reason}}", reason)
	if strings.TrimSpace(action.Comment) == "" && len(action.LabelsAdd) == 0 && len(action.LabelsRemove) == 0 {
		return issue, true, nil
	}
	applied, err := actions.Apply(ctx, cfg, gh, queue, issue, action)
	if err != nil {
		return model.IssueSnapshot{}, false, err
	}
	return applied.UpdatedIssue, true, nil
}

func handoffGateEnabled(gate config.HandoffGateConfig) bool {
	return gate.Required || len(gate.From) > 0 || len(gate.Decisions) > 0 || gate.NextRoute.Mode != config.HandoffNextRouteDisabled || strings.TrimSpace(gate.Freshness) != ""
}

func nextRouteWanted(next config.HandoffNextRouteConfig, current string) string {
	switch next.Mode {
	case config.HandoffNextRouteCurrent:
		return current
	case config.HandoffNextRouteExact:
		return next.Value
	default:
		return ""
	}
}

func sourceFresh(handoff model.Handoff, issue model.IssueSnapshot) bool {
	if handoff.SourceKind != "" && handoff.SourceKind != "github_issue" {
		return false
	}
	if handoff.SourceKey != "" && handoff.SourceKey != issue.IssueKey && handoff.SourceKey != fmt.Sprintf("#%d", issue.Number) {
		return false
	}
	if handoff.SourceFingerprint == "" {
		return false
	}
	_, ok := issueFingerprints(issue)[handoff.SourceFingerprint]
	return ok
}

func issueFingerprints(issue model.IssueSnapshot) map[string]struct{} {
	fps := map[string]struct{}{}
	if !issue.GitHubUpdatedAt.IsZero() {
		fps[issue.GitHubUpdatedAt.UTC().Format(time.RFC3339Nano)] = struct{}{}
		fps[issue.GitHubUpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")] = struct{}{}
	}
	fps[issueBodySHA256(issue)] = struct{}{}
	return fps
}

func gateScopeHash(issue model.IssueSnapshot, decision gateDecision) string {
	if decision.Handoff != nil {
		return hashScope("handoff", decision.Handoff.ID)
	}
	return hashScope("issue", issue.IssueKey, issue.GitHubUpdatedAt.UTC().Format(time.RFC3339Nano))
}

func AttemptScopeHash(ctx context.Context, queue store.QueueStore, route config.RouteConfig, issue model.IssueSnapshot) (string, error) {
	descriptor := attemptScopeDescriptorFor(route.Job.AttemptScope)
	if descriptor.namespace == config.AttemptScopeLegacy {
		return config.AttemptScopeLegacy, nil
	}

	decision, err := EvaluateGate(ctx, queue, route, issue)
	if err != nil {
		return "", err
	}
	if decision.Blocked {
		return "", fmt.Errorf("%w: %s", ErrAttemptScopeBlocked, decision.Reason)
	}

	switch descriptor.namespace {
	case config.AttemptScopeHandoff:
		if decision.Handoff == nil {
			return "", fmt.Errorf("%w: %s", ErrAttemptScopeBlocked, model.GateBlockReasonMissingHandoff)
		}
		return handoffAttemptScopeHash(*decision.Handoff), nil
	case config.AttemptScopeIssue:
		return issueAttemptScopeHash(issue), nil
	case config.AttemptScopePRHead, config.AttemptScopeCIHead:
		return targetAttemptScopeHash(descriptor, decision), nil
	default:
		return config.AttemptScopeLegacy, nil
	}
}

func handoffAttemptScopeHash(handoff model.Handoff) string {
	fingerprint := handoff.SourceFingerprint
	if strings.TrimSpace(handoff.ID) != "" {
		fingerprint = handoff.ID
	}
	return hashScope(config.AttemptScopeHandoff, fingerprint)
}

func issueAttemptScopeHash(issue model.IssueSnapshot) string {
	return hashScope(config.AttemptScopeIssue, issue.IssueKey, issue.GitHubUpdatedAt.UTC().Format(time.RFC3339Nano), issueBodySHA256(issue))
}

func targetAttemptScopeHash(descriptor attemptScopeDescriptor, decision gateDecision) string {
	if decision.Handoff == nil || strings.TrimSpace(decision.Handoff.TargetKey) == "" {
		return config.AttemptScopeLegacy
	}
	targetKind := strings.TrimSpace(decision.Handoff.TargetKind)
	if targetKind != "" && !containsString(descriptor.targetKinds, targetKind) {
		return config.AttemptScopeLegacy
	}
	return hashScope(descriptor.namespace, decision.Handoff.TargetKind, decision.Handoff.TargetKey)
}

type attemptScopeDescriptor struct {
	namespace   string
	targetKinds []string
}

func attemptScopeDescriptorFor(scope string) attemptScopeDescriptor {
	switch strings.TrimSpace(scope) {
	case config.AttemptScopeHandoff:
		return attemptScopeDescriptor{namespace: config.AttemptScopeHandoff}
	case config.AttemptScopeIssue:
		return attemptScopeDescriptor{namespace: config.AttemptScopeIssue}
	case config.AttemptScopePRHead:
		return attemptScopeDescriptor{namespace: config.AttemptScopePRHead, targetKinds: []string{"github_pr", "github_pull_request", "pull_request"}}
	case config.AttemptScopeCIHead:
		return attemptScopeDescriptor{namespace: config.AttemptScopeCIHead, targetKinds: []string{"github_ci", "github_check_run", "ci_run"}}
	default:
		return attemptScopeDescriptor{namespace: config.AttemptScopeLegacy}
	}
}

func issueBodySHA256(issue model.IssueSnapshot) string {
	h := sha256.Sum256([]byte(issue.Body))
	return hex.EncodeToString(h[:])
}

func hashScope(namespace string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(namespace))
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func JobCreate(route config.RouteConfig, issue model.IssueSnapshot) model.JobCreate {
	return model.JobCreate{
		IssueKey:  issue.IssueKey,
		RouteName: route.Name,
		Kind:      route.Job.Kind,
		Priority:  route.Job.Priority,
		DedupeKey: DedupeKey(issue, route.Name),
	}
}

func DedupeKey(issue model.IssueSnapshot, routeName string) string {
	return strings.Join([]string{
		issue.IssueKey,
		routeName,
		LabelHash(issue.Labels),
		issue.GitHubUpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
	}, ":")
}

func LabelHash(labels []string) string {
	copyLabels := append([]string(nil), labels...)
	sort.Strings(copyLabels)
	h := sha256.New()
	for _, label := range copyLabels {
		h.Write([]byte(label))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func labelSet(labels []string) map[string]struct{} {
	set := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		set[label] = struct{}{}
	}
	return set
}

func capabilityAllows(capabilities []string, kind string) bool {
	if len(capabilities) == 0 {
		return true
	}
	for _, capability := range capabilities {
		if capability == kind {
			return true
		}
	}
	return false
}
