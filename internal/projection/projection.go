package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	issuegithub "issueq/internal/github"
	"issueq/internal/model"
)

const MarkerPrefix = "<!-- issueq:projection:"

var allowedLabels = map[string]struct{}{
	"agent-active":      {},
	"agent-merge-ready": {},
	"agent-needs-human": {},
	"agent-stale":       {},
	"agent-failed":      {},
}

type Projection struct {
	Comment string   `json:"comment"`
	Labels  []string `json:"labels"`
}

type Result struct {
	TargetNumber int      `json:"target_number"`
	CommentID    string   `json:"comment_id,omitempty"`
	Created      bool     `json:"created"`
	Updated      bool     `json:"updated"`
	Labels       []string `json:"labels,omitempty"`
}

func ProjectEvent(ctx context.Context, gh issuegithub.Client, ev model.AutomationEvent) (Result, error) {
	if gh == nil {
		return Result{}, fmt.Errorf("github client is required")
	}
	number, err := targetNumber(ev)
	if err != nil {
		return Result{}, err
	}
	proj := projectionFromEvent(ev)
	body := managedBody(ev.EventKey, proj.Comment)
	comments, err := gh.ListIssueComments(ctx, ev.Owner, ev.Repo, number)
	if err != nil {
		return Result{}, err
	}
	marker := markerFor(ev.EventKey)
	for _, comment := range comments {
		if strings.Contains(comment.Body, marker) {
			if err := gh.UpdateComment(ctx, ev.Owner, ev.Repo, comment.ID, body); err != nil {
				return Result{}, err
			}
			if err := applyLabels(ctx, gh, ev, number, proj.Labels); err != nil {
				return Result{}, err
			}
			return Result{TargetNumber: number, CommentID: comment.ID, Updated: true, Labels: proj.Labels}, nil
		}
	}
	if err := gh.CreateComment(ctx, ev.Owner, ev.Repo, number, body); err != nil {
		return Result{}, err
	}
	if err := applyLabels(ctx, gh, ev, number, proj.Labels); err != nil {
		return Result{}, err
	}
	return Result{TargetNumber: number, Created: true, Labels: proj.Labels}, nil
}

func projectionFromEvent(ev model.AutomationEvent) Projection {
	var raw struct {
		Projection Projection `json:"projection"`
		Summary    string     `json:"summary_markdown"`
		Decision   string     `json:"decision"`
	}
	_ = json.Unmarshal([]byte(ev.ResultJSON), &raw)
	p := raw.Projection
	if strings.TrimSpace(p.Comment) == "" {
		p.Comment = raw.Summary
	}
	if strings.TrimSpace(p.Comment) == "" {
		p.Comment = fmt.Sprintf("IssueQ event `%s` is `%s`.", ev.EventKey, ev.Status)
	}
	if len(p.Labels) == 0 {
		switch ev.Status {
		case model.AutomationEventStatusRunning:
			p.Labels = []string{"agent-active"}
		case model.AutomationEventStatusBlocked, model.AutomationEventStatusNeedsHuman:
			p.Labels = []string{"agent-needs-human"}
		case model.AutomationEventStatusStale:
			p.Labels = []string{"agent-stale"}
		case model.AutomationEventStatusFailed:
			p.Labels = []string{"agent-failed"}
		case model.AutomationEventStatusSucceeded:
			if raw.Decision == "merge_ready" {
				p.Labels = []string{"agent-merge-ready"}
			}
		}
	}
	p.Labels = filterAllowedLabels(p.Labels)
	return p
}

func managedBody(eventKey, comment string) string {
	return fmt.Sprintf("%s -->\n%s", markerFor(eventKey), strings.TrimSpace(comment))
}

func markerFor(eventKey string) string {
	return MarkerPrefix + eventKey
}

func targetNumber(ev model.AutomationEvent) (int, error) {
	parts := strings.Split(ev.TargetKey, "-")
	if len(parts) < 2 {
		return 0, fmt.Errorf("target_key %q does not contain a numeric suffix", ev.TargetKey)
	}
	n, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("target_key %q does not contain a positive numeric suffix", ev.TargetKey)
	}
	return n, nil
}

func filterAllowedLabels(labels []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if _, ok := allowedLabels[label]; !ok {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	sort.Strings(out)
	return out
}

func applyLabels(ctx context.Context, gh issuegithub.Client, ev model.AutomationEvent, number int, labels []string) error {
	latest, err := gh.GetIssue(ctx, ev.Owner, ev.Repo, number)
	if err != nil {
		return err
	}
	desired := map[string]struct{}{}
	for _, label := range labels {
		desired[label] = struct{}{}
	}
	seen := map[string]struct{}{}
	var next []string
	for _, label := range latest.Labels {
		if _, uiOnly := allowedLabels[label]; uiOnly {
			if _, keep := desired[label]; !keep {
				continue
			}
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		next = append(next, label)
	}
	for _, label := range labels {
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		next = append(next, label)
	}
	return gh.SetLabels(ctx, ev.Owner, ev.Repo, number, next)
}
