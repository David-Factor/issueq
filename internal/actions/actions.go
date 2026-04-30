// Package actions applies configured and subprocess-returned GitHub actions.
package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"issueq/internal/config"
	issuegithub "issueq/internal/github"
	"issueq/internal/model"
	"issueq/internal/store"
)

type ResultAction struct {
	Comment      string   `json:"comment"`
	LabelsAdd    []string `json:"labels_add"`
	LabelsRemove []string `json:"labels_remove"`
}

type ResultActionEnvelope struct {
	Comment      string           `json:"comment"`
	LabelsAdd    []string         `json:"labels_add"`
	LabelsRemove []string         `json:"labels_remove"`
	Unsupported  *json.RawMessage `json:"enqueue,omitempty"`
}

func ParseResultFile(path string) (ResultAction, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return ResultAction{}, false, nil
	}
	if err != nil {
		return ResultAction{}, false, fmt.Errorf("read result JSON: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return ResultAction{}, false, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return ResultAction{}, true, fmt.Errorf("parse result JSON: %w", err)
	}
	for key := range raw {
		switch key {
		case "comment", "labels_add", "labels_remove":
		default:
			return ResultAction{}, true, fmt.Errorf("unsupported result JSON field %q", key)
		}
	}
	var result ResultAction
	if err := json.Unmarshal(data, &result); err != nil {
		return ResultAction{}, true, fmt.Errorf("parse result JSON: %w", err)
	}
	return result, true, nil
}

func Merge(base config.ActionConfig, result ResultAction) config.ActionConfig {
	merged := config.ActionConfig{
		LabelsAdd:    append([]string{}, base.LabelsAdd...),
		LabelsRemove: append([]string{}, base.LabelsRemove...),
		Comment:      base.Comment,
	}
	if result.Comment != "" {
		if merged.Comment != "" {
			merged.Comment += "\n\n" + result.Comment
		} else {
			merged.Comment = result.Comment
		}
	}
	add := orderedSet(merged.LabelsAdd)
	remove := orderedSet(merged.LabelsRemove)
	for _, label := range result.LabelsAdd {
		delete(remove.seen, label)
		remove.values = removeWithout(remove.values, label)
		add.add(label)
	}
	for _, label := range result.LabelsRemove {
		delete(add.seen, label)
		add.values = removeWithout(add.values, label)
		remove.add(label)
	}
	merged.LabelsAdd = add.values
	merged.LabelsRemove = remove.values
	return merged
}

type ApplyResult struct {
	UpdatedIssue model.IssueSnapshot
	Changed      bool
}

type ApplyHooks struct {
	BeforeSideEffect func() error
}

func (h ApplyHooks) beforeSideEffect() error {
	if h.BeforeSideEffect == nil {
		return nil
	}
	return h.BeforeSideEffect()
}

func Apply(ctx context.Context, cfg config.Config, client issuegithub.Client, queue store.QueueStore, issue model.IssueSnapshot, action config.ActionConfig) (ApplyResult, error) {
	return ApplyWithHooks(ctx, cfg, client, queue, issue, action, ApplyHooks{})
}

func ApplyWithHooks(ctx context.Context, cfg config.Config, client issuegithub.Client, queue store.QueueStore, issue model.IssueSnapshot, action config.ActionConfig, hooks ApplyHooks) (ApplyResult, error) {
	if err := hooks.beforeSideEffect(); err != nil {
		return ApplyResult{}, err
	}
	latest, err := client.GetIssue(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo, issue.Number)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("refresh issue before action: %w", err)
	}
	beforeLabels := append([]string{}, latest.Labels...)
	mutated := false
	if len(action.LabelsRemove) > 0 {
		if err := hooks.beforeSideEffect(); err != nil {
			return ApplyResult{}, err
		}
		if err := client.RemoveLabels(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo, issue.Number, action.LabelsRemove); err != nil && !isAbsentLabelError(err) {
			return ApplyResult{}, fmt.Errorf("remove labels: %w", err)
		}
		mutated = true
	}
	if len(action.LabelsAdd) > 0 {
		if err := hooks.beforeSideEffect(); err != nil {
			return ApplyResult{}, err
		}
		if err := client.AddLabels(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo, issue.Number, action.LabelsAdd); err != nil {
			return ApplyResult{}, fmt.Errorf("add labels: %w", err)
		}
		mutated = true
	}
	if strings.TrimSpace(action.Comment) != "" {
		if err := hooks.beforeSideEffect(); err != nil {
			return ApplyResult{}, err
		}
		if err := client.CreateComment(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo, issue.Number, action.Comment); err != nil {
			return ApplyResult{}, fmt.Errorf("create comment: %w", err)
		}
		mutated = true
	}
	if mutated {
		if err := hooks.beforeSideEffect(); err != nil {
			return ApplyResult{}, err
		}
		latest, err = client.GetIssue(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo, issue.Number)
		if err != nil {
			return ApplyResult{}, fmt.Errorf("refresh issue after action: %w", err)
		}
	}
	latest.IssueKey = model.IssueKey(latest.Host, latest.Owner, latest.Repo, latest.Number)
	if err := hooks.beforeSideEffect(); err != nil {
		return ApplyResult{}, err
	}
	if err := queue.UpsertIssue(ctx, latest); err != nil {
		return ApplyResult{}, fmt.Errorf("update local issue snapshot: %w", err)
	}
	return ApplyResult{UpdatedIssue: latest, Changed: !sameLabelSet(beforeLabels, latest.Labels)}, nil
}

func sameLabelSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, label := range a {
		seen[label]++
	}
	for _, label := range b {
		seen[label]--
		if seen[label] < 0 {
			return false
		}
	}
	return true
}

func isAbsentLabelError(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "not found") || strings.Contains(s, "404")
}

type labelSet struct {
	seen   map[string]struct{}
	values []string
}

func orderedSet(values []string) labelSet {
	set := labelSet{seen: map[string]struct{}{}, values: []string{}}
	for _, value := range values {
		set.add(value)
	}
	return set
}

func (s *labelSet) add(value string) {
	if _, ok := s.seen[value]; ok {
		return
	}
	s.seen[value] = struct{}{}
	s.values = append(s.values, value)
}

func removeWithout(values []string, value string) []string {
	out := values[:0]
	for _, existing := range values {
		if existing != value {
			out = append(out, existing)
		}
	}
	return out
}
