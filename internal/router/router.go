// Package router evaluates configured routes and enqueues matching jobs.
package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/store"
)

type Result struct {
	IssuesEvaluated int
	RoutesMatched   int
	JobsCreated     int
	JobsExisting    int
}

func Route(ctx context.Context, cfg config.Config, queue store.QueueStore) (Result, error) {
	issues, err := queue.ListRoutableIssues(ctx)
	if err != nil {
		return Result{}, err
	}

	var result Result
	result.IssuesEvaluated = len(issues)
	for _, issue := range issues {
		for _, route := range cfg.Routes {
			if !Matches(cfg, route, issue) {
				continue
			}
			result.RoutesMatched++
			job, inserted, err := queue.EnqueueJob(ctx, JobCreate(route, issue))
			if err != nil {
				return Result{}, fmt.Errorf("enqueue route %q for issue %s: %w", route.Name, issue.IssueKey, err)
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
