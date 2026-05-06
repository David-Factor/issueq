// Package poller synchronizes GitHub issue snapshots into the local store.
package poller

import (
	"context"
	"fmt"
	"time"

	"issueq/internal/config"
	issuegithub "issueq/internal/github"
	"issueq/internal/handoff"
	"issueq/internal/store"
)

type Result struct {
	IssuesFetched    int
	IssuesUpserted   int
	HandoffsFound    int
	HandoffsInserted int
}

func Poll(ctx context.Context, cfg config.Config, client issuegithub.Client, queue store.QueueStore) (Result, error) {
	issues, err := client.ListOpenIssues(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo)
	if err != nil {
		return Result{}, fmt.Errorf("list GitHub issues: %w", err)
	}
	var result Result
	result.IssuesFetched = len(issues)
	syncedAt := time.Now().UTC()
	for _, issue := range issues {
		if issue.Host == "" {
			issue.Host = cfg.GitHub.Host
		}
		if issue.Owner == "" {
			issue.Owner = cfg.GitHub.Owner
		}
		if issue.Repo == "" {
			issue.Repo = cfg.GitHub.Repo
		}
		if issue.IssueKey == "" {
			issue.IssueKey = issueKey(issue.Host, issue.Owner, issue.Repo, issue.Number)
		}
		issue.SyncedAt = syncedAt
		if err := queue.UpsertIssue(ctx, issue); err != nil {
			return Result{}, fmt.Errorf("upsert issue %s: %w", issue.IssueKey, err)
		}
		result.IssuesUpserted++
		comments, err := client.ListIssueComments(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo, issue.Number)
		if err != nil {
			return Result{}, fmt.Errorf("list issue comments for %s: %w", issue.IssueKey, err)
		}
		for _, comment := range comments {
			parsed := handoff.ParseComment(issue.IssueKey, comment.Body, comment.CreatedAt)
			result.HandoffsFound += len(parsed.Handoffs)
			for _, h := range parsed.Handoffs {
				inserted, err := queue.UpsertHandoff(ctx, h)
				if err != nil {
					return Result{}, fmt.Errorf("upsert handoff %s for issue %s: %w", h.ID, issue.IssueKey, err)
				}
				if inserted {
					result.HandoffsInserted++
				}
			}
		}
	}
	return result, nil
}

func issueKey(host, owner, repo string, number int) string {
	return fmt.Sprintf("%s/%s/%s#%d", host, owner, repo, number)
}
