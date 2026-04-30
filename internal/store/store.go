// Package store defines queue and bookkeeping storage interfaces.
package store

import (
	"context"

	"issueq/internal/model"
)

type QueueStore interface {
	UpsertIssue(ctx context.Context, issue model.IssueSnapshot) error
	ListRoutableIssues(ctx context.Context) ([]model.IssueSnapshot, error)
	EnqueueJob(ctx context.Context, job model.JobCreate) (model.Job, bool, error)
	ListJobs(ctx context.Context) ([]model.Job, error)
	ListIssues(ctx context.Context) ([]model.IssueSnapshot, error)
	InsertJobEvent(ctx context.Context, event model.JobEvent) (model.JobEvent, error)
	ListJobEvents(ctx context.Context, jobID string) ([]model.JobEvent, error)
	Close() error
}
