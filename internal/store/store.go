// Package store defines queue and bookkeeping storage interfaces.
package store

import (
	"context"
	"time"

	"issueq/internal/model"
)

type QueueStore interface {
	UpsertIssue(ctx context.Context, issue model.IssueSnapshot) error
	GetIssue(ctx context.Context, issueKey string) (model.IssueSnapshot, error)
	ListRoutableIssues(ctx context.Context) ([]model.IssueSnapshot, error)
	EnqueueJob(ctx context.Context, job model.JobCreate) (model.Job, bool, error)
	ClaimNextJob(ctx context.Context, runnerID string, allowedKinds []string, maxGlobal int, perRouteLimit map[string]int, leaseDuration time.Duration) (*model.Job, error)
	ReleaseExpiredLeases(ctx context.Context, now time.Time) (int, error)
	FinalizeJob(ctx context.Context, jobID string, result model.JobFinalize) error
	UpdateJobArtifacts(ctx context.Context, jobID, contextPath, resultPath, stdoutPath, stderrPath string, pid int) error
	UpdateJobAttempts(ctx context.Context, jobID string, attempts int) error
	IncrementAttempts(ctx context.Context, issueKey string, generation int, routeName string) (int, error)
	GetIssueState(ctx context.Context, issueKey string) (generation int, transitions int, err error)
	IncrementTransitions(ctx context.Context, issueKey string) (int, error)
	ListJobs(ctx context.Context) ([]model.Job, error)
	ListIssues(ctx context.Context) ([]model.IssueSnapshot, error)
	InsertJobEvent(ctx context.Context, event model.JobEvent) (model.JobEvent, error)
	ListJobEvents(ctx context.Context, jobID string) ([]model.JobEvent, error)
	Close() error
}
