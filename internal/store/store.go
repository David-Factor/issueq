// Package store defines queue and bookkeeping storage interfaces.
package store

import (
	"context"
	"errors"
	"time"

	"issueq/internal/model"
)

var (
	ErrNotOwner  = errors.New("job is not owned by runner instance")
	ErrLostLease = errors.New("job lease is no longer valid")
)

type QueueStore interface {
	UpsertIssue(ctx context.Context, issue model.IssueSnapshot) error
	GetIssue(ctx context.Context, issueKey string) (model.IssueSnapshot, error)
	ListRoutableIssues(ctx context.Context) ([]model.IssueSnapshot, error)
	EnqueueJob(ctx context.Context, job model.JobCreate) (model.Job, bool, error)
	ClaimNextJob(ctx context.Context, identity model.RunnerIdentity, allowedKinds []string, maxGlobal int, perRouteLimit map[string]int, leaseDuration time.Duration) (*model.Job, error)
	ClaimNextJobInFrontier(ctx context.Context, identity model.RunnerIdentity, allowedKinds []string, maxGlobal int, perRouteLimit map[string]int, leaseDuration time.Duration, frontierJobIDs []string) (*model.Job, error)
	ReleaseExpiredLeases(ctx context.Context, now time.Time, staleHeartbeatBefore time.Time, currentRunnerInstanceID string, activeJobIDs []string) (int, error)
	FinalizeJobOwned(ctx context.Context, jobID string, runnerInstanceID string, result model.JobFinalize) error
	PersistLaunchSpecOwned(ctx context.Context, jobID, runnerInstanceID string, record model.LaunchSpecRecord) error
	MarkJobLaunchingOwned(ctx context.Context, jobID, runnerInstanceID, launchToken string) error
	PersistLaunchRecordOwned(ctx context.Context, jobID, runnerInstanceID string, record model.LaunchRecord) error
	ListOwnedRunningJobs(ctx context.Context, runnerInstanceID string) ([]model.Job, error)
	CountRunningJobs(ctx context.Context) (int, error)
	CountRunningJobsByRoute(ctx context.Context, routeName string) (int, error)
	ListStaleDurableRunningJobs(ctx context.Context, now, staleHeartbeatBefore time.Time) ([]model.Job, error)
	AdoptStaleRunningJob(ctx context.Context, jobID, oldRunnerInstanceID string, newIdentity model.RunnerIdentity, leaseDuration time.Duration, now, staleHeartbeatBefore time.Time) (*model.Job, error)
	MarkStaleRunningJobUnknown(ctx context.Context, jobID, oldRunnerInstanceID string, now, staleHeartbeatBefore time.Time) error
	IncrementAttemptsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string, generation int, routeName string) (int, error)
	IncrementTransitionsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string) (int, error)
	GetIssueState(ctx context.Context, issueKey string) (generation int, transitions int, err error)
	HeartbeatRunner(ctx context.Context, identity model.RunnerIdentity, pid int, now time.Time) error
	DeleteRunnerHeartbeat(ctx context.Context, runnerInstanceID string) error
	PruneStaleRunnerHeartbeats(ctx context.Context, before time.Time) (int, error)
	AssertJobOwned(ctx context.Context, jobID, runnerInstanceID string) error
	RenewJobLease(ctx context.Context, jobID, runnerInstanceID string, leaseDuration time.Duration) error
	ListEligibleJobIDs(ctx context.Context, now time.Time) ([]string, error)
	ListJobs(ctx context.Context) ([]model.Job, error)
	ListIssues(ctx context.Context) ([]model.IssueSnapshot, error)
	InsertJobEvent(ctx context.Context, event model.JobEvent) (model.JobEvent, error)
	ListJobEvents(ctx context.Context, jobID string) ([]model.JobEvent, error)
	Close() error
}
