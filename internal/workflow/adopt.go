package workflow

import (
	"context"
	"fmt"
	"time"

	"issueq/internal/model"
	"issueq/internal/supervisor"
)

type StaleDurableStore interface {
	ListStaleDurableRunningJobs(ctx context.Context, now, staleHeartbeatBefore time.Time) ([]model.Job, error)
	AdoptStaleRunningJob(ctx context.Context, jobID, oldRunnerInstanceID string, newIdentity model.RunnerIdentity, leaseDuration time.Duration, now, staleHeartbeatBefore time.Time) (*model.Job, error)
	MarkStaleRunningJobUnknown(ctx context.Context, jobID, oldRunnerInstanceID string, now, staleHeartbeatBefore time.Time) error
}

type StaleDurableRecovery struct {
	Job           model.Job
	Observation   supervisor.Observation
	Adopted       bool
	MarkedUnknown bool
	OwnershipLost bool
}

type StaleDurableRecoverySummary struct {
	Scanned       int
	Adopted       int
	MarkedUnknown int
	OwnershipLost int
}

func RecoverStaleDurableRunningJobs(ctx context.Context, queue StaleDurableStore, backend supervisor.Supervisor, identity model.RunnerIdentity, lease time.Duration, now time.Time) ([]StaleDurableRecovery, StaleDurableRecoverySummary, error) {
	var summary StaleDurableRecoverySummary
	staleBefore := now.UTC().Add(-lease)
	jobs, err := queue.ListStaleDurableRunningJobs(ctx, now.UTC(), staleBefore)
	if err != nil {
		return nil, summary, err
	}
	summary.Scanned = len(jobs)
	recovered := make([]StaleDurableRecovery, 0, len(jobs))
	for _, job := range jobs {
		item, err := recoverOneStaleDurable(ctx, queue, backend, identity, lease, now.UTC(), staleBefore, job)
		if err != nil {
			return recovered, summary, err
		}
		switch {
		case item.Adopted:
			summary.Adopted++
			recovered = append(recovered, item)
		case item.MarkedUnknown:
			summary.MarkedUnknown++
		case item.OwnershipLost:
			summary.OwnershipLost++
		}
	}
	return recovered, summary, nil
}

func recoverOneStaleDurable(ctx context.Context, queue StaleDurableStore, backend supervisor.Supervisor, identity model.RunnerIdentity, lease time.Duration, now, staleBefore time.Time, job model.Job) (StaleDurableRecovery, error) {
	item := StaleDurableRecovery{Job: job}
	record, ok, _ := DurableLaunchRecord(job)
	if !ok {
		marked, err := markStaleUnknown(ctx, queue, job, now, staleBefore)
		item.MarkedUnknown = marked
		item.OwnershipLost = !marked && err == nil
		return item, err
	}
	obs, err := backend.Inspect(ctx, record)
	if err != nil {
		return item, fmt.Errorf("inspect stale durable job %s: %w", job.ID, err)
	}
	item.Observation = obs
	if ObservationToDecision(obs) == DecisionUnknown {
		marked, err := markStaleUnknown(ctx, queue, job, now, staleBefore)
		item.MarkedUnknown = marked
		item.OwnershipLost = !marked && err == nil
		return item, err
	}
	adopted, err := queue.AdoptStaleRunningJob(ctx, job.ID, job.RunnerInstanceID, identity, lease, now, staleBefore)
	if err != nil {
		if IsOwnershipLoss(err) {
			item.OwnershipLost = true
			return item, nil
		}
		return item, err
	}
	obs, err = backend.Inspect(ctx, record)
	if err != nil {
		return item, fmt.Errorf("inspect adopted durable job %s: %w", job.ID, err)
	}
	item.Observation = obs
	item.Job = *adopted
	item.Adopted = true
	return item, nil
}

func markStaleUnknown(ctx context.Context, queue StaleDurableStore, job model.Job, now, staleBefore time.Time) (bool, error) {
	err := queue.MarkStaleRunningJobUnknown(ctx, job.ID, job.RunnerInstanceID, now, staleBefore)
	if err != nil {
		if IsOwnershipLoss(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
