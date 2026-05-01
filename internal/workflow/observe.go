package workflow

import (
	"context"
	"fmt"

	"issueq/internal/model"
	"issueq/internal/supervisor"
)

type OwnedRunningJobLister interface {
	ListOwnedRunningJobs(ctx context.Context, runnerInstanceID string) ([]model.Job, error)
}

type JobObservation struct {
	Job         model.Job
	Observation supervisor.Observation
}

func DurableLaunchRecord(job model.Job) (supervisor.LaunchRecord, bool, string) {
	if job.Status != model.JobStatusRunning {
		return supervisor.LaunchRecord{}, false, "job is not running"
	}
	if job.SupervisorKind == "" {
		return supervisor.LaunchRecord{}, false, "missing supervisor kind"
	}
	if job.SupervisorKind != supervisor.KindWrapper {
		return supervisor.LaunchRecord{}, false, fmt.Sprintf("unsupported supervisor kind %q", job.SupervisorKind)
	}
	if job.LaunchToken == "" {
		return supervisor.LaunchRecord{}, false, "missing launch token"
	}
	switch job.LaunchState {
	case model.LaunchStateLaunching, model.LaunchStateRunning:
	case model.LaunchStatePreparing:
		return supervisor.LaunchRecord{}, false, "launch is still preparing"
	case model.LaunchStateUnknown:
		return supervisor.LaunchRecord{}, false, "launch state is unknown"
	case "":
		return supervisor.LaunchRecord{}, false, "missing launch state"
	default:
		return supervisor.LaunchRecord{}, false, fmt.Sprintf("unsupported launch state %q", job.LaunchState)
	}
	if job.RunMetadataPath == "" && job.SupervisorID == "" && job.PID <= 0 {
		return supervisor.LaunchRecord{}, false, "missing durable launch evidence"
	}

	record := supervisor.LaunchRecord{
		Kind:         job.SupervisorKind,
		ID:           job.SupervisorID,
		JobID:        job.ID,
		LaunchToken:  job.LaunchToken,
		PID:          job.PID,
		PGID:         job.PGID,
		MetadataPath: job.RunMetadataPath,
	}
	if job.ProcessStartedAt != nil {
		record.ProcessStartedAt = *job.ProcessStartedAt
	}
	if job.StartedAt != nil {
		record.StartedAt = *job.StartedAt
	}
	if job.TimeoutAt != nil {
		record.TimeoutAt = *job.TimeoutAt
	}
	return record, true, ""
}

func InspectDurableJob(ctx context.Context, backend supervisor.Supervisor, job model.Job) (supervisor.Observation, error) {
	record, ok, reason := DurableLaunchRecord(job)
	if !ok {
		return supervisor.Observation{State: supervisor.RunUnknown, Error: reason}, nil
	}
	return backend.Inspect(ctx, record)
}

func ObserveOwnedRunningJobs(ctx context.Context, lister OwnedRunningJobLister, backend supervisor.Supervisor, identity model.RunnerIdentity) ([]JobObservation, error) {
	jobs, err := lister.ListOwnedRunningJobs(ctx, identity.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("list owned running jobs: %w", err)
	}
	observations := make([]JobObservation, 0, len(jobs))
	for _, job := range jobs {
		obs, err := InspectDurableJob(ctx, backend, job)
		if err != nil {
			return nil, fmt.Errorf("inspect durable job %s: %w", job.ID, err)
		}
		observations = append(observations, JobObservation{Job: job, Observation: obs})
	}
	return observations, nil
}
