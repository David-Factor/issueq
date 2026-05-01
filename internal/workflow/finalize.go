package workflow

import (
	"context"
	"fmt"
	"time"

	"issueq/internal/model"
	"issueq/internal/supervisor"
)

type ObservationFinalizer interface {
	FinalizeJobOwned(ctx context.Context, jobID string, runnerInstanceID string, result model.JobFinalize) error
}

type ObservationFinalization struct {
	JobID         string
	Decision      ObservationDecision
	Status        string
	Finalized     bool
	OwnershipLost bool
}

type ObservationFinalizationSummary struct {
	Observed      int
	Finalized     int
	Succeeded     int
	Failed        int
	Cancelled     int
	KeptRunning   int
	Unknown       int
	OwnershipLost int
}

func LastErrorForObservation(obs supervisor.Observation) string {
	if obs.Error != "" {
		return obs.Error
	}
	switch obs.State {
	case supervisor.RunExited:
		if obs.HasExitCode && obs.ExitCode != 0 {
			return fmt.Sprintf("exit code %d", obs.ExitCode)
		}
		if !obs.HasExitCode {
			return "process exited without exit code"
		}
	case supervisor.RunFailed:
		return "run failed"
	case supervisor.RunTimedOut:
		return "run timed out"
	case supervisor.RunCancelled:
		return "run cancelled"
	}
	return ""
}

func FinalizeOwnedObservation(ctx context.Context, queue ObservationFinalizer, identity model.RunnerIdentity, job model.Job, obs supervisor.Observation, now time.Time) (ObservationFinalization, error) {
	result := ObservationFinalization{JobID: job.ID, Decision: ObservationToDecision(obs)}
	if _, ok, _ := DurableLaunchRecord(job); !ok {
		result.Decision = DecisionUnknown
		return result, nil
	}
	status, terminal := StatusForObservation(obs)
	if !terminal {
		return result, nil
	}
	finishedAt := obs.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = now.UTC()
	}
	finalize := model.JobFinalize{
		Status:     status,
		LastError:  LastErrorForObservation(obs),
		ResultPath: obs.ResultPath,
		StdoutPath: obs.StdoutPath,
		StderrPath: obs.StderrPath,
		FinishedAt: finishedAt,
	}
	dropped, err := DropOnOwnershipLoss(queue.FinalizeJobOwned(ctx, job.ID, identity.InstanceID, finalize))
	if err != nil {
		return result, err
	}
	if dropped {
		result.OwnershipLost = true
		return result, nil
	}
	result.Status = status
	result.Finalized = true
	return result, nil
}

func FinalizeOwnedObservations(ctx context.Context, queue ObservationFinalizer, identity model.RunnerIdentity, items []JobObservation, now time.Time) (ObservationFinalizationSummary, error) {
	var summary ObservationFinalizationSummary
	for _, item := range items {
		summary.Observed++
		finalized, err := FinalizeOwnedObservation(ctx, queue, identity, item.Job, item.Observation, now)
		if err != nil {
			return summary, fmt.Errorf("finalize durable job %s: %w", item.Job.ID, err)
		}
		if finalized.OwnershipLost {
			summary.OwnershipLost++
			continue
		}
		if finalized.Finalized {
			summary.Finalized++
			switch finalized.Status {
			case model.JobStatusSucceeded:
				summary.Succeeded++
			case model.JobStatusCancelled:
				summary.Cancelled++
			default:
				summary.Failed++
			}
			continue
		}
		switch finalized.Decision {
		case DecisionUnknown:
			summary.Unknown++
		default:
			summary.KeptRunning++
		}
	}
	return summary, nil
}
