// Package workflow contains queue/GitHub lifecycle primitives shared by entrypoints.
package workflow

import (
	"context"
	"errors"
	"time"

	"issueq/internal/actions"
	"issueq/internal/config"
	issuegithub "issueq/internal/github"
	"issueq/internal/model"
	"issueq/internal/router"
	"issueq/internal/store"
	"issueq/internal/supervisor"
)

type Result struct {
	Claimed   int
	Succeeded int
	Failed    int
	Skipped   int
	Dead      int
}

type ObservationDecision string

const (
	DecisionKeepRunning ObservationDecision = "keep_running"
	DecisionSucceeded   ObservationDecision = "succeeded"
	DecisionFailed      ObservationDecision = "failed"
	DecisionCancelled   ObservationDecision = "cancelled"
	DecisionUnknown     ObservationDecision = "unknown"
)

func HeartbeatRunner(ctx context.Context, queue store.QueueStore, identity model.RunnerIdentity, pid int, now time.Time) error {
	return queue.HeartbeatRunner(ctx, identity, pid, now.UTC())
}

func RecoverExpiredLeases(ctx context.Context, queue store.QueueStore, now time.Time, lease time.Duration, currentRunnerInstanceID string, activeJobIDs []string) (int, error) {
	return queue.ReleaseExpiredLeases(ctx, now.UTC(), now.UTC().Add(-lease), currentRunnerInstanceID, activeJobIDs)
}

func PruneStaleHeartbeats(ctx context.Context, queue store.QueueStore, before time.Time) (int, error) {
	return queue.PruneStaleRunnerHeartbeats(ctx, before.UTC())
}

func ClaimOne(ctx context.Context, cfg config.Config, queue store.QueueStore, identity model.RunnerIdentity, lease time.Duration, perRouteLimit map[string]int) (*model.Job, error) {
	return queue.ClaimNextJob(ctx, identity, cfg.Runner.Capabilities, maxGlobalConcurrency(cfg), perRouteLimit, lease)
}

func ClaimOneInFrontier(ctx context.Context, cfg config.Config, queue store.QueueStore, identity model.RunnerIdentity, lease time.Duration, perRouteLimit map[string]int, frontierJobIDs []string) (*model.Job, error) {
	return queue.ClaimNextJobInFrontier(ctx, identity, cfg.Runner.Capabilities, maxGlobalConcurrency(cfg), perRouteLimit, lease, frontierJobIDs)
}

func maxGlobalConcurrency(cfg config.Config) int {
	if cfg.Queue.MaxGlobalConcurrency <= 0 {
		return 1
	}
	return cfg.Queue.MaxGlobalConcurrency
}

func ObservationToDecision(obs supervisor.Observation) ObservationDecision {
	switch obs.State {
	case supervisor.RunStarting, supervisor.RunRunning:
		return DecisionKeepRunning
	case supervisor.RunExited:
		if obs.HasExitCode && obs.ExitCode == 0 {
			return DecisionSucceeded
		}
		return DecisionFailed
	case supervisor.RunFailed, supervisor.RunTimedOut:
		return DecisionFailed
	case supervisor.RunCancelled:
		return DecisionCancelled
	case supervisor.RunUnknown:
		return DecisionUnknown
	default:
		return DecisionUnknown
	}
}

func StatusForObservation(obs supervisor.Observation) (string, bool) {
	switch ObservationToDecision(obs) {
	case DecisionSucceeded:
		return model.JobStatusSucceeded, true
	case DecisionFailed:
		return model.JobStatusFailed, true
	case DecisionCancelled:
		return model.JobStatusCancelled, true
	default:
		return "", false
	}
}

func IsOwnershipLoss(err error) bool {
	return errors.Is(err, store.ErrLostLease) || errors.Is(err, store.ErrNotOwner)
}

func DropOnOwnershipLoss(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if IsOwnershipLoss(err) {
		return true, nil
	}
	return false, err
}

func ApplyOwned(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, identity model.RunnerIdentity, job *model.Job, lease time.Duration, issue model.IssueSnapshot, action config.ActionConfig) (actions.ApplyResult, error) {
	return actions.ApplyWithHooks(ctx, cfg, gh, queue, issue, action, actions.ApplyHooks{BeforeSideEffect: func() error {
		return queue.RenewJobLease(ctx, job.ID, identity.InstanceID, lease)
	}})
}

func RouteByName(cfg config.Config, name string) (config.RouteConfig, bool) {
	for _, route := range cfg.Routes {
		if route.Name == name {
			return route, true
		}
	}
	return config.RouteConfig{}, false
}

func RouteStillMatches(cfg config.Config, route config.RouteConfig, issue model.IssueSnapshot) bool {
	return router.Matches(cfg, route, issue)
}
