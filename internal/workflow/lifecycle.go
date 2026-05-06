package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"issueq/internal/actions"
	"issueq/internal/config"
	issuegithub "issueq/internal/github"
	"issueq/internal/model"
	"issueq/internal/router"
	"issueq/internal/store"
	"issueq/internal/supervisor"
)

type PrepareOutcome string

const (
	PrepareLaunch  PrepareOutcome = "launch"
	PrepareDone    PrepareOutcome = "done"
	PrepareDropped PrepareOutcome = "dropped"
)

type PrepareClaimedWrapperInput struct {
	Config   config.Config
	Queue    store.QueueStore
	GitHub   issuegithub.Client
	Identity model.RunnerIdentity
	Job      *model.Job
	Route    config.RouteConfig
	Lease    time.Duration
}

type PrepareClaimedWrapperResult struct {
	Issue         model.IssueSnapshot
	Outcome       PrepareOutcome
	Status        string
	Finalized     bool
	OwnershipLost bool
}

func PrepareClaimedWrapperLaunch(ctx context.Context, in PrepareClaimedWrapperInput) (PrepareClaimedWrapperResult, error) {
	result := PrepareClaimedWrapperResult{Outcome: PrepareLaunch}
	issue, err := in.Queue.GetIssue(ctx, in.Job.IssueKey)
	if err != nil {
		return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("load issue: %v", err))
	}
	if in.GitHub == nil {
		result.Issue = issue
		return result, nil
	}
	if err := in.Queue.RenewJobLease(ctx, in.Job.ID, in.Identity.InstanceID, in.Lease); err != nil {
		return dropPrepared(result, err)
	}
	latest, err := in.GitHub.GetIssue(ctx, in.Config.GitHub.Owner, in.Config.GitHub.Repo, issue.Number)
	if err != nil {
		return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("refresh issue: %v", err))
	}
	if err := in.Queue.RenewJobLease(ctx, in.Job.ID, in.Identity.InstanceID, in.Lease); err != nil {
		return dropPrepared(result, err)
	}
	if err := in.Queue.UpsertIssue(ctx, latest); err != nil {
		return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("store refreshed issue: %v", err))
	}
	if !RouteStillMatches(in.Config, in.Route, latest) {
		return finalizePrepared(ctx, in, result, model.JobStatusSkipped, "stale route predicate")
	}
	issue = latest
	generation, _, err := in.Queue.GetIssueState(ctx, issue.IssueKey)
	if err != nil {
		return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("load issue state: %v", err))
	}
	scopeHash, err := router.AttemptScopeHash(ctx, in.Queue, in.Route, issue)
	if err != nil {
		if errors.Is(err, router.ErrAttemptScopeBlocked) {
			return finalizePrepared(ctx, in, result, model.JobStatusSkipped, err.Error())
		}
		return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("derive attempt scope: %v", err))
	}
	attempts, err := in.Queue.IncrementAttemptsForJob(ctx, in.Job.ID, in.Identity.InstanceID, issue.IssueKey, generation, in.Route.Name, scopeHash)
	if err != nil {
		if IsOwnershipLoss(err) {
			result.Outcome = PrepareDropped
			result.OwnershipLost = true
			return result, nil
		}
		return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("increment attempts: %v", err))
	}
	in.Job.Attempts = attempts
	if attempts > in.Route.Job.MaxAttempts {
		if err := in.Queue.RenewJobLease(ctx, in.Job.ID, in.Identity.InstanceID, in.Lease); err != nil {
			return dropPrepared(result, err)
		}
		if _, err := ApplyOwned(ctx, in.Config, in.Queue, in.GitHub, in.Identity, in.Job, in.Lease, issue, in.Route.Job.OnAttemptsExceeded); err != nil {
			if IsOwnershipLoss(err) {
				result.Outcome = PrepareDropped
				result.OwnershipLost = true
				return result, nil
			}
			return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("apply attempts-exceeded actions: %v", err))
		}
		return finalizePrepared(ctx, in, result, model.JobStatusDead, "max attempts exceeded")
	}
	if err := in.Queue.RenewJobLease(ctx, in.Job.ID, in.Identity.InstanceID, in.Lease); err != nil {
		return dropPrepared(result, err)
	}
	applied, err := ApplyOwned(ctx, in.Config, in.Queue, in.GitHub, in.Identity, in.Job, in.Lease, issue, in.Route.Job.OnStart)
	if err != nil {
		if IsOwnershipLoss(err) {
			result.Outcome = PrepareDropped
			result.OwnershipLost = true
			return result, nil
		}
		return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("apply on_start actions: %v", err))
	}
	issue = applied.UpdatedIssue
	if applied.Changed {
		dead, err := CheckTransitionLimitOwned(ctx, in.Config, in.Queue, in.GitHub, in.Identity, in.Job, in.Lease, issue)
		if err != nil {
			if IsOwnershipLoss(err) {
				result.Outcome = PrepareDropped
				result.OwnershipLost = true
				return result, nil
			}
			return finalizePrepared(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("check transition limit: %v", err))
		}
		if dead {
			return finalizePrepared(ctx, in, result, model.JobStatusDead, "max transitions exceeded")
		}
	}
	_, _ = in.Queue.InsertJobEvent(ctx, model.JobEvent{JobID: in.Job.ID, IssueKey: in.Job.IssueKey, EventType: "action_on_start"})
	result.Issue = issue
	return result, nil
}

func finalizePrepared(ctx context.Context, in PrepareClaimedWrapperInput, result PrepareClaimedWrapperResult, status, message string) (PrepareClaimedWrapperResult, error) {
	finalized, err := FinalizeClaimedJobOwned(ctx, in.Queue, *in.Job, in.Identity.InstanceID, status, message)
	if err != nil {
		return result, err
	}
	result.Outcome = PrepareDone
	result.Status = status
	result.Finalized = finalized
	result.OwnershipLost = !finalized
	return result, nil
}

func dropPrepared(result PrepareClaimedWrapperResult, err error) (PrepareClaimedWrapperResult, error) {
	if IsOwnershipLoss(err) {
		result.Outcome = PrepareDropped
		result.OwnershipLost = true
		return result, nil
	}
	return result, err
}

func FinalizeClaimedJobOwned(ctx context.Context, queue store.QueueStore, job model.Job, runnerInstanceID, status, message string) (bool, error) {
	dropped, err := DropOnOwnershipLoss(queue.FinalizeJobOwned(ctx, job.ID, runnerInstanceID, model.JobFinalize{Status: status, LastError: message, FinishedAt: time.Now().UTC()}))
	if err != nil || dropped {
		return false, err
	}
	_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "job_" + status, Message: message})
	return true, nil
}

type FinalizeObservationLifecycleInput struct {
	Config   config.Config
	Queue    store.QueueStore
	GitHub   issuegithub.Client
	Identity model.RunnerIdentity
	Job      model.Job
	Obs      supervisor.Observation
	Lease    time.Duration
	Now      time.Time
}

func FinalizeOwnedObservationWithLifecycle(ctx context.Context, in FinalizeObservationLifecycleInput) (ObservationFinalization, error) {
	status, terminal := StatusForObservation(in.Obs)
	result := ObservationFinalization{JobID: in.Job.ID, Decision: ObservationToDecision(in.Obs)}
	if !terminal {
		return result, nil
	}
	lastErr := LastErrorForObservation(in.Obs)
	var resultAction actions.ResultAction
	resultActionFound := false
	if in.Obs.ResultPath != "" {
		if in.GitHub == nil {
			if workStarted, _, parseErr := actions.ParseWorkStartedFile(in.Obs.ResultPath); parseErr == nil {
				resultAction.WorkStarted = workStarted
			}
		} else {
			parsed, found, parseErr := actions.ParseResultFile(in.Obs.ResultPath)
			if parseErr != nil {
				status = model.JobStatusFailed
				lastErr = parseErr.Error()
			} else if found {
				resultAction = parsed
				resultActionFound = true
			}
		}
	}
	if in.GitHub != nil && status != model.JobStatusCancelled {
		route, ok := RouteByName(in.Config, in.Job.RouteName)
		if !ok {
			return finalizeObservedTerminal(ctx, in, result, status, lastErr, resultAction.WorkStarted)
		}
		issue, err := in.Queue.GetIssue(ctx, in.Job.IssueKey)
		if err != nil {
			return finalizeObservedTerminal(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("load issue: %v", err), resultAction.WorkStarted)
		}
		finalAction := route.Job.OnSuccess
		if status != model.JobStatusSucceeded {
			finalAction = route.Job.OnFailure
		}
		if resultActionFound {
			finalAction = actions.Merge(finalAction, resultAction)
		}
		if err := in.Queue.RenewJobLease(ctx, in.Job.ID, in.Identity.InstanceID, in.Lease); err != nil {
			dropped, err := DropOnOwnershipLoss(err)
			if dropped {
				result.OwnershipLost = true
				return result, nil
			}
			return result, err
		}
		applied, err := ApplyOwned(ctx, in.Config, in.Queue, in.GitHub, in.Identity, &in.Job, in.Lease, issue, finalAction)
		if err != nil {
			if IsOwnershipLoss(err) {
				result.OwnershipLost = true
				return result, nil
			}
			status = model.JobStatusFailed
			lastErr = err.Error()
		} else if applied.Changed {
			dead, err := CheckTransitionLimitOwned(ctx, in.Config, in.Queue, in.GitHub, in.Identity, &in.Job, in.Lease, applied.UpdatedIssue)
			if err != nil {
				if IsOwnershipLoss(err) {
					result.OwnershipLost = true
					return result, nil
				}
				return finalizeObservedTerminal(ctx, in, result, model.JobStatusFailed, fmt.Sprintf("check transition limit: %v", err), resultAction.WorkStarted)
			}
			if dead {
				status = model.JobStatusDead
				lastErr = "max transitions exceeded"
			}
		}
	}
	return finalizeObservedTerminal(ctx, in, result, status, lastErr, resultAction.WorkStarted)
}

func finalizeObservedTerminal(ctx context.Context, in FinalizeObservationLifecycleInput, result ObservationFinalization, status, lastErr string, workStarted *bool) (ObservationFinalization, error) {
	finishedAt := in.Obs.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = in.Now.UTC()
	}
	dropped, err := DropOnOwnershipLoss(in.Queue.FinalizeJobOwned(ctx, in.Job.ID, in.Identity.InstanceID, model.JobFinalize{Status: status, LastError: lastErr, ResultPath: in.Obs.ResultPath, StdoutPath: in.Obs.StdoutPath, StderrPath: in.Obs.StderrPath, FinishedAt: finishedAt, WorkStarted: workStarted}))
	if err != nil {
		return result, err
	}
	if dropped {
		result.OwnershipLost = true
		return result, nil
	}
	_, _ = in.Queue.InsertJobEvent(ctx, model.JobEvent{JobID: in.Job.ID, IssueKey: in.Job.IssueKey, EventType: "job_" + status, Message: lastErr})
	result.Status = status
	result.Finalized = true
	return result, nil
}

func CheckTransitionLimitOwned(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, identity model.RunnerIdentity, job *model.Job, lease time.Duration, issue model.IssueSnapshot) (bool, error) {
	if err := queue.RenewJobLease(ctx, job.ID, identity.InstanceID, lease); err != nil {
		return false, err
	}
	count, err := queue.IncrementTransitionsForJob(ctx, job.ID, identity.InstanceID, issue.IssueKey)
	if err != nil {
		return false, err
	}
	limit := cfg.Workflow.MaxTransitionsPerIssue
	if limit == 0 {
		limit = 10
	}
	if limit >= 0 && count <= limit {
		return false, nil
	}
	if gh != nil {
		if err := queue.RenewJobLease(ctx, job.ID, identity.InstanceID, lease); err != nil {
			return false, err
		}
		if _, err := ApplyOwned(ctx, cfg, queue, gh, identity, job, lease, issue, cfg.Workflow.OnTransitionsExceeded); err != nil {
			if IsOwnershipLoss(err) {
				return false, err
			}
			_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: issue.IssueKey, EventType: "terminal_action_failed", Message: err.Error()})
			return false, fmt.Errorf("apply transitions-exceeded actions: %w", err)
		}
	}
	return true, nil
}
