// Package dispatcher claims queued jobs and supervises bounded subprocesses.
package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"issueq/internal/actions"
	"issueq/internal/config"
	issuegithub "issueq/internal/github"
	"issueq/internal/model"
	"issueq/internal/router"
	"issueq/internal/runner"
	"issueq/internal/store"
)

type Result struct {
	Claimed   int
	Succeeded int
	Failed    int
	Skipped   int
	Dead      int
}

func Dispatch(ctx context.Context, cfg config.Config, queue store.QueueStore) (Result, error) {
	return dispatchLocal(ctx, cfg, queue)
}

func DispatchWithGitHub(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client) (Result, error) {
	if gh == nil {
		return dispatchLocal(ctx, cfg, queue)
	}
	runnerIdentity := model.RunnerIdentity{RunnerID: runnerID(cfg), InstanceID: runnerID(cfg)}
	runnerInfo := model.RunnerInfo{ID: runnerIdentity.RunnerID, Name: cfg.Runner.Name}
	limits := perRouteLimits(cfg)
	maxGlobal := cfg.Queue.MaxGlobalConcurrency
	if maxGlobal <= 0 {
		maxGlobal = 1
	}
	lease := cfg.Queue.LeaseDuration.Duration
	if lease <= 0 {
		lease = config.DefaultLeaseDuration
	}

	var result Result
	for {
		job, err := queue.ClaimNextJob(ctx, runnerIdentity, cfg.Runner.Capabilities, maxGlobal, limits, lease)
		if err != nil {
			return result, err
		}
		if job == nil {
			return result, nil
		}
		result.Claimed++
		route, ok := findRoute(cfg, job.RouteName)
		if !ok {
			if err := queue.FinalizeJob(ctx, job.ID, model.JobFinalize{Status: model.JobStatusFailed, LastError: "route not found"}); err != nil {
				return result, err
			}
			result.Failed++
			continue
		}
		issue, err := queue.GetIssue(ctx, job.IssueKey)
		if err != nil {
			if ferr := queue.FinalizeJob(ctx, job.ID, model.JobFinalize{Status: model.JobStatusFailed, LastError: fmt.Sprintf("load issue: %v", err)}); ferr != nil {
				return result, ferr
			}
			result.Failed++
			continue
		}

		if gh != nil {
			latest, err := gh.GetIssue(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo, issue.Number)
			if err != nil {
				if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("refresh issue: %v", err)); ferr != nil {
					return result, ferr
				}
				continue
			}
			if err := queue.UpsertIssue(ctx, latest); err != nil {
				if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("store refreshed issue: %v", err)); ferr != nil {
					return result, ferr
				}
				continue
			}
			if !router.Matches(cfg, route, latest) {
				if err := queue.FinalizeJob(ctx, job.ID, model.JobFinalize{Status: model.JobStatusSkipped, LastError: "stale route predicate"}); err != nil {
					return result, err
				}
				_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "job_skipped", Message: "stale route predicate"})
				result.Skipped++
				continue
			}
			issue = latest
			generation, _, err := queue.GetIssueState(ctx, issue.IssueKey)
			if err != nil {
				if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("load issue state: %v", err)); ferr != nil {
					return result, ferr
				}
				continue
			}
			attempts, err := queue.IncrementAttempts(ctx, issue.IssueKey, generation, route.Name)
			if err != nil {
				if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("increment attempts: %v", err)); ferr != nil {
					return result, ferr
				}
				continue
			}
			job.Attempts = attempts
			if err := queue.UpdateJobAttempts(ctx, job.ID, attempts); err != nil {
				if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("persist job attempts: %v", err)); ferr != nil {
					return result, ferr
				}
				continue
			}
			if attempts > route.Job.MaxAttempts {
				if _, err := actions.Apply(ctx, cfg, gh, queue, issue, route.Job.OnAttemptsExceeded); err != nil {
					if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("apply attempts-exceeded actions: %v", err)); ferr != nil {
						return result, ferr
					}
					continue
				}
				if err := queue.FinalizeJob(ctx, job.ID, model.JobFinalize{Status: model.JobStatusDead, LastError: "max attempts exceeded"}); err != nil {
					return result, err
				}
				_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "job_dead", Message: "max attempts exceeded"})
				result.Dead++
				continue
			}
			applied, err := actions.Apply(ctx, cfg, gh, queue, issue, route.Job.OnStart)
			if err != nil {
				if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("apply on_start actions: %v", err)); ferr != nil {
					return result, ferr
				}
				continue
			}
			issue = applied.UpdatedIssue
			if applied.Changed {
				dead, err := checkTransitionLimit(ctx, cfg, queue, gh, job.ID, issue, route)
				if err != nil {
					if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("check transition limit: %v", err)); ferr != nil {
						return result, ferr
					}
					continue
				}
				if dead {
					if err := queue.FinalizeJob(ctx, job.ID, model.JobFinalize{Status: model.JobStatusDead, LastError: "max transitions exceeded"}); err != nil {
						return result, err
					}
					_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "job_dead", Message: "max transitions exceeded"})
					result.Dead++
					continue
				}
			}
			_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "action_on_start"})
		}

		runResult := runner.Run(ctx, cfg, route, *job, issue, runnerInfo)
		_ = queue.UpdateJobArtifacts(ctx, job.ID, runResult.Paths.ContextPath, runResult.Paths.ResultPath, runResult.Paths.StdoutPath, runResult.Paths.StderrPath, runResult.PID)
		status := model.JobStatusSucceeded
		lastErr := ""
		baseAction := route.Job.OnSuccess
		if runResult.Error != nil || runResult.ExitCode != 0 {
			status = model.JobStatusFailed
			lastErr = runResult.ErrorString()
			baseAction = route.Job.OnFailure
		}

		finalAction := baseAction
		if runResult.Paths.ResultPath != "" {
			resultAction, found, parseErr := actions.ParseResultFile(runResult.Paths.ResultPath)
			if parseErr != nil {
				status = model.JobStatusFailed
				lastErr = parseErr.Error()
				finalAction = route.Job.OnFailure
			} else if found {
				finalAction = actions.Merge(baseAction, resultAction)
			}
		}

		if gh != nil {
			applied, err := actions.Apply(ctx, cfg, gh, queue, issue, finalAction)
			if err != nil {
				status = model.JobStatusFailed
				lastErr = err.Error()
			} else {
				issue = applied.UpdatedIssue
				if applied.Changed {
					dead, err := checkTransitionLimit(ctx, cfg, queue, gh, job.ID, issue, route)
					if err != nil {
						if ferr := failClaimedJob(ctx, queue, job, &result, fmt.Sprintf("check transition limit: %v", err)); ferr != nil {
							return result, ferr
						}
						continue
					}
					if dead {
						status = model.JobStatusDead
						lastErr = "max transitions exceeded"
					}
				}
			}
			_ = issue
		}

		if err := queue.FinalizeJob(ctx, job.ID, model.JobFinalize{
			Status:     status,
			LastError:  lastErr,
			ResultPath: runResult.Paths.ResultPath,
			StdoutPath: runResult.Paths.StdoutPath,
			StderrPath: runResult.Paths.StderrPath,
			FinishedAt: runResult.FinishedAt,
		}); err != nil {
			return result, err
		}
		_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "job_" + status, Message: lastErr})
		if status == model.JobStatusSucceeded {
			result.Succeeded++
		} else if status == model.JobStatusDead {
			result.Dead++
		} else {
			result.Failed++
		}
	}
}

func dispatchLocal(ctx context.Context, cfg config.Config, queue store.QueueStore) (Result, error) {
	runnerIdentity := newRunnerIdentity(cfg)
	runnerInfo := model.RunnerInfo{ID: runnerIdentity.RunnerID, Name: cfg.Runner.Name}
	limits := perRouteLimits(cfg)
	maxGlobal := cfg.Queue.MaxGlobalConcurrency
	if maxGlobal <= 0 {
		maxGlobal = 1
	}
	lease := cfg.Queue.LeaseDuration.Duration
	if lease <= 0 {
		lease = config.DefaultLeaseDuration
	}
	now := time.Now().UTC()
	if err := queue.HeartbeatRunner(ctx, runnerIdentity, os.Getpid(), now); err != nil {
		return Result{}, err
	}
	staleBefore := now.Add(-lease)
	if _, err := queue.ReleaseExpiredLeases(ctx, now, staleBefore, runnerIdentity.InstanceID, nil); err != nil {
		return Result{}, err
	}
	frontier, err := queue.ListEligibleJobIDs(ctx, now)
	if err != nil {
		return Result{}, err
	}

	active := map[string]*localActiveJob{}
	frontierRemaining := stringSet(frontier)
	var result Result
	for len(frontierRemaining) > 0 || len(active) > 0 {
		if err := queue.HeartbeatRunner(ctx, runnerIdentity, os.Getpid(), time.Now().UTC()); err != nil {
			if len(active) == 0 {
				return result, err
			}
		} else {
			for len(active) < maxGlobal && len(frontierRemaining) > 0 {
				job, err := queue.ClaimNextJobInFrontier(ctx, runnerIdentity, cfg.Runner.Capabilities, maxGlobal, limits, lease, keys(frontierRemaining))
				if err != nil {
					cancelActive(active, err)
					_ = waitActive(ctx, queue, runnerIdentity, active, &result)
					return result, err
				}
				if job == nil {
					break
				}
				delete(frontierRemaining, job.ID)
				result.Claimed++
				route, ok := findRoute(cfg, job.RouteName)
				if !ok {
					if err := queue.FinalizeJobOwned(ctx, job.ID, runnerIdentity.InstanceID, model.JobFinalize{Status: model.JobStatusFailed, LastError: "route not found"}); err != nil {
						if isOwnershipLoss(err) {
							continue
						}
						return result, err
					}
					result.Failed++
					continue
				}
				issue, err := queue.GetIssue(ctx, job.IssueKey)
				if err != nil {
					if err := queue.FinalizeJobOwned(ctx, job.ID, runnerIdentity.InstanceID, model.JobFinalize{Status: model.JobStatusFailed, LastError: fmt.Sprintf("load issue: %v", err)}); err != nil {
						if isOwnershipLoss(err) {
							continue
						}
						return result, err
					}
					result.Failed++
					continue
				}
				handle, err := runner.Start(ctx, cfg, route, *job, issue, runnerInfo)
				if err != nil {
					var startErr runner.StartError
					if !errors.As(err, &startErr) {
						return result, err
					}
					if err := queue.UpdateJobArtifactsOwned(ctx, job.ID, runnerIdentity.InstanceID, startErr.Result.Paths.ContextPath, startErr.Result.Paths.ResultPath, startErr.Result.Paths.StdoutPath, startErr.Result.Paths.StderrPath, startErr.Result.PID); err != nil {
						if isOwnershipLoss(err) {
							continue
						}
						return result, err
					}
					if err := queue.FinalizeJobOwned(ctx, job.ID, runnerIdentity.InstanceID, model.JobFinalize{Status: model.JobStatusFailed, LastError: startErr.Result.ErrorString(), FinishedAt: startErr.Result.FinishedAt}); err != nil {
						if isOwnershipLoss(err) {
							continue
						}
						return result, err
					}
					result.Failed++
					continue
				}
				if err := queue.UpdateJobArtifactsOwned(ctx, job.ID, runnerIdentity.InstanceID, handle.Paths.ContextPath, handle.Paths.ResultPath, handle.Paths.StdoutPath, handle.Paths.StderrPath, handle.PID); err != nil {
					handle.Cancel(err)
					_ = runner.Wait(handle)
					if isOwnershipLoss(err) {
						continue
					}
					return result, err
				}
				active[job.ID] = &localActiveJob{job: job, handle: handle}
			}
		}
		if len(active) == 0 {
			if len(frontierRemaining) > 0 {
				return result, nil
			}
			continue
		}
		waitCh := firstDone(active)
		heartbeatTimer := time.NewTimer(heartbeatInterval(lease))
		select {
		case <-ctx.Done():
			stopTimer(heartbeatTimer)
			cancelActive(active, context.Cause(ctx))
			_ = waitActive(ctx, queue, runnerIdentity, active, &result)
			return result, ctx.Err()
		case <-waitCh:
			stopTimer(heartbeatTimer)
		case <-heartbeatTimer.C:
			if err := renewActive(ctx, queue, runnerIdentity, active, lease); err != nil {
				if len(active) == 0 {
					return result, nil
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					cancelActive(active, err)
					_ = waitActive(ctx, queue, runnerIdentity, active, &result)
					return result, err
				}
			}
			continue
		}
		if err := reapReady(ctx, queue, runnerIdentity, active, &result); err != nil {
			cancelActive(active, err)
			_ = waitActive(ctx, queue, runnerIdentity, active, &result)
			return result, err
		}
	}
	return result, nil
}

type localActiveJob struct {
	job           *model.Job
	handle        *runner.Handle
	lostOwnership bool
}

func renewActive(ctx context.Context, queue store.QueueStore, identity model.RunnerIdentity, active map[string]*localActiveJob, lease time.Duration) error {
	var transientErr error
	if err := queue.HeartbeatRunner(ctx, identity, os.Getpid(), time.Now().UTC()); err != nil {
		transientErr = err
	}
	for id, activeJob := range active {
		if err := queue.RenewJobLease(ctx, activeJob.job.ID, identity.InstanceID, lease); err != nil {
			if isOwnershipLoss(err) {
				activeJob.lostOwnership = true
				activeJob.handle.Cancel(err)
				delete(active, id)
				_ = runner.Wait(activeJob.handle)
				continue
			}
			if transientErr == nil {
				transientErr = err
			}
		}
	}
	return transientErr
}

func isOwnershipLoss(err error) bool {
	return errors.Is(err, store.ErrLostLease) || errors.Is(err, store.ErrNotOwner)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func cancelActive(active map[string]*localActiveJob, cause error) {
	for _, activeJob := range active {
		activeJob.handle.Cancel(cause)
	}
}

func waitActive(ctx context.Context, queue store.QueueStore, identity model.RunnerIdentity, active map[string]*localActiveJob, result *Result) error {
	for len(active) > 0 {
		<-firstDone(active)
		if err := reapReady(ctx, queue, identity, active, result); err != nil {
			return err
		}
	}
	return nil
}

func reapReady(ctx context.Context, queue store.QueueStore, identity model.RunnerIdentity, active map[string]*localActiveJob, result *Result) error {
	for id, activeJob := range active {
		select {
		case <-activeJob.handle.Done:
		case <-activeJob.handle.ContextDone:
		default:
			continue
		}
		delete(active, id)
		runResult := runner.Wait(activeJob.handle)
		if activeJob.lostOwnership {
			continue
		}
		if err := queue.UpdateJobArtifactsOwned(ctx, activeJob.job.ID, identity.InstanceID, runResult.Paths.ContextPath, runResult.Paths.ResultPath, runResult.Paths.StdoutPath, runResult.Paths.StderrPath, runResult.PID); err != nil {
			if isOwnershipLoss(err) {
				continue
			}
			return err
		}
		status := model.JobStatusSucceeded
		lastErr := ""
		if runResult.Error != nil || runResult.ExitCode != 0 {
			status = model.JobStatusFailed
			lastErr = runResult.ErrorString()
		}
		if status == model.JobStatusSucceeded && runResult.Paths.ResultPath != "" {
			_, _, parseErr := actions.ParseResultFile(runResult.Paths.ResultPath)
			if parseErr != nil {
				status = model.JobStatusFailed
				lastErr = parseErr.Error()
			}
		}
		if err := queue.FinalizeJobOwned(ctx, activeJob.job.ID, identity.InstanceID, model.JobFinalize{Status: status, LastError: lastErr, ResultPath: runResult.Paths.ResultPath, StdoutPath: runResult.Paths.StdoutPath, StderrPath: runResult.Paths.StderrPath, FinishedAt: runResult.FinishedAt}); err != nil {
			if isOwnershipLoss(err) {
				continue
			}
			return err
		}
		_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: activeJob.job.ID, IssueKey: activeJob.job.IssueKey, EventType: "job_" + status, Message: lastErr})
		if status == model.JobStatusSucceeded {
			result.Succeeded++
		} else {
			result.Failed++
		}
	}
	return nil
}

func firstDone(active map[string]*localActiveJob) <-chan struct{} {
	out := make(chan struct{})
	var once sync.Once
	for _, activeJob := range active {
		go func(done <-chan struct{}, contextDone <-chan struct{}) {
			select {
			case <-done:
			case <-contextDone:
			}
			once.Do(func() { close(out) })
		}(activeJob.handle.Done, activeJob.handle.ContextDone)
	}
	return out
}

func heartbeatInterval(lease time.Duration) time.Duration {
	interval := lease / 4
	if interval <= 0 || interval > time.Second {
		interval = time.Second
	}
	return interval
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func keys(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	return values
}

func findRoute(cfg config.Config, name string) (config.RouteConfig, bool) {
	for _, route := range cfg.Routes {
		if route.Name == name {
			return route, true
		}
	}
	return config.RouteConfig{}, false
}

func perRouteLimits(cfg config.Config) map[string]int {
	limits := map[string]int{}
	for _, route := range cfg.Routes {
		limits[route.Name] = route.Job.Concurrency
	}
	return limits
}

func newRunnerIdentity(cfg config.Config) model.RunnerIdentity {
	id := runnerID(cfg)
	return model.RunnerIdentity{RunnerID: id, InstanceID: fmt.Sprintf("%s-%d-%d", id, os.Getpid(), time.Now().UTC().UnixNano())}
}

func runnerID(cfg config.Config) string {
	if cfg.Runner.Name != "" {
		return cfg.Runner.Name
	}
	return "issueq-local"
}

func failClaimedJob(ctx context.Context, queue store.QueueStore, job *model.Job, result *Result, message string) error {
	if err := queue.FinalizeJob(ctx, job.ID, model.JobFinalize{Status: model.JobStatusFailed, LastError: message}); err != nil {
		return err
	}
	_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "job_failed", Message: message})
	result.Failed++
	return nil
}

func checkTransitionLimit(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, jobID string, issue model.IssueSnapshot, route config.RouteConfig) (bool, error) {
	count, err := queue.IncrementTransitions(ctx, issue.IssueKey)
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
		if _, err := actions.Apply(ctx, cfg, gh, queue, issue, cfg.Workflow.OnTransitionsExceeded); err != nil {
			_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: jobID, IssueKey: issue.IssueKey, EventType: "terminal_action_failed", Message: err.Error()})
			return false, fmt.Errorf("apply transitions-exceeded actions: %w", err)
		}
	}
	return true, nil
}
