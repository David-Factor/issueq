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
	return dispatchGitHub(ctx, cfg, queue, gh)
}

func dispatchLocal(ctx context.Context, cfg config.Config, queue store.QueueStore) (Result, error) {
	return dispatchConcurrent(ctx, cfg, queue, nil)
}

func dispatchGitHub(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client) (Result, error) {
	return dispatchConcurrent(ctx, cfg, queue, gh)
}

func dispatchConcurrent(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client) (Result, error) {
	supervisor := NewSupervisor(cfg, queue, gh)
	now := time.Now().UTC()
	if err := supervisor.Heartbeat(ctx); err != nil {
		return Result{}, err
	}
	staleBefore := now.Add(-supervisor.LeaseDuration())
	if _, err := queue.ReleaseExpiredLeases(ctx, now, staleBefore, supervisor.Identity().InstanceID, nil); err != nil {
		return Result{}, err
	}
	frontier, err := queue.ListEligibleJobIDs(ctx, now)
	if err != nil {
		return Result{}, err
	}

	frontierRemaining := stringSet(frontier)
	for len(frontierRemaining) > 0 || supervisor.ActiveCount() > 0 {
		if err := supervisor.Heartbeat(ctx); err != nil {
			if supervisor.ActiveCount() == 0 {
				return supervisor.Result(), err
			}
		} else {
			if err := supervisor.refill(ctx, frontierRemaining); err != nil {
				supervisor.CancelActive(err)
				_ = supervisor.Wait(ctx)
				return supervisor.Result(), err
			}
		}
		if supervisor.ActiveCount() == 0 {
			if len(frontierRemaining) > 0 {
				return supervisor.Result(), nil
			}
			continue
		}
		waitCh := supervisor.firstDone()
		heartbeatTimer := time.NewTimer(HeartbeatInterval(supervisor.LeaseDuration()))
		select {
		case <-ctx.Done():
			stopTimer(heartbeatTimer)
			cleanupCtx, cancel := context.WithTimeout(context.Background(), CleanupTimeout(supervisor.LeaseDuration()))
			supervisor.CancelActive(context.Cause(ctx))
			_ = supervisor.Wait(cleanupCtx)
			cancel()
			return supervisor.Result(), ctx.Err()
		case <-waitCh:
			stopTimer(heartbeatTimer)
		case <-heartbeatTimer.C:
			if err := supervisor.Renew(ctx); err != nil {
				if supervisor.ActiveCount() == 0 {
					return supervisor.Result(), nil
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), CleanupTimeout(supervisor.LeaseDuration()))
					supervisor.CancelActive(err)
					_ = supervisor.Wait(cleanupCtx)
					cancel()
					return supervisor.Result(), err
				}
			}
			continue
		}
		if err := supervisor.ReapReady(ctx); err != nil {
			supervisor.CancelActive(err)
			_ = supervisor.Wait(ctx)
			return supervisor.Result(), err
		}
	}
	return supervisor.Result(), nil
}

type Supervisor struct {
	mu         sync.Mutex
	cfg        config.Config
	queue      store.QueueStore
	gh         issuegithub.Client
	identity   model.RunnerIdentity
	runnerInfo model.RunnerInfo
	limits     map[string]int
	maxGlobal  int
	lease      time.Duration
	active     map[string]*localActiveJob
	result     Result
	processID  int
}

func NewSupervisor(cfg config.Config, queue store.QueueStore, gh issuegithub.Client) *Supervisor {
	identity := newRunnerIdentity(cfg)
	maxGlobal := cfg.Queue.MaxGlobalConcurrency
	if maxGlobal <= 0 {
		maxGlobal = 1
	}
	lease := cfg.Queue.LeaseDuration.Duration
	if lease <= 0 {
		lease = config.DefaultLeaseDuration
	}
	return &Supervisor{
		cfg:        cfg,
		queue:      queue,
		gh:         gh,
		identity:   identity,
		runnerInfo: model.RunnerInfo{ID: identity.RunnerID, Name: cfg.Runner.Name},
		limits:     perRouteLimits(cfg),
		maxGlobal:  maxGlobal,
		lease:      lease,
		active:     map[string]*localActiveJob{},
		processID:  os.Getpid(),
	}
}

func (s *Supervisor) lock(ctx context.Context) error {
	for {
		if s.mu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *Supervisor) Identity() model.RunnerIdentity {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.identity
}
func (s *Supervisor) LeaseDuration() time.Duration { return s.lease }
func (s *Supervisor) ActiveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}
func (s *Supervisor) Result() Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func (s *Supervisor) ActiveJobIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeJobIDsLocked()
}

func (s *Supervisor) activeJobIDsLocked() []string {
	ids := make([]string, 0, len(s.active))
	for id := range s.active {
		ids = append(ids, id)
	}
	return ids
}

func (s *Supervisor) Heartbeat(ctx context.Context) error {
	return s.queue.HeartbeatRunner(ctx, s.identity, s.processID, time.Now().UTC())
}

func (s *Supervisor) ReleaseExpiredLeases(ctx context.Context) (int, error) {
	s.mu.Lock()
	activeJobIDs := s.activeJobIDsLocked()
	instanceID := s.identity.InstanceID
	s.mu.Unlock()
	now := time.Now().UTC()
	return s.queue.ReleaseExpiredLeases(ctx, now, now.Add(-s.lease), instanceID, activeJobIDs)
}

func (s *Supervisor) PruneStaleHeartbeats(ctx context.Context) (int, error) {
	return s.queue.PruneStaleRunnerHeartbeats(ctx, time.Now().UTC().Add(-s.lease))
}

func (s *Supervisor) DeleteHeartbeat(ctx context.Context) error {
	return s.queue.DeleteRunnerHeartbeat(ctx, s.identity.InstanceID)
}

func (s *Supervisor) Refill(ctx context.Context) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.refill(ctx, nil)
}

func (s *Supervisor) refill(ctx context.Context, frontierRemaining map[string]struct{}) error {
	for len(s.active) < s.maxGlobal {
		var job *model.Job
		var err error
		if frontierRemaining != nil {
			if len(frontierRemaining) == 0 {
				return nil
			}
			job, err = s.queue.ClaimNextJobInFrontier(ctx, s.identity, s.cfg.Runner.Capabilities, s.maxGlobal, s.limits, s.lease, keys(frontierRemaining))
		} else {
			job, err = s.queue.ClaimNextJob(ctx, s.identity, s.cfg.Runner.Capabilities, s.maxGlobal, s.limits, s.lease)
		}
		if err != nil {
			return err
		}
		if job == nil {
			return nil
		}
		if frontierRemaining != nil {
			delete(frontierRemaining, job.ID)
		}
		s.result.Claimed++
		route, ok := findRoute(s.cfg, job.RouteName)
		if !ok {
			dropped, err := dropOnOwnershipLoss(s.queue.FinalizeJobOwned(ctx, job.ID, s.identity.InstanceID, model.JobFinalize{Status: model.JobStatusFailed, LastError: "route not found"}))
			if err != nil {
				return err
			}
			if dropped {
				continue
			}
			s.result.Failed++
			continue
		}
		activeJob := &localActiveJob{job: job, route: route}
		s.active[job.ID] = activeJob
		issue, outcome, err := prepareClaimedJob(ctx, s.cfg, s.queue, s.gh, s.identity, job, route, s.lease, &s.result)
		if err != nil {
			if ctx.Err() == nil {
				activeJob.preStartError = err.Error()
			}
			return err
		}
		if outcome != claimStart {
			delete(s.active, job.ID)
			continue
		}
		activeJob.issue = issue
		handle, err := runner.Start(ctx, s.cfg, route, *job, issue, s.runnerInfo)
		activeJob.handle = handle
		if err != nil {
			var startErr runner.StartError
			if !errors.As(err, &startErr) {
				if ctx.Err() == nil {
					activeJob.preStartError = err.Error()
				}
				return err
			}
			dropped, err := updateArtifactsOwnedOrDrop(ctx, s.queue, job.ID, s.identity.InstanceID, startErr.Result)
			if err != nil {
				if ctx.Err() == nil {
					activeJob.preStartError = err.Error()
				}
				return err
			}
			if dropped {
				delete(s.active, job.ID)
				continue
			}
			if err := finalizeRunResult(ctx, s.cfg, s.queue, s.gh, activeJob, s.identity, s.lease, startErr.Result, &s.result); err != nil {
				if ctx.Err() == nil {
					activeJob.preStartError = err.Error()
				}
				return err
			}
			delete(s.active, job.ID)
			continue
		}
		if err := s.queue.UpdateJobArtifactsOwned(ctx, job.ID, s.identity.InstanceID, handle.Paths.ContextPath, handle.Paths.ResultPath, handle.Paths.StdoutPath, handle.Paths.StderrPath, handle.PID); err != nil {
			handle.Cancel(err)
			runResult := runner.Wait(handle)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), CleanupTimeout(s.lease))
				dropped, cleanupErr := updateArtifactsOwnedOrDrop(cleanupCtx, s.queue, job.ID, s.identity.InstanceID, runResult)
				if cleanupErr == nil && !dropped {
					cleanupErr = finalizeRunResult(cleanupCtx, s.cfg, s.queue, s.gh, activeJob, s.identity, s.lease, runResult, &s.result)
				}
				cancel()
				if cleanupErr != nil {
					return cleanupErr
				}
				delete(s.active, job.ID)
				return err
			}
			dropped, err := dropOnOwnershipLoss(err)
			if err != nil {
				return err
			}
			delete(s.active, job.ID)
			if dropped {
				continue
			}
			continue
		}
	}
	return nil
}

func (s *Supervisor) Renew(ctx context.Context) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return renewActive(ctx, s.queue, s.identity, s.active, s.lease)
}

func (s *Supervisor) ReapReady(ctx context.Context) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return reapReady(ctx, s.cfg, s.queue, s.gh, s.identity, s.active, s.lease, &s.result)
}

func (s *Supervisor) CancelActive(cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cancelActive(s.active, cause)
}

func (s *Supervisor) Wait(ctx context.Context) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return waitActive(ctx, s.cfg, s.queue, s.gh, s.identity, s.active, s.lease, &s.result)
}

func (s *Supervisor) Shutdown(ctx context.Context, cause error) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if cause == nil {
		cause = context.Canceled
	}
	cancelActive(s.active, cause)
	return waitActive(ctx, s.cfg, s.queue, s.gh, s.identity, s.active, s.lease, &s.result)
}

func (s *Supervisor) firstDone() <-chan struct{} {
	return firstDone(s.active)
}

type localActiveJob struct {
	job           *model.Job
	issue         model.IssueSnapshot
	route         config.RouteConfig
	handle        *runner.Handle
	lostOwnership bool
	preStartError string
}

type claimOutcome int

const (
	claimStart claimOutcome = iota
	claimDrop
	claimContinue
)

func applyOwned(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, identity model.RunnerIdentity, job *model.Job, lease time.Duration, issue model.IssueSnapshot, action config.ActionConfig) (actions.ApplyResult, error) {
	return actions.ApplyWithHooks(ctx, cfg, gh, queue, issue, action, actions.ApplyHooks{BeforeSideEffect: func() error {
		return queue.RenewJobLease(ctx, job.ID, identity.InstanceID, lease)
	}})
}

func prepareClaimedJob(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, identity model.RunnerIdentity, job *model.Job, route config.RouteConfig, lease time.Duration, result *Result) (model.IssueSnapshot, claimOutcome, error) {
	issue, err := queue.GetIssue(ctx, job.IssueKey)
	if err != nil {
		return failClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, fmt.Sprintf("load issue: %v", err))
	}
	if gh == nil {
		return issue, claimStart, nil
	}
	if err := queue.RenewJobLease(ctx, job.ID, identity.InstanceID, lease); err != nil {
		outcome, err := dropClaimOnOwnershipLoss(err)
		return model.IssueSnapshot{}, outcome, err
	}
	latest, err := gh.GetIssue(ctx, cfg.GitHub.Owner, cfg.GitHub.Repo, issue.Number)
	if err != nil {
		return failClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, fmt.Sprintf("refresh issue: %v", err))
	}
	if err := queue.RenewJobLease(ctx, job.ID, identity.InstanceID, lease); err != nil {
		outcome, err := dropClaimOnOwnershipLoss(err)
		return model.IssueSnapshot{}, outcome, err
	}
	if err := queue.UpsertIssue(ctx, latest); err != nil {
		return failClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, fmt.Sprintf("store refreshed issue: %v", err))
	}
	if !router.Matches(cfg, route, latest) {
		outcome, err := finalizeClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, model.JobStatusSkipped, "stale route predicate")
		return model.IssueSnapshot{}, outcome, err
	}
	issue = latest
	generation, _, err := queue.GetIssueState(ctx, issue.IssueKey)
	if err != nil {
		return failClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, fmt.Sprintf("load issue state: %v", err))
	}
	attempts, err := queue.IncrementAttemptsForJob(ctx, job.ID, identity.InstanceID, issue.IssueKey, generation, route.Name)
	if err != nil {
		if isOwnershipLoss(err) {
			return model.IssueSnapshot{}, claimDrop, nil
		}
		return failClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, fmt.Sprintf("increment attempts: %v", err))
	}
	job.Attempts = attempts
	if attempts > route.Job.MaxAttempts {
		if err := queue.RenewJobLease(ctx, job.ID, identity.InstanceID, lease); err != nil {
			outcome, err := dropClaimOnOwnershipLoss(err)
			return model.IssueSnapshot{}, outcome, err
		}
		if _, err := applyOwned(ctx, cfg, queue, gh, identity, job, lease, issue, route.Job.OnAttemptsExceeded); err != nil {
			if isOwnershipLoss(err) {
				return model.IssueSnapshot{}, claimDrop, nil
			}
			return failClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, fmt.Sprintf("apply attempts-exceeded actions: %v", err))
		}
		outcome, err := finalizeClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, model.JobStatusDead, "max attempts exceeded")
		return model.IssueSnapshot{}, outcome, err
	}
	if err := queue.RenewJobLease(ctx, job.ID, identity.InstanceID, lease); err != nil {
		outcome, err := dropClaimOnOwnershipLoss(err)
		return model.IssueSnapshot{}, outcome, err
	}
	applied, err := applyOwned(ctx, cfg, queue, gh, identity, job, lease, issue, route.Job.OnStart)
	if err != nil {
		if isOwnershipLoss(err) {
			return model.IssueSnapshot{}, claimDrop, nil
		}
		return failClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, fmt.Sprintf("apply on_start actions: %v", err))
	}
	issue = applied.UpdatedIssue
	if applied.Changed {
		dead, err := checkTransitionLimitOwned(ctx, cfg, queue, gh, identity, job, lease, issue)
		if err != nil {
			if isOwnershipLoss(err) {
				return model.IssueSnapshot{}, claimDrop, nil
			}
			return failClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, fmt.Sprintf("check transition limit: %v", err))
		}
		if dead {
			outcome, err := finalizeClaimedJobOwned(ctx, queue, job, identity.InstanceID, result, model.JobStatusDead, "max transitions exceeded")
			return model.IssueSnapshot{}, outcome, err
		}
	}
	_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "action_on_start"})
	return issue, claimStart, nil
}

func dropClaimOnOwnershipLoss(err error) (claimOutcome, error) {
	dropped, err := dropOnOwnershipLoss(err)
	if err != nil {
		return claimDrop, err
	}
	if dropped {
		return claimDrop, nil
	}
	return claimStart, nil
}

func finalizeClaimedJobOwned(ctx context.Context, queue store.QueueStore, job *model.Job, runnerInstanceID string, result *Result, status string, message string) (claimOutcome, error) {
	dropped, err := dropOnOwnershipLoss(queue.FinalizeJobOwned(ctx, job.ID, runnerInstanceID, model.JobFinalize{Status: status, LastError: message}))
	if err != nil || dropped {
		return claimDrop, err
	}
	_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "job_" + status, Message: message})
	switch status {
	case model.JobStatusSucceeded:
		result.Succeeded++
	case model.JobStatusSkipped:
		result.Skipped++
	case model.JobStatusDead:
		result.Dead++
	default:
		result.Failed++
	}
	return claimContinue, nil
}

func failClaimedJobOwned(ctx context.Context, queue store.QueueStore, job *model.Job, runnerInstanceID string, result *Result, message string) (model.IssueSnapshot, claimOutcome, error) {
	outcome, err := finalizeClaimedJobOwned(ctx, queue, job, runnerInstanceID, result, model.JobStatusFailed, message)
	return model.IssueSnapshot{}, outcome, err
}

func checkTransitionLimitOwned(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, identity model.RunnerIdentity, job *model.Job, lease time.Duration, issue model.IssueSnapshot) (bool, error) {
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
		if _, err := applyOwned(ctx, cfg, queue, gh, identity, job, lease, issue, cfg.Workflow.OnTransitionsExceeded); err != nil {
			if isOwnershipLoss(err) {
				return false, err
			}
			_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: issue.IssueKey, EventType: "terminal_action_failed", Message: err.Error()})
			return false, fmt.Errorf("apply transitions-exceeded actions: %w", err)
		}
	}
	return true, nil
}

func renewActive(ctx context.Context, queue store.QueueStore, identity model.RunnerIdentity, active map[string]*localActiveJob, lease time.Duration) error {
	var transientErr error
	if err := queue.HeartbeatRunner(ctx, identity, os.Getpid(), time.Now().UTC()); err != nil {
		transientErr = err
	}
	for id, activeJob := range active {
		if activeJob.handle == nil {
			continue
		}
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

func dropOnOwnershipLoss(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if isOwnershipLoss(err) {
		return true, nil
	}
	return false, err
}

func updateArtifactsOwnedOrDrop(ctx context.Context, queue store.QueueStore, jobID, runnerInstanceID string, runResult runner.Result) (bool, error) {
	return dropOnOwnershipLoss(queue.UpdateJobArtifactsOwned(ctx, jobID, runnerInstanceID, runResult.Paths.ContextPath, runResult.Paths.ResultPath, runResult.Paths.StdoutPath, runResult.Paths.StderrPath, runResult.PID))
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
		if activeJob.handle == nil {
			continue
		}
		activeJob.handle.Cancel(cause)
	}
}

func waitActive(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, identity model.RunnerIdentity, active map[string]*localActiveJob, lease time.Duration, result *Result) error {
	for len(active) > 0 {
		progress, err := finalizePreStartCancelled(ctx, queue, identity, active, result)
		if err != nil {
			return err
		}
		if progress {
			continue
		}
		waitCh := firstDone(active)
		if waitCh == nil {
			return errors.New("active pre-start job could not be finalized")
		}
		<-waitCh
		if err := reapReady(ctx, cfg, queue, gh, identity, active, lease, result); err != nil {
			return err
		}
	}
	return nil
}

func finalizePreStartCancelled(ctx context.Context, queue store.QueueStore, identity model.RunnerIdentity, active map[string]*localActiveJob, result *Result) (bool, error) {
	for id, activeJob := range active {
		if activeJob.handle != nil {
			continue
		}
		dropped, err := dropOnOwnershipLoss(queue.FinalizeJobOwned(ctx, activeJob.job.ID, identity.InstanceID, model.JobFinalize{Status: model.JobStatusCancelled, LastError: "runner shutting down"}))
		if err != nil {
			return false, err
		}
		if !dropped {
			_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: activeJob.job.ID, IssueKey: activeJob.job.IssueKey, EventType: "job_" + model.JobStatusCancelled, Message: "runner shutting down"})
		}
		delete(active, id)
		_ = result
		return true, nil
	}
	return false, nil
}

func reapReady(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, identity model.RunnerIdentity, active map[string]*localActiveJob, lease time.Duration, result *Result) error {
	for id, activeJob := range active {
		if activeJob.handle == nil {
			if activeJob.preStartError != "" {
				dropped, err := dropOnOwnershipLoss(queue.FinalizeJobOwned(ctx, activeJob.job.ID, identity.InstanceID, model.JobFinalize{Status: model.JobStatusFailed, LastError: activeJob.preStartError}))
				if err != nil {
					return err
				}
				delete(active, id)
				if !dropped {
					_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: activeJob.job.ID, IssueKey: activeJob.job.IssueKey, EventType: "job_" + model.JobStatusFailed, Message: activeJob.preStartError})
					result.Failed++
				}
			}
			continue
		}
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
		dropped, err := updateArtifactsOwnedOrDrop(ctx, queue, activeJob.job.ID, identity.InstanceID, runResult)
		if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), CleanupTimeout(lease))
			dropped, err = updateArtifactsOwnedOrDrop(cleanupCtx, queue, activeJob.job.ID, identity.InstanceID, runResult)
			if err == nil && !dropped {
				err = finalizeRunResult(cleanupCtx, cfg, queue, gh, activeJob, identity, lease, runResult, result)
			}
			cancel()
			return err
		}
		if err != nil {
			return err
		}
		if dropped {
			continue
		}
		if err := finalizeRunResult(ctx, cfg, queue, gh, activeJob, identity, lease, runResult, result); err != nil {
			return err
		}
	}
	return nil
}

func finalizeRunResult(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, activeJob *localActiveJob, identity model.RunnerIdentity, lease time.Duration, runResult runner.Result, result *Result) error {
	status := model.JobStatusSucceeded
	lastErr := ""
	baseAction := activeJob.route.Job.OnSuccess
	if runResult.Cancelled {
		status = model.JobStatusCancelled
		lastErr = runResult.ErrorString()
		baseAction = config.ActionConfig{}
	} else if runResult.Error != nil || runResult.ExitCode != 0 {
		status = model.JobStatusFailed
		lastErr = runResult.ErrorString()
		baseAction = activeJob.route.Job.OnFailure
	}
	finalAction := baseAction
	if !runResult.Cancelled && runResult.Paths.ResultPath != "" {
		resultAction, found, parseErr := actions.ParseResultFile(runResult.Paths.ResultPath)
		if parseErr != nil {
			status = model.JobStatusFailed
			lastErr = parseErr.Error()
			finalAction = activeJob.route.Job.OnFailure
		} else if found {
			finalAction = actions.Merge(baseAction, resultAction)
		}
	}
	if gh != nil && !runResult.Cancelled {
		if err := queue.RenewJobLease(ctx, activeJob.job.ID, identity.InstanceID, lease); err != nil {
			dropped, err := dropOnOwnershipLoss(err)
			if dropped || err == nil {
				return err
			}
			return err
		}
		applied, err := applyOwned(ctx, cfg, queue, gh, identity, activeJob.job, lease, activeJob.issue, finalAction)
		if err != nil {
			if isOwnershipLoss(err) {
				return nil
			}
			status = model.JobStatusFailed
			lastErr = err.Error()
		} else {
			activeJob.issue = applied.UpdatedIssue
			if applied.Changed {
				dead, err := checkTransitionLimitOwned(ctx, cfg, queue, gh, identity, activeJob.job, lease, activeJob.issue)
				if err != nil {
					if isOwnershipLoss(err) {
						return nil
					}
					return failClaimedJobAfterRunOwned(ctx, queue, activeJob.job, identity.InstanceID, result, fmt.Sprintf("check transition limit: %v", err))
				}
				if dead {
					status = model.JobStatusDead
					lastErr = "max transitions exceeded"
				}
			}
		}
	}
	dropped, err := dropOnOwnershipLoss(queue.FinalizeJobOwned(ctx, activeJob.job.ID, identity.InstanceID, model.JobFinalize{Status: status, LastError: lastErr, ResultPath: runResult.Paths.ResultPath, StdoutPath: runResult.Paths.StdoutPath, StderrPath: runResult.Paths.StderrPath, FinishedAt: runResult.FinishedAt}))
	if err != nil {
		return err
	}
	if dropped {
		return nil
	}
	_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: activeJob.job.ID, IssueKey: activeJob.job.IssueKey, EventType: "job_" + status, Message: lastErr})
	switch status {
	case model.JobStatusSucceeded:
		result.Succeeded++
	case model.JobStatusDead:
		result.Dead++
	case model.JobStatusCancelled:
		// Cancellation is tracked by job status but is not a command failure.
	default:
		result.Failed++
	}
	return nil
}

func failClaimedJobAfterRunOwned(ctx context.Context, queue store.QueueStore, job *model.Job, runnerInstanceID string, result *Result, message string) error {
	dropped, err := dropOnOwnershipLoss(queue.FinalizeJobOwned(ctx, job.ID, runnerInstanceID, model.JobFinalize{Status: model.JobStatusFailed, LastError: message}))
	if err != nil || dropped {
		return err
	}
	_, _ = queue.InsertJobEvent(ctx, model.JobEvent{JobID: job.ID, IssueKey: job.IssueKey, EventType: "job_failed", Message: message})
	result.Failed++
	return nil
}

func firstDone(active map[string]*localActiveJob) <-chan struct{} {
	if len(active) == 0 {
		return nil
	}
	out := make(chan struct{})
	var once sync.Once
	for _, activeJob := range active {
		if activeJob.handle == nil {
			continue
		}
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

func CleanupTimeout(lease time.Duration) time.Duration {
	if lease <= 0 {
		lease = config.DefaultLeaseDuration
	}
	timeout := lease
	if timeout < time.Second {
		timeout = time.Second
	}
	if timeout > 10*time.Second {
		timeout = 10 * time.Second
	}
	return timeout
}

func HeartbeatInterval(lease time.Duration) time.Duration {
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
