// Package daemon coordinates poll, route, and dispatch loops.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"issueq/internal/config"
	"issueq/internal/dispatcher"
	issuegithub "issueq/internal/github"
	"issueq/internal/model"
	"issueq/internal/poller"
	"issueq/internal/router"
	"issueq/internal/store"
	"issueq/internal/supervisor"
	"issueq/internal/supervisor/wrapper"
	"issueq/internal/workflow"
)

type Result struct {
	Poll     poller.Result
	Route    router.Result
	Dispatch dispatcher.Result
}

func Once(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client) (Result, error) {
	return onceWithSupervisor(ctx, cfg, queue, gh, wrapper.New(""))
}

func onceWithSupervisor(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, backend supervisor.Supervisor) (Result, error) {
	var result Result
	wave := dispatcher.NewWave(cfg, queue, gh, backend)
	if err := wave.Preflight(ctx); err != nil {
		return result, err
	}
	if gh != nil {
		pollResult, err := poller.Poll(ctx, cfg, gh, queue)
		if err != nil {
			return result, err
		}
		result.Poll = pollResult
	}
	routeResult, err := router.RouteWithGitHub(ctx, cfg, queue, gh)
	if err != nil {
		return result, err
	}
	result.Route = routeResult
	dispatchResult, err := wave.RunFrontier(ctx)
	if err != nil {
		return result, err
	}
	result.Dispatch = dispatchResult
	return result, nil
}

func Run(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, logger *slog.Logger) error {
	return runWithSupervisor(ctx, cfg, queue, gh, logger, wrapper.New(""))
}

func runWithSupervisor(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, logger *slog.Logger, backend supervisor.Supervisor) error {
	if logger == nil {
		logger = slog.Default()
	}
	loop := newLoop(cfg, queue, gh, logger, backend)
	return loop.run(ctx)
}

type loop struct {
	cfg        config.Config
	queue      store.QueueStore
	gh         issuegithub.Client
	logger     *slog.Logger
	backend    supervisor.Supervisor
	identity   model.RunnerIdentity
	runnerInfo model.RunnerInfo
	lease      time.Duration
	maxGlobal  int
	limits     map[string]int
	processID  int
}

func newLoop(cfg config.Config, queue store.QueueStore, gh issuegithub.Client, logger *slog.Logger, backend supervisor.Supervisor) *loop {
	identity := newRunnerIdentity(cfg)
	lease := cfg.Queue.LeaseDuration.Duration
	if lease <= 0 {
		lease = config.DefaultLeaseDuration
	}
	maxGlobal := cfg.Queue.MaxGlobalConcurrency
	if maxGlobal <= 0 {
		maxGlobal = 1
	}
	return &loop{cfg: cfg, queue: queue, gh: gh, logger: logger, backend: backend, identity: identity, runnerInfo: model.RunnerInfo{ID: identity.RunnerID, Name: cfg.Runner.Name}, lease: lease, maxGlobal: maxGlobal, limits: perRouteLimits(cfg), processID: os.Getpid()}
}

func (l *loop) run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	pollInterval := l.cfg.Polling.Interval.Duration
	if pollInterval <= 0 {
		pollInterval = config.DefaultPolling
	}
	heartbeatInterval := heartbeatInterval(l.lease)
	reconcileInterval := heartbeatInterval
	if reconcileInterval > 200*time.Millisecond {
		reconcileInterval = 200 * time.Millisecond
	}
	pollTimer := time.NewTimer(0)
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	reconcileTicker := time.NewTicker(reconcileInterval)
	defer pollTimer.Stop()
	defer heartbeatTicker.Stop()
	defer reconcileTicker.Stop()

	pollCtx, pollCancel := context.WithCancel(ctx)
	defer pollCancel()
	type pollDone struct{}
	pollDoneCh := make(chan pollDone, 1)
	pollRunning := false
	reconcileCtx, reconcileCancel := context.WithCancel(ctx)
	defer reconcileCancel()
	reconcileRequest := make(chan struct{}, 1)
	reconcileDoneCh := make(chan error, 1)
	reconcileRunning := false
	requestReconcile := func() {
		select {
		case reconcileRequest <- struct{}{}:
		default:
		}
	}
	startReconcile := func() {
		if reconcileRunning {
			requestReconcile()
			return
		}
		reconcileRunning = true
		go func() { reconcileDoneCh <- l.reconcile(reconcileCtx) }()
	}
	stop := func() error {
		pollCancel()
		if pollRunning {
			<-pollDoneCh
			pollRunning = false
		}
		reconcileCancel()
		if reconcileRunning {
			<-reconcileDoneCh
			reconcileRunning = false
		}
		return l.stop(ctx)
	}

	if err := l.heartbeat(ctx); err != nil {
		return err
	}
	requestReconcile()
	for {
		select {
		case <-ctx.Done():
			return stop()
		case <-heartbeatTicker.C:
			if err := l.heartbeat(ctx); err != nil {
				if ctx.Err() != nil {
					return stop()
				}
				l.logger.Error("runner heartbeat failed", "error", err)
			}
		case <-reconcileTicker.C:
			startReconcile()
		case <-reconcileRequest:
			startReconcile()
		case err := <-reconcileDoneCh:
			reconcileRunning = false
			if err != nil {
				if ctx.Err() != nil {
					return stop()
				}
				l.logger.Error("daemon reconcile failed", "error", err)
			}
		case <-pollTimer.C:
			if pollRunning {
				pollTimer.Reset(pollInterval)
				continue
			}
			pollRunning = true
			go func() {
				l.pollRoute(pollCtx)
				pollDoneCh <- pollDone{}
			}()
		case <-pollDoneCh:
			pollRunning = false
			requestReconcile()
			pollTimer.Reset(pollInterval)
		}
	}
}

func (l *loop) heartbeat(ctx context.Context) error {
	return workflow.HeartbeatRunner(ctx, l.queue, l.identity, l.processID, time.Now().UTC())
}

func (l *loop) reconcile(ctx context.Context) error {
	observed, err := workflow.ObserveOwnedRunningJobs(ctx, l.queue, l.backend, l.identity)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	activeIDs := make([]string, 0, len(observed))
	for _, item := range observed {
		activeIDs = append(activeIDs, item.Job.ID)
		decision := workflow.ObservationToDecision(item.Observation)
		switch decision {
		case workflow.DecisionKeepRunning:
			if err := l.queue.RenewJobLease(ctx, item.Job.ID, l.identity.InstanceID, l.lease); err != nil {
				if workflow.IsOwnershipLoss(err) {
					continue
				}
				return err
			}
		case workflow.DecisionUnknown:
			l.logger.Warn("owned durable job is unknown", "job_id", item.Job.ID, "error", item.Observation.Error)
		case workflow.DecisionSucceeded, workflow.DecisionFailed, workflow.DecisionCancelled:
			if err := l.finalizeObservation(ctx, item.Job, item.Observation, now); err != nil {
				return err
			}
		}
	}
	now = time.Now().UTC()
	adopted, summary, err := workflow.RecoverStaleDurableRunningJobs(ctx, l.queue, l.backend, l.identity, l.lease, now)
	if err != nil {
		return err
	}
	if summary.Scanned > 0 {
		l.logger.Info("stale durable recovery complete", "scanned", summary.Scanned, "adopted", summary.Adopted, "marked_unknown", summary.MarkedUnknown, "ownership_lost", summary.OwnershipLost)
	}
	for _, item := range adopted {
		activeIDs = append(activeIDs, item.Job.ID)
		switch workflow.ObservationToDecision(item.Observation) {
		case workflow.DecisionKeepRunning:
			if err := l.queue.RenewJobLease(ctx, item.Job.ID, l.identity.InstanceID, l.lease); err != nil {
				if workflow.IsOwnershipLoss(err) {
					continue
				}
				return err
			}
		case workflow.DecisionSucceeded, workflow.DecisionFailed, workflow.DecisionCancelled:
			if err := l.finalizeObservation(ctx, item.Job, item.Observation, now); err != nil {
				return err
			}
		case workflow.DecisionUnknown:
			l.logger.Warn("adopted durable job is unknown", "job_id", item.Job.ID, "error", item.Observation.Error)
		}
	}
	now = time.Now().UTC()
	if _, err := workflow.RecoverExpiredLeases(ctx, l.queue, now, l.lease, l.identity.InstanceID, activeIDs); err != nil {
		return err
	}
	if _, err := workflow.PruneStaleHeartbeats(ctx, l.queue, now.Add(-l.lease)); err != nil {
		return err
	}
	return l.refill(ctx)
}

func (l *loop) refill(ctx context.Context) error {
	for {
		running, err := l.queue.CountRunningJobs(ctx)
		if err != nil {
			return err
		}
		if running >= l.maxGlobal {
			return nil
		}
		job, err := workflow.ClaimOne(ctx, l.cfg, l.queue, l.identity, l.lease, l.limits)
		if err != nil {
			return err
		}
		if job == nil {
			return nil
		}
		route, ok := workflow.RouteByName(l.cfg, job.RouteName)
		if !ok {
			if err := finalizeClaimed(ctx, l.queue, *job, l.identity.InstanceID, model.JobStatusFailed, "route not found"); err != nil {
				return err
			}
			continue
		}
		issue, launch, err := l.prepareClaimed(ctx, job, route)
		if err != nil {
			return err
		}
		if !launch {
			continue
		}
		if _, err := workflow.LaunchClaimedWrapper(ctx, workflow.LaunchClaimedWrapperInput{Config: l.cfg, Route: route, Job: *job, Issue: issue, Identity: l.identity, RunnerInfo: l.runnerInfo, Store: l.queue, Supervisor: l.backend, CleanupTimeout: cleanupTimeout(l.lease)}); err != nil {
			if workflow.IsOwnershipLoss(err) {
				continue
			}
			return err
		}
	}
}

func (l *loop) prepareClaimed(ctx context.Context, job *model.Job, route config.RouteConfig) (model.IssueSnapshot, bool, error) {
	prepared, err := workflow.PrepareClaimedWrapperLaunch(ctx, workflow.PrepareClaimedWrapperInput{Config: l.cfg, Queue: l.queue, GitHub: l.gh, Identity: l.identity, Job: job, Route: route, Lease: l.lease})
	if err != nil {
		return model.IssueSnapshot{}, false, err
	}
	return prepared.Issue, prepared.Outcome == workflow.PrepareLaunch, nil
}

func (l *loop) finalizeObservation(ctx context.Context, job model.Job, obs supervisor.Observation, now time.Time) error {
	_, err := workflow.FinalizeOwnedObservationWithLifecycle(ctx, workflow.FinalizeObservationLifecycleInput{Config: l.cfg, Queue: l.queue, GitHub: l.gh, Identity: l.identity, Job: job, Obs: obs, Lease: l.lease, Now: now})
	return err
}

func finalizeClaimed(ctx context.Context, queue store.QueueStore, job model.Job, runnerInstanceID, status, message string) error {
	_, err := workflow.FinalizeClaimedJobOwned(ctx, queue, job, runnerInstanceID, status, message)
	return err
}

func (l *loop) stop(ctx context.Context) error {
	l.logger.Info("daemon stopping")
	if err := l.shutdown(context.Cause(ctx)); err == nil {
		l.deleteHeartbeatAfterCleanShutdown()
	} else {
		l.logger.Error("daemon shutdown cleanup failed", "error", err)
	}
	l.logger.Info("daemon stopped")
	return ctx.Err()
}

func (l *loop) pollRoute(ctx context.Context) {
	var pollResult poller.Result
	if l.gh != nil {
		result, err := poller.Poll(ctx, l.cfg, l.gh, l.queue)
		if err != nil {
			l.logger.Error("daemon poll failed", "error", err)
			return
		}
		pollResult = result
	}
	routeResult, err := router.RouteWithGitHub(ctx, l.cfg, l.queue, l.gh)
	if err != nil {
		l.logger.Error("daemon route failed", "error", err)
		return
	}
	l.logger.Info("daemon poll/route complete", "fetched", pollResult.IssuesFetched, "jobs_created", routeResult.JobsCreated)
}

func (l *loop) shutdown(cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout(l.lease))
	defer cancel()
	deadline := time.NewTicker(20 * time.Millisecond)
	defer deadline.Stop()
	for {
		if err := l.heartbeat(cleanupCtx); err != nil {
			return err
		}
		jobs, err := l.queue.ListOwnedRunningJobs(cleanupCtx, l.identity.InstanceID)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			return nil
		}
		remaining := 0
		for _, job := range jobs {
			if err := l.queue.RenewJobLease(cleanupCtx, job.ID, l.identity.InstanceID, l.lease); err != nil {
				if workflow.IsOwnershipLoss(err) {
					continue
				}
				return err
			}
			record, ok, _ := workflow.DurableLaunchRecord(job)
			if !ok {
				if job.LaunchState == model.LaunchStatePreparing || job.SupervisorKind == "" || job.LaunchToken == "" {
					dropped, err := workflow.DropOnOwnershipLoss(l.queue.FinalizeJobOwned(cleanupCtx, job.ID, l.identity.InstanceID, model.JobFinalize{Status: model.JobStatusCancelled, LastError: "runner shutting down", FinishedAt: time.Now().UTC()}))
					if err != nil {
						return err
					}
					if !dropped {
						continue
					}
				}
				remaining++
				continue
			}
			if err := l.backend.Cancel(cleanupCtx, record, supervisor.CancelShutdown); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			obs, err := workflow.InspectDurableJob(cleanupCtx, l.backend, job)
			if err != nil {
				return err
			}
			finalized, err := workflow.FinalizeOwnedObservation(cleanupCtx, l.queue, l.identity, job, obs, time.Now().UTC())
			if err != nil {
				return err
			}
			if !finalized.Finalized && !finalized.OwnershipLost {
				remaining++
			}
		}
		if remaining == 0 {
			return nil
		}
		select {
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		case <-deadline.C:
		}
	}
}

func (l *loop) deleteHeartbeatAfterCleanShutdown() {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout(l.lease))
	defer cancel()
	if err := l.queue.DeleteRunnerHeartbeat(cleanupCtx, l.identity.InstanceID); err != nil {
		l.logger.Error("delete runner heartbeat failed", "error", err)
	}
}

func cleanupTimeout(lease time.Duration) time.Duration {
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

func heartbeatInterval(lease time.Duration) time.Duration {
	interval := lease / 4
	if interval <= 0 || interval > time.Second {
		interval = time.Second
	}
	return interval
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
