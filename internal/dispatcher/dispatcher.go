// Package dispatcher runs bounded durable wrapper dispatch waves.
package dispatcher

import (
	"context"
	"fmt"
	"os"
	"time"

	"issueq/internal/config"
	issuegithub "issueq/internal/github"
	"issueq/internal/model"
	"issueq/internal/store"
	"issueq/internal/supervisor"
	"issueq/internal/supervisor/wrapper"
	"issueq/internal/workflow"
)

type Result struct {
	Claimed   int
	Succeeded int
	Failed    int
	Skipped   int
	Dead      int
}

func Dispatch(ctx context.Context, cfg config.Config, queue store.QueueStore) (Result, error) {
	return DispatchWithGitHub(ctx, cfg, queue, nil)
}

func DispatchWithGitHub(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client) (Result, error) {
	return DispatchWithSupervisor(ctx, cfg, queue, gh, wrapper.New(""))
}

func DispatchWithSupervisor(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, backend supervisor.Supervisor) (Result, error) {
	wave := NewWave(cfg, queue, gh, backend)
	if err := wave.Preflight(ctx); err != nil {
		return Result{}, err
	}
	return wave.RunFrontier(ctx)
}

type Wave struct {
	cfg        config.Config
	queue      store.QueueStore
	gh         issuegithub.Client
	backend    supervisor.Supervisor
	identity   model.RunnerIdentity
	runnerInfo model.RunnerInfo
	lease      time.Duration
	maxGlobal  int
	limits     map[string]int
	processID  int
	result     Result
	active     map[string]waveActiveJob
}

type waveActiveJob struct {
	job   model.Job
	issue model.IssueSnapshot
	route config.RouteConfig
}

func NewWave(cfg config.Config, queue store.QueueStore, gh issuegithub.Client, backend supervisor.Supervisor) *Wave {
	identity := newRunnerIdentity(cfg)
	lease := cfg.Queue.LeaseDuration.Duration
	if lease <= 0 {
		lease = config.DefaultLeaseDuration
	}
	maxGlobal := cfg.Queue.MaxGlobalConcurrency
	if maxGlobal <= 0 {
		maxGlobal = 1
	}
	return &Wave{cfg: cfg, queue: queue, gh: gh, backend: backend, identity: identity, runnerInfo: model.RunnerInfo{ID: identity.RunnerID, Name: cfg.Runner.Name}, lease: lease, maxGlobal: maxGlobal, limits: perRouteLimits(cfg), processID: os.Getpid(), active: map[string]waveActiveJob{}}
}

func (w *Wave) Preflight(ctx context.Context) error {
	if w.backend == nil {
		return fmt.Errorf("supervisor is required")
	}
	if err := workflow.HeartbeatRunner(ctx, w.queue, w.identity, w.processID, time.Now().UTC()); err != nil {
		return err
	}
	now := time.Now().UTC()
	if _, err := workflow.RecoverExpiredLeases(ctx, w.queue, now, w.lease, w.identity.InstanceID, nil); err != nil {
		return err
	}
	_, err := workflow.PruneStaleHeartbeats(ctx, w.queue, now.Add(-w.lease))
	return err
}

func (w *Wave) RunFrontier(ctx context.Context) (Result, error) {
	frontier, err := w.queue.ListEligibleJobIDs(ctx, time.Now().UTC())
	if err != nil {
		return w.result, err
	}
	frontierRemaining := stringSet(frontier)
	for len(frontierRemaining) > 0 || len(w.active) > 0 {
		if err := workflow.HeartbeatRunner(ctx, w.queue, w.identity, w.processID, time.Now().UTC()); err != nil && len(w.active) == 0 {
			return w.result, err
		}
		if err := w.reapRenewActive(ctx); err != nil {
			_ = w.cancelActive(context.Background(), err)
			return w.result, err
		}
		if err := w.refill(ctx, frontierRemaining); err != nil {
			_ = w.cancelActive(context.Background(), err)
			return w.result, err
		}
		if len(w.active) == 0 {
			return w.result, nil
		}
		select {
		case <-ctx.Done():
			cleanupCtx, cancel := context.WithTimeout(context.Background(), CleanupTimeout(w.lease))
			_ = w.cancelActive(cleanupCtx, context.Cause(ctx))
			cancel()
			return w.result, ctx.Err()
		case <-time.After(activePollInterval(w.lease)):
		}
	}
	return w.result, nil
}

func (w *Wave) refill(ctx context.Context, frontierRemaining map[string]struct{}) error {
	for len(w.active) < w.maxGlobal && len(frontierRemaining) > 0 {
		job, err := workflow.ClaimOneInFrontier(ctx, w.cfg, w.queue, w.identity, w.lease, w.limits, keys(frontierRemaining))
		if err != nil {
			return err
		}
		if job == nil {
			return nil
		}
		delete(frontierRemaining, job.ID)
		w.result.Claimed++
		route, ok := workflow.RouteByName(w.cfg, job.RouteName)
		if !ok {
			if finalized, err := workflow.FinalizeClaimedJobOwned(ctx, w.queue, *job, w.identity.InstanceID, model.JobStatusFailed, "route not found"); err != nil {
				return err
			} else if finalized {
				w.result.Failed++
			}
			continue
		}
		prepared, err := workflow.PrepareClaimedWrapperLaunch(ctx, workflow.PrepareClaimedWrapperInput{Config: w.cfg, Queue: w.queue, GitHub: w.gh, Identity: w.identity, Job: job, Route: route, Lease: w.lease})
		if err != nil {
			return err
		}
		w.countPrepare(prepared)
		if prepared.Outcome != workflow.PrepareLaunch {
			continue
		}
		if _, err := workflow.LaunchClaimedWrapper(ctx, workflow.LaunchClaimedWrapperInput{Config: w.cfg, Route: route, Job: *job, Issue: prepared.Issue, Identity: w.identity, RunnerInfo: w.runnerInfo, Store: w.queue, Supervisor: w.backend, CleanupTimeout: CleanupTimeout(w.lease)}); err != nil {
			if workflow.IsOwnershipLoss(err) {
				continue
			}
			return err
		}
		w.active[job.ID] = waveActiveJob{job: *job, issue: prepared.Issue, route: route}
	}
	return nil
}

func (w *Wave) countPrepare(prepared workflow.PrepareClaimedWrapperResult) {
	if !prepared.Finalized {
		return
	}
	switch prepared.Status {
	case model.JobStatusSkipped:
		w.result.Skipped++
	case model.JobStatusDead:
		w.result.Dead++
	case model.JobStatusSucceeded:
		w.result.Succeeded++
	default:
		w.result.Failed++
	}
}

func (w *Wave) reapRenewActive(ctx context.Context) error {
	if len(w.active) == 0 {
		return nil
	}
	jobs, err := w.queue.ListOwnedRunningJobs(ctx, w.identity.InstanceID)
	if err != nil {
		return err
	}
	owned := map[string]model.Job{}
	for _, job := range jobs {
		owned[job.ID] = job
	}
	for id := range w.active {
		job, ok := owned[id]
		if !ok {
			delete(w.active, id)
			continue
		}
		obs, err := workflow.InspectDurableJob(ctx, w.backend, job)
		if err != nil {
			return err
		}
		switch workflow.ObservationToDecision(obs) {
		case workflow.DecisionKeepRunning:
			if err := w.queue.RenewJobLease(ctx, job.ID, w.identity.InstanceID, w.lease); err != nil {
				if workflow.IsOwnershipLoss(err) {
					delete(w.active, id)
					continue
				}
				return err
			}
		case workflow.DecisionUnknown:
			if err := w.queue.RenewJobLease(ctx, job.ID, w.identity.InstanceID, w.lease); err != nil && !workflow.IsOwnershipLoss(err) {
				return err
			}
		case workflow.DecisionSucceeded, workflow.DecisionFailed, workflow.DecisionCancelled:
			finalized, err := workflow.FinalizeOwnedObservationWithLifecycle(ctx, workflow.FinalizeObservationLifecycleInput{Config: w.cfg, Queue: w.queue, GitHub: w.gh, Identity: w.identity, Job: job, Obs: obs, Lease: w.lease, Now: time.Now().UTC()})
			if err != nil {
				return err
			}
			if finalized.Finalized {
				w.countFinalized(finalized.Status)
			}
			delete(w.active, id)
		}
	}
	return nil
}

func (w *Wave) countFinalized(status string) {
	switch status {
	case model.JobStatusSucceeded:
		w.result.Succeeded++
	case model.JobStatusSkipped:
		w.result.Skipped++
	case model.JobStatusDead:
		w.result.Dead++
	case model.JobStatusCancelled:
		// Cancellation is terminal but is not counted as command failure.
	default:
		w.result.Failed++
	}
}

func (w *Wave) cancelActive(ctx context.Context, cause error) error {
	if cause == nil {
		cause = context.Canceled
	}
	for len(w.active) > 0 {
		jobs, err := w.queue.ListOwnedRunningJobs(ctx, w.identity.InstanceID)
		if err != nil {
			return err
		}
		owned := map[string]model.Job{}
		for _, job := range jobs {
			owned[job.ID] = job
		}
		progress := false
		for id := range w.active {
			job, ok := owned[id]
			if !ok {
				delete(w.active, id)
				progress = true
				continue
			}
			record, ok, _ := workflow.DurableLaunchRecord(job)
			if !ok {
				if finalized, err := workflow.FinalizeClaimedJobOwned(ctx, w.queue, job, w.identity.InstanceID, model.JobStatusCancelled, "runner shutting down"); err != nil {
					return err
				} else if finalized {
					delete(w.active, id)
					progress = true
				}
				continue
			}
			if err := w.backend.Cancel(ctx, record, supervisor.CancelShutdown); err != nil && ctx.Err() == nil {
				return err
			}
			obs, err := workflow.InspectDurableJob(ctx, w.backend, job)
			if err != nil {
				return err
			}
			finalized, err := workflow.FinalizeOwnedObservationWithLifecycle(ctx, workflow.FinalizeObservationLifecycleInput{Config: w.cfg, Queue: w.queue, GitHub: w.gh, Identity: w.identity, Job: job, Obs: obs, Lease: w.lease, Now: time.Now().UTC()})
			if err != nil {
				return err
			}
			if finalized.Finalized || finalized.OwnershipLost {
				delete(w.active, id)
				progress = true
			}
		}
		if !progress {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	return nil
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

func activePollInterval(lease time.Duration) time.Duration {
	interval := HeartbeatInterval(lease)
	if interval > 50*time.Millisecond {
		interval = 50 * time.Millisecond
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
