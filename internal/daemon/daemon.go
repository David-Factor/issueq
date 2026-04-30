// Package daemon coordinates poll, route, and dispatch loops.
package daemon

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"issueq/internal/config"
	"issueq/internal/dispatcher"
	issuegithub "issueq/internal/github"
	"issueq/internal/poller"
	"issueq/internal/router"
	"issueq/internal/store"
)

type Result struct {
	Poll     poller.Result
	Route    router.Result
	Dispatch dispatcher.Result
}

func Once(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client) (Result, error) {
	var result Result
	if gh != nil {
		pollResult, err := poller.Poll(ctx, cfg, gh, queue)
		if err != nil {
			return result, err
		}
		result.Poll = pollResult
	}
	routeResult, err := router.Route(ctx, cfg, queue)
	if err != nil {
		return result, err
	}
	result.Route = routeResult
	heartbeatGrace := cfg.Queue.LeaseDuration.Duration
	if heartbeatGrace <= 0 {
		heartbeatGrace = config.DefaultLeaseDuration
	}
	if _, err := queue.ReleaseExpiredLeases(ctx, time.Now().UTC(), time.Now().UTC().Add(-heartbeatGrace), "", nil); err != nil {
		return result, err
	}
	dispatchResult, err := dispatcher.DispatchWithGitHub(ctx, cfg, queue, gh)
	if err != nil {
		return result, err
	}
	result.Dispatch = dispatchResult
	return result, nil
}

func Run(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	loop := newLoop(cfg, queue, gh, logger)
	return loop.run(ctx)
}

type loop struct {
	cfg        config.Config
	queue      store.QueueStore
	gh         issuegithub.Client
	logger     *slog.Logger
	supervisor *dispatcher.Supervisor
}

func newLoop(cfg config.Config, queue store.QueueStore, gh issuegithub.Client, logger *slog.Logger) *loop {
	return &loop{cfg: cfg, queue: queue, gh: gh, logger: logger, supervisor: dispatcher.NewSupervisor(cfg, queue, gh)}
}

func (l *loop) run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	lease := l.supervisor.LeaseDuration()
	pollInterval := l.cfg.Polling.Interval.Duration
	if pollInterval <= 0 {
		pollInterval = config.DefaultPolling
	}
	heartbeatInterval := dispatcher.HeartbeatInterval(lease)
	reapInterval := heartbeatInterval
	if reapInterval > 200*time.Millisecond {
		reapInterval = 200 * time.Millisecond
	}
	pollTimer := time.NewTimer(0)
	pollCtx, pollCancel := context.WithCancel(ctx)
	defer pollCancel()
	type pollDone struct{}
	pollDoneCh := make(chan pollDone, 1)
	pollRunning := false
	reapTick := make(chan struct{}, 1)
	reapDoneCh := make(chan error, 1)
	reapRunning := false
	requestReap := func() {
		select {
		case reapTick <- struct{}{}:
		default:
		}
	}
	renewDoneCh := make(chan error, 1)
	renewRunning := false
	requestRenew := func() {
		if renewRunning {
			return
		}
		renewRunning = true
		go func() { renewDoneCh <- l.supervisor.Renew(ctx) }()
	}
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	renewTicker := time.NewTicker(heartbeatInterval)
	reapTicker := time.NewTicker(reapInterval)
	defer pollTimer.Stop()
	defer heartbeatTicker.Stop()
	defer renewTicker.Stop()
	defer reapTicker.Stop()

	if err := l.supervisor.Heartbeat(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return l.stop(ctx)
		case <-heartbeatTicker.C:
			if err := l.supervisor.Heartbeat(ctx); err != nil {
				if ctx.Err() != nil {
					return l.stop(ctx)
				}
				l.logger.Error("runner heartbeat failed", "error", err)
			}
		case <-renewTicker.C:
			requestRenew()
		case err := <-renewDoneCh:
			renewRunning = false
			if err != nil {
				if ctx.Err() != nil {
					return l.stop(ctx)
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					_ = l.shutdown(context.Cause(ctx))
					return err
				}
				l.logger.Error("active job lease renewal failed", "error", err)
			}
		case <-reapTicker.C:
			requestReap()
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
			requestReap()
			pollTimer.Reset(pollInterval)
		case <-reapTick:
			if reapRunning {
				continue
			}
			reapRunning = true
			go func() { reapDoneCh <- l.reapReleaseRefill(ctx) }()
		case err := <-reapDoneCh:
			reapRunning = false
			if err != nil {
				if ctx.Err() != nil {
					return l.stop(ctx)
				}
				l.logger.Error("daemon reap/refill failed", "error", err)
			}
		}
	}
}

func (l *loop) stop(ctx context.Context) error {
	l.logger.Info("daemon stopping")
	if err := l.shutdown(context.Cause(ctx)); err == nil {
		l.deleteHeartbeatAfterCleanShutdown()
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
	routeResult, err := router.Route(ctx, l.cfg, l.queue)
	if err != nil {
		l.logger.Error("daemon route failed", "error", err)
		return
	}
	l.logger.Info("daemon poll/route complete", "fetched", pollResult.IssuesFetched, "jobs_created", routeResult.JobsCreated)
}

func (l *loop) reapReleaseRefill(ctx context.Context) error {
	if err := l.supervisor.ReapReady(ctx); err != nil {
		return err
	}
	if _, err := l.supervisor.ReleaseExpiredLeases(ctx); err != nil {
		return err
	}
	if _, err := l.supervisor.PruneStaleHeartbeats(ctx); err != nil {
		return err
	}
	if err := l.supervisor.Refill(ctx); err != nil {
		return err
	}
	if l.supervisor.ActiveCount() == 0 {
		return nil
	}
	return l.supervisor.ReapReady(ctx)
}

func (l *loop) shutdown(cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), dispatcher.CleanupTimeout(l.supervisor.LeaseDuration()))
	defer cancel()
	if err := l.supervisor.Shutdown(cleanupCtx, cause); err != nil {
		l.logger.Error("daemon shutdown cleanup failed", "error", err)
		return err
	}
	return nil
}

func (l *loop) deleteHeartbeatAfterCleanShutdown() {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), dispatcher.CleanupTimeout(l.supervisor.LeaseDuration()))
	defer cancel()
	if err := l.supervisor.DeleteHeartbeat(cleanupCtx); err != nil {
		l.logger.Error("delete runner heartbeat failed", "error", err)
	}
}
