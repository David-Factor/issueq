package daemon

import (
	"context"
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
	interval := cfg.Polling.Interval.Duration
	if interval <= 0 {
		interval = config.DefaultPolling
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, err := Once(ctx, cfg, queue, gh)
		if err != nil {
			logger.Error("daemon cycle failed", "error", err)
		} else {
			logger.Info("daemon cycle complete", "fetched", result.Poll.IssuesFetched, "jobs_created", result.Route.JobsCreated, "claimed", result.Dispatch.Claimed)
		}
		select {
		case <-ctx.Done():
			logger.Info("daemon stopped")
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
