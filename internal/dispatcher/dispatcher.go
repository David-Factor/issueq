// Package dispatcher claims queued jobs and supervises bounded subprocesses.
package dispatcher

import (
	"context"
	"fmt"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/runner"
	"issueq/internal/store"
)

type Result struct {
	Claimed   int
	Succeeded int
	Failed    int
}

func Dispatch(ctx context.Context, cfg config.Config, queue store.QueueStore) (Result, error) {
	runnerInfo := model.RunnerInfo{ID: runnerID(cfg), Name: cfg.Runner.Name}
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
		job, err := queue.ClaimNextJob(ctx, runnerInfo.ID, cfg.Runner.Capabilities, maxGlobal, limits, lease)
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

		runResult := runner.Run(ctx, cfg, route, *job, issue, runnerInfo)
		_ = queue.UpdateJobArtifacts(ctx, job.ID, runResult.Paths.ContextPath, runResult.Paths.ResultPath, runResult.Paths.StdoutPath, runResult.Paths.StderrPath, runResult.PID)
		status := model.JobStatusSucceeded
		lastErr := ""
		if runResult.Error != nil || runResult.ExitCode != 0 {
			status = model.JobStatusFailed
			lastErr = runResult.ErrorString()
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
		} else {
			result.Failed++
		}
	}
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

func runnerID(cfg config.Config) string {
	if cfg.Runner.Name != "" {
		return cfg.Runner.Name
	}
	return "issueq-local"
}
