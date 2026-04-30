// Package dispatcher claims queued jobs and supervises bounded subprocesses.
package dispatcher

import (
	"context"
	"fmt"

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
	return DispatchWithGitHub(ctx, cfg, queue, nil)
}

func DispatchWithGitHub(ctx context.Context, cfg config.Config, queue store.QueueStore, gh issuegithub.Client) (Result, error) {
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
		_, _ = actions.Apply(ctx, cfg, gh, queue, issue, cfg.Workflow.OnTransitionsExceeded)
	}
	return true, nil
}
