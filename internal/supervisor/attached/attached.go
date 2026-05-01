// Package attached provides a temporary supervisor bridge over the legacy runner.
package attached

import (
	"context"
	"fmt"
	"sync"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/runner"
	"issueq/internal/supervisor"
)

type JobLaunchSpec struct {
	supervisor.LaunchSpec
	Config config.Config
	Route  config.RouteConfig
	Job    model.Job
	Issue  model.IssueSnapshot
	Runner model.RunnerInfo
	Paths  runner.Paths
}

type Supervisor struct {
	mu       sync.Mutex
	nextID   int
	handles  map[string]*runner.Handle
	terminal map[string]supervisor.Observation
}

func New() *Supervisor {
	return &Supervisor{handles: map[string]*runner.Handle{}, terminal: map[string]supervisor.Observation{}}
}

func (s *Supervisor) LaunchJob(ctx context.Context, spec JobLaunchSpec) (supervisor.LaunchRecord, error) {
	handle, err := runner.StartWithPaths(ctx, spec.Config, spec.Route, spec.Job, spec.Issue, spec.Runner, spec.Paths)
	if err != nil {
		return supervisor.LaunchRecord{}, err
	}
	timeout := handle.Timeout
	if timeout <= 0 {
		timeout = spec.Timeout
	}
	s.mu.Lock()
	s.nextID++
	id := fmt.Sprintf("attached-%s-%d", spec.Job.ID, s.nextID)
	s.handles[id] = handle
	s.mu.Unlock()
	return supervisor.LaunchRecord{
		Kind:        supervisor.KindAttached,
		ID:          id,
		JobID:       spec.Job.ID,
		LaunchToken: spec.LaunchToken,
		PID:         handle.PID,
		StartedAt:   handle.StartedAt,
		TimeoutAt:   handle.StartedAt.Add(timeout),
	}, nil
}

func (s *Supervisor) Inspect(ctx context.Context, record supervisor.LaunchRecord) (supervisor.Observation, error) {
	if err := ctx.Err(); err != nil {
		return supervisor.Observation{}, err
	}
	s.mu.Lock()
	if obs, ok := s.terminal[record.ID]; ok {
		s.mu.Unlock()
		return obs, nil
	}
	handle := s.handles[record.ID]
	s.mu.Unlock()
	if handle == nil {
		return supervisor.Observation{State: supervisor.RunUnknown}, nil
	}
	select {
	case <-handle.Done:
	case <-handle.ContextDone:
	default:
		return supervisor.Observation{State: supervisor.RunRunning, StartedAt: handle.StartedAt, ResultPath: handle.Paths.ResultPath, StdoutPath: handle.Paths.StdoutPath, StderrPath: handle.Paths.StderrPath}, nil
	}
	result := runner.Wait(handle)
	obs := ObservationFromResult(result)
	s.mu.Lock()
	delete(s.handles, record.ID)
	s.terminal[record.ID] = obs
	s.mu.Unlock()
	return obs, nil
}

func (s *Supervisor) Cancel(ctx context.Context, record supervisor.LaunchRecord, reason supervisor.CancelReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	handle := s.handles[record.ID]
	s.mu.Unlock()
	if handle == nil {
		return nil
	}
	handle.Cancel(fmt.Errorf("%s", reason))
	return nil
}

func ObservationFromResult(result runner.Result) supervisor.Observation {
	obs := supervisor.Observation{
		ExitCode:    result.ExitCode,
		HasExitCode: result.ExitCode >= 0,
		Error:       result.ErrorString(),
		StartedAt:   result.StartedAt,
		FinishedAt:  result.FinishedAt,
		ResultPath:  result.Paths.ResultPath,
		StdoutPath:  result.Paths.StdoutPath,
		StderrPath:  result.Paths.StderrPath,
	}
	switch {
	case result.Cancelled:
		obs.State = supervisor.RunCancelled
	case result.TimedOut:
		obs.State = supervisor.RunTimedOut
	case result.Error != nil:
		if result.ExitCode >= 0 {
			obs.State = supervisor.RunExited
		} else {
			obs.State = supervisor.RunFailed
		}
	default:
		obs.State = supervisor.RunExited
	}
	return obs
}
