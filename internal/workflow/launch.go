package workflow

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/runner"
	"issueq/internal/supervisor"

	"github.com/oklog/ulid/v2"
)

type DurableLaunchStore interface {
	PersistLaunchSpecOwned(ctx context.Context, jobID, runnerInstanceID string, record model.LaunchSpecRecord) error
	MarkJobLaunchingOwned(ctx context.Context, jobID, runnerInstanceID, launchToken string) error
	PersistLaunchRecordOwned(ctx context.Context, jobID, runnerInstanceID string, record model.LaunchRecord) error
	FinalizeJobOwned(ctx context.Context, jobID, runnerInstanceID string, result model.JobFinalize) error
}

type DurableLaunchSupervisor interface {
	Launch(context.Context, supervisor.LaunchSpec) (supervisor.LaunchRecord, error)
	Cancel(context.Context, supervisor.LaunchRecord, supervisor.CancelReason) error
}

type LaunchTokenFunc func() (string, error)

type LaunchClaimedWrapperInput struct {
	Config     config.Config
	Route      config.RouteConfig
	Job        model.Job
	Issue      model.IssueSnapshot
	Identity   model.RunnerIdentity
	RunnerInfo model.RunnerInfo

	Store      DurableLaunchStore
	Supervisor DurableLaunchSupervisor

	NewLaunchToken LaunchTokenFunc
	Now            func() time.Time
	CleanupTimeout time.Duration
}

type LaunchClaimedWrapperResult struct {
	LaunchToken  string
	Paths        runner.Paths
	SpecPath     string
	MetadataPath string
	Spec         supervisor.LaunchSpec
	Record       supervisor.LaunchRecord
}

func LaunchClaimedWrapper(ctx context.Context, in LaunchClaimedWrapperInput) (LaunchClaimedWrapperResult, error) {
	if in.Store == nil {
		return LaunchClaimedWrapperResult{}, errors.New("store is required")
	}
	if in.Supervisor == nil {
		return LaunchClaimedWrapperResult{}, errors.New("supervisor is required")
	}
	if in.Identity.InstanceID == "" {
		return LaunchClaimedWrapperResult{}, errors.New("runner instance id is required")
	}
	if in.Job.ID == "" {
		return LaunchClaimedWrapperResult{}, errors.New("job id is required")
	}
	if len(in.Route.Job.Command) == 0 || in.Route.Job.Command[0] == "" {
		return LaunchClaimedWrapperResult{}, errors.New("job command is empty")
	}
	now := launchNow(in)
	token, err := launchToken(in)
	if err != nil {
		return LaunchClaimedWrapperResult{}, err
	}
	paths, err := absolutePaths(runner.PreparePaths(in.Config.Workdir.Path, in.Job.ID))
	if err != nil {
		return LaunchClaimedWrapperResult{}, fmt.Errorf("prepare wrapper paths: %w", err)
	}
	specPath := filepath.Join(paths.Dir, token+"-spec.json")
	metadataPath := filepath.Join(paths.Dir, token+"-run.json")
	timeout := in.Route.Job.Timeout.Duration
	if timeout <= 0 {
		timeout = config.DefaultLeaseDuration
	}
	ctxData := runner.Context{
		Issue: in.Issue,
		Job: runner.JobContext{
			ID:          in.Job.ID,
			Route:       in.Job.RouteName,
			Kind:        in.Job.Kind,
			Attempt:     launchAttempt(in.Job.Attempts),
			MaxAttempts: in.Route.Job.MaxAttempts,
		},
		Runner: in.RunnerInfo,
	}
	if err := runner.WriteContext(paths, ctxData); err != nil {
		finalizeCtx, cancel := launchCompensationContext(in)
		_ = finalizeLaunchTerminal(finalizeCtx, in.Store, in.Job, in.Identity.InstanceID, model.JobStatusFailed, fmt.Sprintf("prepare wrapper launch: %v", err), now)
		cancel()
		return LaunchClaimedWrapperResult{}, fmt.Errorf("prepare wrapper launch: %w", err)
	}
	command := append([]string(nil), in.Route.Job.Command...)
	command = append(command, paths.ContextPath, paths.ResultPath)
	spec := supervisor.LaunchSpec{
		JobID:        in.Job.ID,
		LaunchToken:  token,
		Command:      command,
		Env:          runner.BuildEnv(in.Config, in.Route, in.Job, in.Issue, paths),
		Workdir:      "",
		ContextPath:  paths.ContextPath,
		ResultPath:   paths.ResultPath,
		StdoutPath:   paths.StdoutPath,
		StderrPath:   paths.StderrPath,
		MetadataPath: metadataPath,
		Timeout:      timeout,
		SpecPath:     specPath,
	}
	result := LaunchClaimedWrapperResult{LaunchToken: token, Paths: paths, SpecPath: specPath, MetadataPath: metadataPath, Spec: spec}
	if err := in.Store.PersistLaunchSpecOwned(ctx, in.Job.ID, in.Identity.InstanceID, model.LaunchSpecRecord{
		SupervisorKind:  supervisor.KindWrapper,
		LaunchToken:     token,
		LaunchState:     model.LaunchStatePreparing,
		LaunchSpecPath:  specPath,
		ContextPath:     paths.ContextPath,
		ResultPath:      paths.ResultPath,
		StdoutPath:      paths.StdoutPath,
		StderrPath:      paths.StderrPath,
		RunMetadataPath: metadataPath,
		TimeoutAt:       now.Add(timeout),
	}); err != nil {
		return result, fmt.Errorf("persist wrapper launch spec: %w", err)
	}
	if err := in.Store.MarkJobLaunchingOwned(ctx, in.Job.ID, in.Identity.InstanceID, token); err != nil {
		return result, fmt.Errorf("mark wrapper job launching: %w", err)
	}
	record, err := in.Supervisor.Launch(ctx, spec)
	if err != nil {
		status := model.JobStatusFailed
		message := fmt.Sprintf("wrapper launch failed: %v", err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = model.JobStatusCancelled
			message = err.Error()
		}
		finalizeCtx, cancel := launchCompensationContext(in)
		_ = finalizeLaunchTerminal(finalizeCtx, in.Store, in.Job, in.Identity.InstanceID, status, message, now)
		cancel()
		return result, fmt.Errorf("wrapper launch failed: %w", err)
	}
	result.Record = record
	persist := model.LaunchRecord{
		SupervisorKind:   supervisor.KindWrapper,
		SupervisorID:     record.ID,
		LaunchToken:      token,
		PID:              record.PID,
		PGID:             record.PGID,
		ProcessStartedAt: record.ProcessStartedAt,
		RunMetadataPath:  metadataPath,
		LaunchSpecPath:   specPath,
		ContextPath:      paths.ContextPath,
		ResultPath:       paths.ResultPath,
		StdoutPath:       paths.StdoutPath,
		StderrPath:       paths.StderrPath,
		TimeoutAt:        record.TimeoutAt,
	}
	if persist.TimeoutAt.IsZero() {
		persist.TimeoutAt = now.Add(timeout)
	}
	if err := in.Store.PersistLaunchRecordOwned(ctx, in.Job.ID, in.Identity.InstanceID, persist); err != nil {
		cleanupErr := cleanupLaunchedWrapper(in, record)
		if IsOwnershipLoss(err) {
			return result, errors.Join(fmt.Errorf("persist wrapper launch record: %w", err), cleanupErr)
		}
		if cleanupErr != nil {
			return result, errors.Join(fmt.Errorf("persist wrapper launch record: %w", err), cleanupErr)
		}
		status := model.JobStatusFailed
		message := fmt.Sprintf("persist wrapper launch record: %v", err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = model.JobStatusCancelled
			message = err.Error()
		}
		finalizeCtx, cancel := launchCompensationContext(in)
		_ = finalizeLaunchTerminal(finalizeCtx, in.Store, in.Job, in.Identity.InstanceID, status, message, now)
		cancel()
		return result, fmt.Errorf("persist wrapper launch record: %w", err)
	}
	return result, nil
}

func launchCompensationContext(in LaunchClaimedWrapperInput) (context.Context, context.CancelFunc) {
	timeout := in.CleanupTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

func launchNow(in LaunchClaimedWrapperInput) time.Time {
	if in.Now != nil {
		return in.Now().UTC()
	}
	return time.Now().UTC()
}

func launchToken(in LaunchClaimedWrapperInput) (string, error) {
	if in.NewLaunchToken != nil {
		token, err := in.NewLaunchToken()
		if err != nil {
			return "", err
		}
		if !validLaunchToken(token) {
			return "", fmt.Errorf("invalid launch token %q", token)
		}
		return token, nil
	}
	entropy := ulid.Monotonic(rand.Reader, 0)
	id, err := ulid.New(ulid.Timestamp(time.Now().UTC()), entropy)
	if err != nil {
		return "", fmt.Errorf("generate launch token: %w", err)
	}
	return "launch_" + id.String(), nil
}

var launchTokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

func validLaunchToken(token string) bool {
	return launchTokenPattern.MatchString(token) && filepath.Base(token) == token
}

func absolutePaths(paths runner.Paths) (runner.Paths, error) {
	var err error
	paths.Dir, err = filepath.Abs(paths.Dir)
	if err != nil {
		return runner.Paths{}, err
	}
	paths.ContextPath, err = filepath.Abs(paths.ContextPath)
	if err != nil {
		return runner.Paths{}, err
	}
	paths.ResultPath, err = filepath.Abs(paths.ResultPath)
	if err != nil {
		return runner.Paths{}, err
	}
	paths.StdoutPath, err = filepath.Abs(paths.StdoutPath)
	if err != nil {
		return runner.Paths{}, err
	}
	paths.StderrPath, err = filepath.Abs(paths.StderrPath)
	if err != nil {
		return runner.Paths{}, err
	}
	return paths, nil
}

func launchAttempt(attempts int) int {
	if attempts < 1 {
		return 1
	}
	return attempts
}

func finalizeLaunchTerminal(ctx context.Context, queue DurableLaunchStore, job model.Job, runnerInstanceID string, status string, message string, now time.Time) error {
	dropped, err := DropOnOwnershipLoss(queue.FinalizeJobOwned(ctx, job.ID, runnerInstanceID, model.JobFinalize{Status: status, LastError: message, FinishedAt: now.UTC()}))
	if dropped {
		return nil
	}
	return err
}

func cleanupLaunchedWrapper(in LaunchClaimedWrapperInput, record supervisor.LaunchRecord) error {
	timeout := in.CleanupTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := in.Supervisor.Cancel(ctx, record, supervisor.CancelShutdown); err != nil {
		return fmt.Errorf("cancel wrapper after launch-record persistence failure: %w", err)
	}
	return nil
}
