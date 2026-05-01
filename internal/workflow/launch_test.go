package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"issueq/internal/config"
	"issueq/internal/model"
	"issueq/internal/runner"
	"issueq/internal/store"
	"issueq/internal/supervisor"
)

func TestLaunchClaimedWrapperHappyPathOrderAndFields(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	st := &fakeDurableLaunchStore{}
	sup := &fakeDurableLaunchSupervisor{record: supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: "pid-123", PID: 123, PGID: 456, ProcessStartedAt: now.Add(time.Second), TimeoutAt: now.Add(30 * time.Second)}}
	in := launchInput(t, st, sup, now)

	got, err := LaunchClaimedWrapper(context.Background(), in)
	if err != nil {
		t.Fatalf("LaunchClaimedWrapper error = %v", err)
	}
	if !reflect.DeepEqual(st.calls, []string{"persist-spec", "mark-launching", "persist-record"}) {
		t.Fatalf("store calls = %#v", st.calls)
	}
	if !reflect.DeepEqual(sup.calls, []string{"launch"}) {
		t.Fatalf("supervisor calls = %#v", sup.calls)
	}
	if got.LaunchToken != "tok-1" || !strings.HasSuffix(got.SpecPath, "tok-1-spec.json") || !strings.HasSuffix(got.MetadataPath, "tok-1-run.json") {
		t.Fatalf("result paths/token = %#v", got)
	}
	assertContextFile(t, got.Paths.ContextPath, in.Job.ID, in.Issue.IssueKey, in.RunnerInfo.ID)
	if st.spec.LaunchToken != "tok-1" || st.spec.SupervisorKind != supervisor.KindWrapper || st.spec.LaunchState != model.LaunchStatePreparing || st.spec.LaunchSpecPath != got.SpecPath || st.spec.RunMetadataPath != got.MetadataPath || st.spec.ContextPath != got.Paths.ContextPath || !st.spec.TimeoutAt.Equal(now.Add(30*time.Second)) {
		t.Fatalf("spec record = %#v", st.spec)
	}
	launchSpec := sup.launches[0]
	if launchSpec.JobID != in.Job.ID || launchSpec.LaunchToken != "tok-1" || launchSpec.SpecPath != got.SpecPath || launchSpec.MetadataPath != got.MetadataPath || launchSpec.Timeout != 30*time.Second {
		t.Fatalf("launch spec = %#v", launchSpec)
	}
	if launchSpec.Workdir != "" {
		t.Fatalf("workdir = %q, want inherited cwd", launchSpec.Workdir)
	}
	if !reflect.DeepEqual(launchSpec.Command, []string{"/bin/echo", "hello", got.Paths.ContextPath, got.Paths.ResultPath}) {
		t.Fatalf("command = %#v", launchSpec.Command)
	}
	if !envContains(launchSpec.Env, "ISSUEQ_CONTEXT_PATH="+got.Paths.ContextPath) || envContainsPrefix(launchSpec.Env, "GITHUB_TOKEN=") {
		t.Fatalf("env = %#v", launchSpec.Env)
	}
	if st.record.SupervisorKind != supervisor.KindWrapper || st.record.SupervisorID != "pid-123" || st.record.LaunchToken != "tok-1" || st.record.PID != 123 || st.record.PGID != 456 || st.record.RunMetadataPath != got.MetadataPath || !st.record.TimeoutAt.Equal(now.Add(30*time.Second)) {
		t.Fatalf("launch record = %#v", st.record)
	}
	if len(sup.cancellations) != 0 || len(st.finalizations) != 0 {
		t.Fatalf("unexpected cleanup cancels=%#v finalizations=%#v", sup.cancellations, st.finalizations)
	}
}

func TestLaunchClaimedWrapperDoesNotWriteSpecFile(t *testing.T) {
	st := &fakeDurableLaunchStore{}
	sup := &fakeDurableLaunchSupervisor{record: supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: "pid-123", PID: 123}, statSpecBeforeLaunch: true}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if err != nil {
		t.Fatalf("LaunchClaimedWrapper error = %v", err)
	}
	if sup.specExistedBeforeLaunch {
		t.Fatal("spec file existed before supervisor launch; workflow should not write it")
	}
}

func TestLaunchClaimedWrapperPersistSpecOwnershipLossStopsBeforeLaunch(t *testing.T) {
	st := &fakeDurableLaunchStore{persistSpecErr: store.ErrNotOwner}
	sup := &fakeDurableLaunchSupervisor{}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, store.ErrNotOwner) {
		t.Fatalf("error = %v, want ErrNotOwner", err)
	}
	if !reflect.DeepEqual(st.calls, []string{"persist-spec"}) || len(sup.calls) != 0 || len(sup.cancellations) != 0 {
		t.Fatalf("calls store=%#v sup=%#v cancels=%#v", st.calls, sup.calls, sup.cancellations)
	}
}

func TestLaunchClaimedWrapperMarkLaunchingOwnershipLossStopsBeforeLaunch(t *testing.T) {
	st := &fakeDurableLaunchStore{markErr: store.ErrLostLease}
	sup := &fakeDurableLaunchSupervisor{}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, store.ErrLostLease) {
		t.Fatalf("error = %v, want ErrLostLease", err)
	}
	if !reflect.DeepEqual(st.calls, []string{"persist-spec", "mark-launching"}) || len(sup.calls) != 0 || len(sup.cancellations) != 0 {
		t.Fatalf("calls store=%#v sup=%#v cancels=%#v", st.calls, sup.calls, sup.cancellations)
	}
}

func TestLaunchClaimedWrapperSupervisorLaunchFailureFinalizesIfOwned(t *testing.T) {
	launchErr := errors.New("exec failed")
	st := &fakeDurableLaunchStore{}
	sup := &fakeDurableLaunchSupervisor{launchErr: launchErr}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, launchErr) {
		t.Fatalf("error = %v, want %v", err, launchErr)
	}
	if !reflect.DeepEqual(st.calls, []string{"persist-spec", "mark-launching", "finalize"}) {
		t.Fatalf("store calls = %#v", st.calls)
	}
	if len(st.finalizations) != 1 || st.finalizations[0].Status != model.JobStatusFailed || !strings.Contains(st.finalizations[0].LastError, "wrapper launch failed") {
		t.Fatalf("finalizations = %#v", st.finalizations)
	}
	if len(sup.cancellations) != 0 {
		t.Fatalf("unexpected cancellation = %#v", sup.cancellations)
	}
}

func TestLaunchClaimedWrapperPersistRecordFailureCancelsAndFinalizes(t *testing.T) {
	persistErr := errors.New("disk full")
	st := &fakeDurableLaunchStore{persistRecordErr: persistErr}
	sup := &fakeDurableLaunchSupervisor{record: supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: "pid-123", PID: 123}}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, persistErr) {
		t.Fatalf("error = %v, want %v", err, persistErr)
	}
	if !reflect.DeepEqual(st.calls, []string{"persist-spec", "mark-launching", "persist-record", "finalize"}) {
		t.Fatalf("store calls = %#v", st.calls)
	}
	if len(sup.cancellations) != 1 || sup.cancellations[0].Reason != supervisor.CancelShutdown || sup.cancellations[0].Record.ID != "pid-123" {
		t.Fatalf("cancellations = %#v", sup.cancellations)
	}
	if len(st.finalizations) != 1 || !strings.Contains(st.finalizations[0].LastError, "persist wrapper launch record") {
		t.Fatalf("finalizations = %#v", st.finalizations)
	}
}

func TestLaunchClaimedWrapperPersistRecordOwnershipLossCancelsWithoutFinalize(t *testing.T) {
	st := &fakeDurableLaunchStore{persistRecordErr: store.ErrLostLease}
	sup := &fakeDurableLaunchSupervisor{record: supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: "pid-123", PID: 123}}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, store.ErrLostLease) {
		t.Fatalf("error = %v, want ErrLostLease", err)
	}
	if len(sup.cancellations) != 1 {
		t.Fatalf("cancellations = %#v", sup.cancellations)
	}
	if len(st.finalizations) != 0 {
		t.Fatalf("finalizations = %#v, want none", st.finalizations)
	}
}

func TestLaunchClaimedWrapperCleanupFailureReturnsBothErrors(t *testing.T) {
	persistErr := errors.New("disk full")
	cancelErr := errors.New("cannot cancel")
	st := &fakeDurableLaunchStore{persistRecordErr: persistErr}
	sup := &fakeDurableLaunchSupervisor{record: supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: "pid-123", PID: 123}, cancelErr: cancelErr}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, persistErr) || !errors.Is(err, cancelErr) {
		t.Fatalf("error = %v, want persist and cancel errors", err)
	}
	if len(st.finalizations) != 0 {
		t.Fatalf("finalizations = %#v, want none when cleanup fails", st.finalizations)
	}
}

func TestLaunchClaimedWrapperRejectsUnsafeLaunchToken(t *testing.T) {
	st := &fakeDurableLaunchStore{}
	sup := &fakeDurableLaunchSupervisor{}
	in := launchInput(t, st, sup, time.Now())
	in.NewLaunchToken = func() (string, error) { return "../evil", nil }
	_, err := LaunchClaimedWrapper(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "invalid launch token") {
		t.Fatalf("error = %v, want invalid launch token", err)
	}
	if len(st.calls) != 0 || len(sup.calls) != 0 {
		t.Fatalf("calls store=%#v sup=%#v", st.calls, sup.calls)
	}
}

func TestLaunchClaimedWrapperContextLaunchFailureFinalizesCancelled(t *testing.T) {
	st := &fakeDurableLaunchStore{}
	sup := &fakeDurableLaunchSupervisor{launchErr: context.Canceled}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if len(st.finalizations) != 1 || st.finalizations[0].Status != model.JobStatusCancelled {
		t.Fatalf("finalizations = %#v", st.finalizations)
	}
}

func TestLaunchClaimedWrapperContextPersistRecordFailureFinalizesCancelled(t *testing.T) {
	st := &fakeDurableLaunchStore{persistRecordErr: context.DeadlineExceeded}
	sup := &fakeDurableLaunchSupervisor{record: supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: "pid-123", PID: 123}}
	_, err := LaunchClaimedWrapper(context.Background(), launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if len(sup.cancellations) != 1 {
		t.Fatalf("cancellations = %#v", sup.cancellations)
	}
	if len(st.finalizations) != 1 || st.finalizations[0].Status != model.JobStatusCancelled {
		t.Fatalf("finalizations = %#v", st.finalizations)
	}
}

func TestLaunchClaimedWrapperCompensatesWithFreshContext(t *testing.T) {
	st := &fakeDurableLaunchStore{respectContextOnFinalize: true}
	sup := &fakeDurableLaunchSupervisor{launchErr: context.Canceled}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := LaunchClaimedWrapper(ctx, launchInput(t, st, sup, time.Now()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if len(st.finalizations) != 1 || st.finalizations[0].Status != model.JobStatusCancelled {
		t.Fatalf("finalizations = %#v", st.finalizations)
	}
}

func launchInput(t *testing.T, store *fakeDurableLaunchStore, sup *fakeDurableLaunchSupervisor, now time.Time) LaunchClaimedWrapperInput {
	t.Helper()
	return LaunchClaimedWrapperInput{
		Config: config.Config{
			Workdir: config.WorkdirConfig{Path: t.TempDir()},
			GitHub:  config.GitHubConfig{Host: "github.com", Owner: "owner", Repo: "repo", TokenEnv: "GITHUB_TOKEN"},
			Runner:  config.RunnerConfig{Env: config.EnvConfig{Inherit: true, Pass: []string{"PATH", "GITHUB_TOKEN"}}},
		},
		Route:          config.RouteConfig{Name: "code", Job: config.JobConfig{Kind: "code", Command: config.Command{"/bin/echo", "hello"}, Timeout: config.Duration{Duration: 30 * time.Second}, MaxAttempts: 3}},
		Job:            model.Job{ID: "job-1", IssueKey: "issue-1", RouteName: "code", Kind: "code", Status: model.JobStatusRunning, Attempts: 2},
		Issue:          model.IssueSnapshot{IssueKey: "issue-1", Host: "github.com", Owner: "owner", Repo: "repo", Number: 1},
		Identity:       model.RunnerIdentity{RunnerID: "runner", InstanceID: "runner-instance"},
		RunnerInfo:     model.RunnerInfo{ID: "runner", Name: "Runner"},
		Store:          store,
		Supervisor:     sup,
		NewLaunchToken: func() (string, error) { return "tok-1", nil },
		Now:            func() time.Time { return now },
		CleanupTimeout: 10 * time.Millisecond,
	}
}

func assertContextFile(t *testing.T, path, wantJobID, wantIssueKey, wantRunnerID string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read context: %v", err)
	}
	var ctxData runner.Context
	if err := json.Unmarshal(data, &ctxData); err != nil {
		t.Fatalf("parse context: %v", err)
	}
	if ctxData.Job.ID != wantJobID || ctxData.Job.Attempt != 2 || ctxData.Issue.IssueKey != wantIssueKey || ctxData.Runner.ID != wantRunnerID {
		t.Fatalf("context = %#v", ctxData)
	}
}

func envContains(env []string, want string) bool {
	for _, pair := range env {
		if pair == want {
			return true
		}
	}
	return false
}

func envContainsPrefix(env []string, prefix string) bool {
	for _, pair := range env {
		if strings.HasPrefix(pair, prefix) {
			return true
		}
	}
	return false
}

type fakeDurableLaunchStore struct {
	calls                    []string
	spec                     model.LaunchSpecRecord
	record                   model.LaunchRecord
	finalizations            []model.JobFinalize
	persistSpecErr           error
	markErr                  error
	persistRecordErr         error
	finalizeErr              error
	respectContextOnFinalize bool
}

func (f *fakeDurableLaunchStore) PersistLaunchSpecOwned(ctx context.Context, jobID, runnerInstanceID string, record model.LaunchSpecRecord) error {
	f.calls = append(f.calls, "persist-spec")
	f.spec = record
	return f.persistSpecErr
}

func (f *fakeDurableLaunchStore) MarkJobLaunchingOwned(ctx context.Context, jobID, runnerInstanceID, launchToken string) error {
	f.calls = append(f.calls, "mark-launching")
	return f.markErr
}

func (f *fakeDurableLaunchStore) PersistLaunchRecordOwned(ctx context.Context, jobID, runnerInstanceID string, record model.LaunchRecord) error {
	f.calls = append(f.calls, "persist-record")
	f.record = record
	return f.persistRecordErr
}

func (f *fakeDurableLaunchStore) FinalizeJobOwned(ctx context.Context, jobID, runnerInstanceID string, result model.JobFinalize) error {
	f.calls = append(f.calls, "finalize")
	f.finalizations = append(f.finalizations, result)
	if f.respectContextOnFinalize {
		return ctx.Err()
	}
	return f.finalizeErr
}

type fakeDurableLaunchSupervisor struct {
	calls                   []string
	launches                []supervisor.LaunchSpec
	record                  supervisor.LaunchRecord
	launchErr               error
	cancelErr               error
	cancellations           []supervisor.FakeCancellation
	statSpecBeforeLaunch    bool
	specExistedBeforeLaunch bool
}

func (f *fakeDurableLaunchSupervisor) Launch(ctx context.Context, spec supervisor.LaunchSpec) (supervisor.LaunchRecord, error) {
	f.calls = append(f.calls, "launch")
	f.launches = append(f.launches, spec)
	if f.statSpecBeforeLaunch {
		_, err := os.Stat(spec.SpecPath)
		f.specExistedBeforeLaunch = err == nil
	}
	if f.launchErr != nil {
		return supervisor.LaunchRecord{}, f.launchErr
	}
	record := f.record
	if record.Kind == "" {
		record.Kind = supervisor.KindWrapper
	}
	if record.JobID == "" {
		record.JobID = spec.JobID
	}
	if record.LaunchToken == "" {
		record.LaunchToken = spec.LaunchToken
	}
	if record.MetadataPath == "" {
		record.MetadataPath = spec.MetadataPath
	}
	return record, nil
}

func (f *fakeDurableLaunchSupervisor) Cancel(ctx context.Context, record supervisor.LaunchRecord, reason supervisor.CancelReason) error {
	f.calls = append(f.calls, "cancel")
	f.cancellations = append(f.cancellations, supervisor.FakeCancellation{Record: record, Reason: reason})
	return f.cancelErr
}
