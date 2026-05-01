package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"issueq/internal/model"
	"issueq/internal/store"
	"issueq/internal/supervisor"
)

func TestFinalizeOwnedObservationTerminalStates(t *testing.T) {
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	finished := now.Add(-time.Minute)
	tests := []struct {
		name          string
		obs           supervisor.Observation
		wantStatus    string
		wantLastError string
	}{
		{name: "success", obs: terminalObservation(supervisor.RunExited, 0, true, "", finished), wantStatus: model.JobStatusSucceeded},
		{name: "nonzero exit", obs: terminalObservation(supervisor.RunExited, 2, true, "", finished), wantStatus: model.JobStatusFailed, wantLastError: "exit code 2"},
		{name: "failed", obs: terminalObservation(supervisor.RunFailed, -1, false, "wrapper failed", finished), wantStatus: model.JobStatusFailed, wantLastError: "wrapper failed"},
		{name: "timed out", obs: terminalObservation(supervisor.RunTimedOut, -1, false, "", finished), wantStatus: model.JobStatusFailed, wantLastError: "run timed out"},
		{name: "cancelled", obs: terminalObservation(supervisor.RunCancelled, -1, false, "", finished), wantStatus: model.JobStatusCancelled, wantLastError: "run cancelled"},
		{name: "uses now when finished missing", obs: terminalObservation(supervisor.RunExited, 0, true, "", time.Time{}), wantStatus: model.JobStatusSucceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := &fakeObservationFinalizer{}
			job := durableWrapperJob("job-1")
			got, err := FinalizeOwnedObservation(context.Background(), queue, model.RunnerIdentity{InstanceID: "runner-1"}, job, tt.obs, now)
			if err != nil {
				t.Fatalf("FinalizeOwnedObservation error = %v", err)
			}
			if !got.Finalized || got.OwnershipLost || got.Status != tt.wantStatus {
				t.Fatalf("finalization = %#v", got)
			}
			if len(queue.calls) != 1 {
				t.Fatalf("finalize calls = %d, want 1", len(queue.calls))
			}
			call := queue.calls[0]
			if call.jobID != job.ID || call.runnerInstanceID != "runner-1" {
				t.Fatalf("call identity = %#v", call)
			}
			if call.result.Status != tt.wantStatus || call.result.ResultPath != tt.obs.ResultPath || call.result.StdoutPath != tt.obs.StdoutPath || call.result.StderrPath != tt.obs.StderrPath {
				t.Fatalf("finalize result = %#v", call.result)
			}
			if call.result.LastError != tt.wantLastError {
				t.Fatalf("LastError = %q, want %q", call.result.LastError, tt.wantLastError)
			}
			wantFinished := tt.obs.FinishedAt
			if wantFinished.IsZero() {
				wantFinished = now
			}
			if !call.result.FinishedAt.Equal(wantFinished) {
				t.Fatalf("FinishedAt = %v, want %v", call.result.FinishedAt, wantFinished)
			}
		})
	}
}

func TestFinalizeOwnedObservationLeavesNonTerminalUntouched(t *testing.T) {
	tests := []supervisor.Observation{
		{State: supervisor.RunStarting},
		{State: supervisor.RunRunning},
		{State: supervisor.RunUnknown, Error: "missing metadata"},
	}
	for _, obs := range tests {
		t.Run(string(obs.State), func(t *testing.T) {
			queue := &fakeObservationFinalizer{}
			got, err := FinalizeOwnedObservation(context.Background(), queue, model.RunnerIdentity{InstanceID: "runner-1"}, durableWrapperJob("job-1"), obs, time.Now())
			if err != nil {
				t.Fatalf("FinalizeOwnedObservation error = %v", err)
			}
			if got.Finalized || got.OwnershipLost {
				t.Fatalf("finalization = %#v", got)
			}
			if len(queue.calls) != 0 {
				t.Fatalf("finalize calls = %d, want 0", len(queue.calls))
			}
		})
	}
}

func TestFinalizeOwnedObservationSuppressesOwnershipLoss(t *testing.T) {
	for _, err := range []error{store.ErrNotOwner, store.ErrLostLease} {
		t.Run(err.Error(), func(t *testing.T) {
			queue := &fakeObservationFinalizer{err: err}
			got, gotErr := FinalizeOwnedObservation(context.Background(), queue, model.RunnerIdentity{InstanceID: "runner-1"}, durableWrapperJob("job-1"), terminalObservation(supervisor.RunExited, 0, true, "", time.Now()), time.Now())
			if gotErr != nil {
				t.Fatalf("FinalizeOwnedObservation error = %v", gotErr)
			}
			if !got.OwnershipLost || got.Finalized {
				t.Fatalf("finalization = %#v", got)
			}
		})
	}
}

func TestFinalizeOwnedObservationPropagatesStoreErrors(t *testing.T) {
	wantErr := errors.New("sqlite locked")
	_, err := FinalizeOwnedObservation(context.Background(), &fakeObservationFinalizer{err: wantErr}, model.RunnerIdentity{InstanceID: "runner-1"}, durableWrapperJob("job-1"), terminalObservation(supervisor.RunExited, 0, true, "", time.Now()), time.Now())
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestFinalizeOwnedObservationSkipsNonWrapperOrIncompleteRows(t *testing.T) {
	base := durableWrapperJob("job-1")
	tests := []struct {
		name string
		edit func(*model.Job)
	}{
		{name: "attached", edit: func(j *model.Job) { j.SupervisorKind = "attached" }},
		{name: "missing launch token", edit: func(j *model.Job) { j.LaunchToken = "" }},
		{name: "missing durable evidence", edit: func(j *model.Job) { j.SupervisorID = ""; j.PID = 0; j.RunMetadataPath = "" }},
		{name: "non running", edit: func(j *model.Job) { j.Status = model.JobStatusPending }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := base
			tt.edit(&job)
			queue := &fakeObservationFinalizer{}
			got, err := FinalizeOwnedObservation(context.Background(), queue, model.RunnerIdentity{InstanceID: "runner-1"}, job, terminalObservation(supervisor.RunExited, 0, true, "", time.Now()), time.Now())
			if err != nil {
				t.Fatalf("FinalizeOwnedObservation error = %v", err)
			}
			if got.Finalized || got.Decision != DecisionUnknown {
				t.Fatalf("finalization = %#v", got)
			}
			if len(queue.calls) != 0 {
				t.Fatalf("finalize calls = %d, want 0", len(queue.calls))
			}
		})
	}
}

func TestFinalizeOwnedObservationsSummaryCounts(t *testing.T) {
	queue := &fakeObservationFinalizer{errByJobID: map[string]error{"job-6": store.ErrLostLease}}
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	items := []JobObservation{
		{Job: durableWrapperJob("job-1"), Observation: terminalObservation(supervisor.RunExited, 0, true, "", now)},
		{Job: durableWrapperJob("job-2"), Observation: terminalObservation(supervisor.RunExited, 7, true, "", now)},
		{Job: durableWrapperJob("job-3"), Observation: terminalObservation(supervisor.RunCancelled, -1, false, "", now)},
		{Job: durableWrapperJob("job-4"), Observation: supervisor.Observation{State: supervisor.RunRunning}},
		{Job: durableWrapperJob("job-5"), Observation: supervisor.Observation{State: supervisor.RunUnknown}},
		{Job: durableWrapperJob("job-6"), Observation: terminalObservation(supervisor.RunExited, 0, true, "", now)},
	}

	summary, err := FinalizeOwnedObservations(context.Background(), queue, model.RunnerIdentity{InstanceID: "runner-1"}, items, now)
	if err != nil {
		t.Fatalf("FinalizeOwnedObservations error = %v", err)
	}
	if summary.Observed != 6 || summary.Finalized != 3 || summary.Succeeded != 1 || summary.Failed != 1 || summary.Cancelled != 1 || summary.KeptRunning != 1 || summary.Unknown != 1 || summary.OwnershipLost != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestFinalizeOwnedObservationsWrapsStoreErrorWithJobID(t *testing.T) {
	wantErr := errors.New("disk full")
	_, err := FinalizeOwnedObservations(context.Background(), &fakeObservationFinalizer{errByJobID: map[string]error{"job-2": wantErr}}, model.RunnerIdentity{InstanceID: "runner-1"}, []JobObservation{
		{Job: durableWrapperJob("job-1"), Observation: terminalObservation(supervisor.RunExited, 0, true, "", time.Now())},
		{Job: durableWrapperJob("job-2"), Observation: terminalObservation(supervisor.RunExited, 0, true, "", time.Now())},
	}, time.Now())
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if !strings.Contains(err.Error(), "job-2") {
		t.Fatalf("error %q does not include job id", err.Error())
	}
}

func terminalObservation(state supervisor.RunState, exitCode int, hasExitCode bool, errMsg string, finished time.Time) supervisor.Observation {
	return supervisor.Observation{
		State:       state,
		ExitCode:    exitCode,
		HasExitCode: hasExitCode,
		Error:       errMsg,
		FinishedAt:  finished,
		ResultPath:  "/tmp/result.json",
		StdoutPath:  "/tmp/stdout.log",
		StderrPath:  "/tmp/stderr.log",
	}
}

type fakeObservationFinalizer struct {
	calls      []finalizeCall
	err        error
	errByJobID map[string]error
}

type finalizeCall struct {
	jobID            string
	runnerInstanceID string
	result           model.JobFinalize
}

func (f *fakeObservationFinalizer) FinalizeJobOwned(ctx context.Context, jobID string, runnerInstanceID string, result model.JobFinalize) error {
	f.calls = append(f.calls, finalizeCall{jobID: jobID, runnerInstanceID: runnerInstanceID, result: result})
	if f.errByJobID != nil && f.errByJobID[jobID] != nil {
		return f.errByJobID[jobID]
	}
	return f.err
}
