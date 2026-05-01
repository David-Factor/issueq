package workflow

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"issueq/internal/model"
	"issueq/internal/supervisor"
)

func TestDurableLaunchRecordMapsWrapperDurableFields(t *testing.T) {
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	processStarted := started.Add(-time.Second)
	timeout := started.Add(time.Minute)
	job := durableWrapperJob("job-1")
	job.SupervisorID = "pid-123"
	job.PID = 123
	job.PGID = 456
	job.ProcessStartedAt = &processStarted
	job.StartedAt = &started
	job.TimeoutAt = &timeout

	record, ok, reason := DurableLaunchRecord(job)
	if !ok {
		t.Fatalf("DurableLaunchRecord ok=false reason=%q", reason)
	}
	want := supervisor.LaunchRecord{
		Kind:             supervisor.KindWrapper,
		ID:               "pid-123",
		JobID:            "job-1",
		LaunchToken:      "tok-job-1",
		PID:              123,
		PGID:             456,
		ProcessStartedAt: processStarted,
		MetadataPath:     "/tmp/job-1/run.json",
		StartedAt:        started,
		TimeoutAt:        timeout,
	}
	if !reflect.DeepEqual(record, want) {
		t.Fatalf("record = %#v, want %#v", record, want)
	}
}

func TestDurableLaunchRecordAcceptsLaunchingWithMetadataOnly(t *testing.T) {
	job := durableWrapperJob("job-1")
	job.LaunchState = model.LaunchStateLaunching
	job.SupervisorID = ""
	job.PID = 0

	record, ok, reason := DurableLaunchRecord(job)
	if !ok {
		t.Fatalf("DurableLaunchRecord ok=false reason=%q", reason)
	}
	if record.Kind != supervisor.KindWrapper || record.JobID != job.ID || record.LaunchToken != job.LaunchToken || record.MetadataPath != job.RunMetadataPath {
		t.Fatalf("record = %#v", record)
	}
}

func TestDurableLaunchRecordRejectsUnsupportedOrIncompleteRows(t *testing.T) {
	base := durableWrapperJob("job-1")
	tests := []struct {
		name string
		edit func(*model.Job)
	}{
		{name: "non running", edit: func(j *model.Job) { j.Status = model.JobStatusPending }},
		{name: "blank supervisor kind", edit: func(j *model.Job) { j.SupervisorKind = "" }},
		{name: "attached", edit: func(j *model.Job) { j.SupervisorKind = supervisor.KindAttached }},
		{name: "systemd", edit: func(j *model.Job) { j.SupervisorKind = supervisor.KindSystemd }},
		{name: "missing launch token", edit: func(j *model.Job) { j.LaunchToken = "" }},
		{name: "preparing", edit: func(j *model.Job) { j.LaunchState = model.LaunchStatePreparing }},
		{name: "unknown", edit: func(j *model.Job) { j.LaunchState = model.LaunchStateUnknown }},
		{name: "blank launch state", edit: func(j *model.Job) { j.LaunchState = "" }},
		{name: "unsupported launch state", edit: func(j *model.Job) { j.LaunchState = "weird" }},
		{name: "no durable evidence", edit: func(j *model.Job) { j.RunMetadataPath = ""; j.SupervisorID = ""; j.PID = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := base
			tt.edit(&job)
			_, ok, reason := DurableLaunchRecord(job)
			if ok {
				t.Fatal("DurableLaunchRecord ok=true, want false")
			}
			if reason == "" {
				t.Fatal("reason is empty")
			}
		})
	}
}

func TestInspectDurableJobReturnsUnknownWithoutBackendForUnsupportedRows(t *testing.T) {
	job := durableWrapperJob("job-1")
	job.SupervisorKind = supervisor.KindAttached
	fake := &supervisor.Fake{NextObservation: supervisor.Observation{State: supervisor.RunRunning}}

	obs, err := InspectDurableJob(context.Background(), fake, job)
	if err != nil {
		t.Fatalf("InspectDurableJob error = %v", err)
	}
	if obs.State != supervisor.RunUnknown || obs.Error == "" {
		t.Fatalf("obs = %#v, want unknown with reason", obs)
	}
	if len(fake.Inspections) != 0 {
		t.Fatalf("backend inspections = %d, want 0", len(fake.Inspections))
	}
}

func TestInspectDurableJobCallsBackendForWrapperRows(t *testing.T) {
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	processStarted := started.Add(-time.Second)
	timeout := started.Add(time.Minute)
	job := durableWrapperJob("job-1")
	job.SupervisorID = "pid-123"
	job.PID = 123
	job.PGID = 456
	job.ProcessStartedAt = &processStarted
	job.StartedAt = &started
	job.TimeoutAt = &timeout
	wantObs := supervisor.Observation{State: supervisor.RunRunning, StartedAt: started}
	fake := &supervisor.Fake{NextObservation: wantObs}

	obs, err := InspectDurableJob(context.Background(), fake, job)
	if err != nil {
		t.Fatalf("InspectDurableJob error = %v", err)
	}
	if !reflect.DeepEqual(obs, wantObs) {
		t.Fatalf("obs = %#v, want %#v", obs, wantObs)
	}
	if len(fake.Inspections) != 1 {
		t.Fatalf("backend inspections = %d, want 1", len(fake.Inspections))
	}
	wantRecord, _, _ := DurableLaunchRecord(job)
	if !reflect.DeepEqual(fake.Inspections[0], wantRecord) {
		t.Fatalf("inspection record = %#v, want %#v", fake.Inspections[0], wantRecord)
	}
}

func TestObserveOwnedRunningJobsListsAndObservesInOrder(t *testing.T) {
	valid := durableWrapperJob("job-1")
	unsupported := durableWrapperJob("job-2")
	unsupported.SupervisorKind = supervisor.KindAttached
	fake := &supervisor.Fake{NextObservation: supervisor.Observation{State: supervisor.RunRunning}}
	lister := &fakeOwnedRunningLister{jobs: []model.Job{valid, unsupported}}

	got, err := ObserveOwnedRunningJobs(context.Background(), lister, fake, model.RunnerIdentity{InstanceID: "runner-1"})
	if err != nil {
		t.Fatalf("ObserveOwnedRunningJobs error = %v", err)
	}
	if !reflect.DeepEqual(lister.seenRunnerInstanceID, "runner-1") {
		t.Fatalf("runner instance = %q, want runner-1", lister.seenRunnerInstanceID)
	}
	if len(got) != 2 {
		t.Fatalf("observations = %d, want 2", len(got))
	}
	if got[0].Job.ID != "job-1" || got[0].Observation.State != supervisor.RunRunning {
		t.Fatalf("first observation = %#v", got[0])
	}
	if got[1].Job.ID != "job-2" || got[1].Observation.State != supervisor.RunUnknown {
		t.Fatalf("second observation = %#v", got[1])
	}
	if len(fake.Inspections) != 1 || fake.Inspections[0].JobID != "job-1" {
		t.Fatalf("inspections = %#v", fake.Inspections)
	}
}

func TestObserveOwnedRunningJobsPropagatesListError(t *testing.T) {
	fake := &supervisor.Fake{}
	wantErr := errors.New("boom")
	_, err := ObserveOwnedRunningJobs(context.Background(), &fakeOwnedRunningLister{err: wantErr}, fake, model.RunnerIdentity{InstanceID: "runner-1"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if len(fake.Inspections) != 0 {
		t.Fatalf("inspections = %d, want 0", len(fake.Inspections))
	}
}

func TestObserveOwnedRunningJobsPropagatesInspectError(t *testing.T) {
	wantErr := errors.New("inspect failed")
	_, err := ObserveOwnedRunningJobs(context.Background(), &fakeOwnedRunningLister{jobs: []model.Job{durableWrapperJob("job-1")}}, &supervisor.Fake{InspectErr: wantErr}, model.RunnerIdentity{InstanceID: "runner-1"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if !strings.Contains(err.Error(), "job-1") {
		t.Fatalf("error %q does not include job id", err.Error())
	}
}

func durableWrapperJob(id string) model.Job {
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	timeout := started.Add(time.Minute)
	return model.Job{
		ID:              id,
		Status:          model.JobStatusRunning,
		SupervisorKind:  supervisor.KindWrapper,
		SupervisorID:    "pid-" + id,
		LaunchToken:     "tok-" + id,
		LaunchState:     model.LaunchStateRunning,
		PID:             100,
		RunMetadataPath: "/tmp/" + id + "/run.json",
		StartedAt:       &started,
		TimeoutAt:       &timeout,
	}
}

type fakeOwnedRunningLister struct {
	jobs                 []model.Job
	err                  error
	seenRunnerInstanceID string
}

func (l *fakeOwnedRunningLister) ListOwnedRunningJobs(ctx context.Context, runnerInstanceID string) ([]model.Job, error) {
	l.seenRunnerInstanceID = runnerInstanceID
	if l.err != nil {
		return nil, l.err
	}
	return l.jobs, nil
}
