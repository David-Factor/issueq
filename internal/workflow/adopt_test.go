package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"issueq/internal/model"
	"issueq/internal/store"
	"issueq/internal/supervisor"
)

func TestRecoverStaleDurableRunningJobsAdoptsVerifiedRunning(t *testing.T) {
	job := durableWrapperJob("job-1")
	job.RunnerInstanceID = "old"
	queue := &fakeStaleDurableStore{jobs: []model.Job{job}}
	backend := &supervisor.Fake{NextObservation: supervisor.Observation{State: supervisor.RunRunning}}
	items, summary, err := RecoverStaleDurableRunningJobs(context.Background(), queue, backend, model.RunnerIdentity{RunnerID: "new", InstanceID: "new-1"}, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Scanned != 1 || summary.Adopted != 1 || len(items) != 1 || !items[0].Adopted || items[0].Job.RunnerInstanceID != "new-1" {
		t.Fatalf("items=%#v summary=%#v", items, summary)
	}
	if len(backend.Inspections) != 2 || queue.marked != 0 {
		t.Fatalf("inspections=%#v marked=%d", backend.Inspections, queue.marked)
	}
}

func TestRecoverStaleDurableRunningJobsMarksUnknownWithoutAdoption(t *testing.T) {
	job := durableWrapperJob("job-1")
	job.RunnerInstanceID = "old"
	queue := &fakeStaleDurableStore{jobs: []model.Job{job}}
	backend := &supervisor.Fake{NextObservation: supervisor.Observation{State: supervisor.RunUnknown, Error: "missing"}}
	items, summary, err := RecoverStaleDurableRunningJobs(context.Background(), queue, backend, model.RunnerIdentity{RunnerID: "new", InstanceID: "new-1"}, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 || summary.MarkedUnknown != 1 || queue.adopted != 0 || queue.marked != 1 {
		t.Fatalf("items=%#v summary=%#v adopted=%d marked=%d", items, summary, queue.adopted, queue.marked)
	}
}

func TestRecoverStaleDurableRunningJobsMarksUnsupportedKindUnknownWithoutInspect(t *testing.T) {
	job := durableWrapperJob("job-1")
	job.RunnerInstanceID = "old"
	job.SupervisorKind = supervisor.KindSystemd
	queue := &fakeStaleDurableStore{jobs: []model.Job{job}}
	backend := &supervisor.Fake{}
	_, summary, err := RecoverStaleDurableRunningJobs(context.Background(), queue, backend, model.RunnerIdentity{RunnerID: "new", InstanceID: "new-1"}, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if summary.MarkedUnknown != 1 || len(backend.Inspections) != 0 || queue.adopted != 0 {
		t.Fatalf("summary=%#v inspections=%#v adopted=%d", summary, backend.Inspections, queue.adopted)
	}
}

func TestRecoverStaleDurableRunningJobsDropsAdoptionRace(t *testing.T) {
	job := durableWrapperJob("job-1")
	job.RunnerInstanceID = "old"
	queue := &fakeStaleDurableStore{jobs: []model.Job{job}, adoptErr: store.ErrNotOwner}
	_, summary, err := RecoverStaleDurableRunningJobs(context.Background(), queue, &supervisor.Fake{}, model.RunnerIdentity{RunnerID: "new", InstanceID: "new-1"}, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if summary.OwnershipLost != 1 || summary.Adopted != 0 {
		t.Fatalf("summary=%#v", summary)
	}
}

func TestRecoverStaleDurableRunningJobsPropagatesInspectError(t *testing.T) {
	job := durableWrapperJob("job-1")
	job.RunnerInstanceID = "old"
	want := errors.New("inspect failed")
	_, _, err := RecoverStaleDurableRunningJobs(context.Background(), &fakeStaleDurableStore{jobs: []model.Job{job}}, &supervisor.Fake{InspectErr: want}, model.RunnerIdentity{RunnerID: "new", InstanceID: "new-1"}, time.Minute, time.Now())
	if !errors.Is(err, want) {
		t.Fatalf("err=%v want %v", err, want)
	}
}

type fakeStaleDurableStore struct {
	jobs     []model.Job
	listErr  error
	adoptErr error
	markErr  error
	adopted  int
	marked   int
}

func (f *fakeStaleDurableStore) ListStaleDurableRunningJobs(ctx context.Context, now, staleHeartbeatBefore time.Time) ([]model.Job, error) {
	return f.jobs, f.listErr
}

func (f *fakeStaleDurableStore) AdoptStaleRunningJob(ctx context.Context, jobID, oldRunnerInstanceID string, newIdentity model.RunnerIdentity, leaseDuration time.Duration, now, staleHeartbeatBefore time.Time) (*model.Job, error) {
	if f.adoptErr != nil {
		return nil, f.adoptErr
	}
	f.adopted++
	job := f.jobs[0]
	job.RunnerInstanceID = newIdentity.InstanceID
	job.LockedBy = newIdentity.RunnerID
	leaseUntil := now.Add(leaseDuration)
	job.LeaseUntil = &leaseUntil
	return &job, nil
}

func (f *fakeStaleDurableStore) MarkStaleRunningJobUnknown(ctx context.Context, jobID, oldRunnerInstanceID string, now, staleHeartbeatBefore time.Time) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.marked++
	return nil
}
