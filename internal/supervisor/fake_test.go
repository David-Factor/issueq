package supervisor

import (
	"context"
	"testing"
)

func TestFakeSupervisorRecordsCallsAndDefaults(t *testing.T) {
	fake := &Fake{}
	record, err := fake.Launch(context.Background(), LaunchSpec{JobID: "job-1", LaunchToken: "tok"})
	if err != nil {
		t.Fatalf("Launch error = %v", err)
	}
	if record.Kind != KindWrapper || record.LaunchToken != "tok" {
		t.Fatalf("record = %#v", record)
	}
	obs, err := fake.Inspect(context.Background(), record)
	if err != nil {
		t.Fatalf("Inspect error = %v", err)
	}
	if obs.State != RunRunning {
		t.Fatalf("obs = %#v", obs)
	}
	if err := fake.Cancel(context.Background(), record, CancelShutdown); err != nil {
		t.Fatalf("Cancel error = %v", err)
	}
	if len(fake.Launches) != 1 || len(fake.Inspections) != 1 || len(fake.Cancellations) != 1 {
		t.Fatalf("calls launches=%d inspections=%d cancellations=%d", len(fake.Launches), len(fake.Inspections), len(fake.Cancellations))
	}
	last, ok := fake.LastLaunch()
	if !ok || last.JobID != "job-1" {
		t.Fatalf("last launch = %#v ok=%v", last, ok)
	}
}
