package supervisor

import (
	"context"
	"errors"
	"sync"
)

type Fake struct {
	mu              sync.Mutex
	NextRecord      LaunchRecord
	NextObservation Observation
	LaunchErr       error
	InspectErr      error
	CancelErr       error
	Launches        []LaunchSpec
	Inspections     []LaunchRecord
	Cancellations   []FakeCancellation
}

type FakeCancellation struct {
	Record LaunchRecord
	Reason CancelReason
}

func (f *Fake) Launch(ctx context.Context, spec LaunchSpec) (LaunchRecord, error) {
	if err := ctx.Err(); err != nil {
		return LaunchRecord{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Launches = append(f.Launches, spec)
	if f.LaunchErr != nil {
		return LaunchRecord{}, f.LaunchErr
	}
	record := f.NextRecord
	if record.Kind == "" {
		record.Kind = KindWrapper
	}
	if record.LaunchToken == "" {
		record.LaunchToken = spec.LaunchToken
	}
	return record, nil
}

func (f *Fake) Inspect(ctx context.Context, record LaunchRecord) (Observation, error) {
	if err := ctx.Err(); err != nil {
		return Observation{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Inspections = append(f.Inspections, record)
	if f.InspectErr != nil {
		return Observation{}, f.InspectErr
	}
	obs := f.NextObservation
	if obs.State == "" {
		obs.State = RunRunning
	}
	return obs, nil
}

func (f *Fake) Cancel(ctx context.Context, record LaunchRecord, reason CancelReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Cancellations = append(f.Cancellations, FakeCancellation{Record: record, Reason: reason})
	return f.CancelErr
}

func (f *Fake) LastLaunch() (LaunchSpec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.Launches) == 0 {
		return LaunchSpec{}, false
	}
	return f.Launches[len(f.Launches)-1], true
}

func (f *Fake) MustHaveNoErrors() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return errors.Join(f.LaunchErr, f.InspectErr, f.CancelErr)
}
