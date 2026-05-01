package wrapper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"issueq/internal/jobwrapper"
	"issueq/internal/supervisor"
)

type Supervisor struct {
	IssueqPath string
}

func New(issueqPath string) *Supervisor {
	return &Supervisor{IssueqPath: issueqPath}
}

func (s *Supervisor) Launch(ctx context.Context, spec supervisor.LaunchSpec) (supervisor.LaunchRecord, error) {
	if err := ctx.Err(); err != nil {
		return supervisor.LaunchRecord{}, err
	}
	if spec.JobID == "" || spec.LaunchToken == "" {
		return supervisor.LaunchRecord{}, errors.New("job id and launch token are required")
	}
	if spec.SpecPath == "" {
		return supervisor.LaunchRecord{}, errors.New("spec path is required")
	}
	if spec.Timeout <= 0 {
		return supervisor.LaunchRecord{}, errors.New("timeout must be positive")
	}
	issueqPath := s.IssueqPath
	if issueqPath == "" {
		var err error
		issueqPath, err = os.Executable()
		if err != nil {
			return supervisor.LaunchRecord{}, fmt.Errorf("resolve issueq executable: %w", err)
		}
	}
	wrapperSpec := jobwrapper.Spec{
		Version:        jobwrapper.SpecVersion,
		JobID:          spec.JobID,
		LaunchToken:    spec.LaunchToken,
		Command:        append([]string(nil), spec.Command...),
		Env:            append([]string(nil), spec.Env...),
		Workdir:        spec.Workdir,
		ContextPath:    spec.ContextPath,
		ResultPath:     spec.ResultPath,
		StdoutPath:     spec.StdoutPath,
		StderrPath:     spec.StderrPath,
		MetadataPath:   spec.MetadataPath,
		TimeoutSeconds: timeoutSeconds(spec.Timeout),
	}
	if wrapperSpec.TimeoutSeconds <= 0 {
		wrapperSpec.TimeoutSeconds = 1
	}
	if err := writeSpecAtomic(spec.SpecPath, wrapperSpec); err != nil {
		return supervisor.LaunchRecord{}, err
	}
	cmd := exec.Command(issueqPath, "job-wrapper", "--spec", spec.SpecPath)
	prepareCommand(cmd)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil
	if err := cmd.Start(); err != nil {
		return supervisor.LaunchRecord{}, fmt.Errorf("start job wrapper: %w", err)
	}
	pid := cmd.Process.Pid
	pgid := processGroupID(pid)
	go func() { _ = cmd.Wait() }()
	started := time.Now().UTC()
	return supervisor.LaunchRecord{Kind: supervisor.KindWrapper, ID: strconv.Itoa(pid), JobID: spec.JobID, LaunchToken: spec.LaunchToken, PID: pid, PGID: pgid, ProcessStartedAt: processStartedAt(pid), MetadataPath: spec.MetadataPath, StartedAt: started, TimeoutAt: started.Add(spec.Timeout)}, nil
}

func (s *Supervisor) Inspect(ctx context.Context, record supervisor.LaunchRecord) (supervisor.Observation, error) {
	if err := ctx.Err(); err != nil {
		return supervisor.Observation{}, err
	}
	if record.Kind != "" && record.Kind != supervisor.KindWrapper {
		return supervisor.Observation{State: supervisor.RunUnknown, Error: "supervisor kind mismatch"}, nil
	}
	if record.JobID == "" || record.LaunchToken == "" {
		return supervisor.Observation{State: supervisor.RunUnknown, Error: "launch identity is incomplete"}, nil
	}
	if record.MetadataPath != "" {
		metadata, err := jobwrapper.LoadMetadata(record.MetadataPath)
		if err == nil {
			return jobwrapper.ObservationFromMetadata(metadata, record.JobID, record.LaunchToken), nil
		}
		if !errors.Is(err, os.ErrNotExist) && !isPathErrorNotExist(err) {
			return supervisor.Observation{State: supervisor.RunUnknown, Error: err.Error()}, nil
		}
	}
	if record.PID > 0 && processMatches(record.PID, record.ProcessStartedAt) {
		return supervisor.Observation{State: supervisor.RunRunning, StartedAt: record.StartedAt}, nil
	}
	if !record.StartedAt.IsZero() && time.Since(record.StartedAt) < 2*time.Second {
		return supervisor.Observation{State: supervisor.RunStarting, StartedAt: record.StartedAt}, nil
	}
	return supervisor.Observation{State: supervisor.RunUnknown, Error: "wrapper metadata missing and process is not verified running"}, nil
}

func (s *Supervisor) Cancel(ctx context.Context, record supervisor.LaunchRecord, reason supervisor.CancelReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.ProcessStartedAt.IsZero() {
		return errors.New("cannot cancel wrapper without process fingerprint")
	}
	if !processExists(record.PID) {
		return nil
	}
	if !processMatches(record.PID, record.ProcessStartedAt) {
		return errors.New("wrapper process identity mismatch")
	}
	return terminateProcess(record.PID)
}

func timeoutSeconds(d time.Duration) int64 {
	seconds := int64(d / time.Second)
	if d%time.Second != 0 {
		seconds++
	}
	if seconds <= 0 {
		return 1
	}
	return seconds
}

func writeSpecAtomic(path string, spec jobwrapper.Spec) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create launch spec dir: %w", err)
	}
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal launch spec: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create launch spec temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write launch spec temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync launch spec temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close launch spec temp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename launch spec: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		if syncErr := dir.Sync(); syncErr != nil {
			_ = dir.Close()
			return fmt.Errorf("sync launch spec dir: %w", syncErr)
		}
		if closeErr := dir.Close(); closeErr != nil {
			return fmt.Errorf("close launch spec dir: %w", closeErr)
		}
	} else {
		return fmt.Errorf("open launch spec dir: %w", err)
	}
	return nil
}

func isPathErrorNotExist(err error) bool {
	var pathErr *os.PathError
	return errors.As(err, &pathErr) && errors.Is(pathErr.Err, os.ErrNotExist)
}
