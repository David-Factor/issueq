// Package jobwrapper implements the durable issueq job execution wrapper.
package jobwrapper

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"issueq/internal/runner"
	"issueq/internal/supervisor"
)

const SpecVersion = 1

type Spec struct {
	Version        int      `json:"version"`
	JobID          string   `json:"job_id"`
	LaunchToken    string   `json:"launch_token"`
	Command        []string `json:"command"`
	Env            []string `json:"env"`
	Workdir        string   `json:"workdir"`
	ContextPath    string   `json:"context_path"`
	ResultPath     string   `json:"result_path"`
	StdoutPath     string   `json:"stdout_path"`
	StderrPath     string   `json:"stderr_path"`
	MetadataPath   string   `json:"metadata_path"`
	TimeoutSeconds int64    `json:"timeout_seconds"`
}

type Metadata struct {
	Version     int       `json:"version"`
	JobID       string    `json:"job_id"`
	LaunchToken string    `json:"launch_token"`
	PID         int       `json:"pid"`
	PGID        int       `json:"pgid,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	ExitCode    int       `json:"exit_code"`
	TimedOut    bool      `json:"timed_out"`
	Cancelled   bool      `json:"cancelled"`
	Error       string    `json:"error"`
	ContextPath string    `json:"context_path"`
	ResultPath  string    `json:"result_path"`
	StdoutPath  string    `json:"stdout_path"`
	StderrPath  string    `json:"stderr_path"`
}

func LoadSpec(path string) (Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Spec{}, fmt.Errorf("read launch spec: %w", err)
	}
	var spec Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return Spec{}, fmt.Errorf("parse launch spec: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return Spec{}, err
	}
	return spec, nil
}

func (s Spec) Validate() error {
	if s.Version != SpecVersion {
		return fmt.Errorf("unsupported launch spec version %d", s.Version)
	}
	if s.JobID == "" {
		return errors.New("job_id is required")
	}
	if s.LaunchToken == "" {
		return errors.New("launch_token is required")
	}
	if len(s.Command) == 0 || s.Command[0] == "" {
		return errors.New("command is required")
	}
	if s.ContextPath == "" || s.ResultPath == "" || s.StdoutPath == "" || s.StderrPath == "" || s.MetadataPath == "" {
		return errors.New("context_path, result_path, stdout_path, stderr_path, and metadata_path are required")
	}
	if s.TimeoutSeconds <= 0 {
		return errors.New("timeout_seconds must be positive")
	}
	return nil
}

func ValidateContext(spec Spec) error {
	data, err := os.ReadFile(spec.ContextPath)
	if err != nil {
		return fmt.Errorf("read context: %w", err)
	}
	var ctx runner.Context
	if err := json.Unmarshal(data, &ctx); err != nil {
		return fmt.Errorf("parse context: %w", err)
	}
	if ctx.Job.ID != spec.JobID {
		return fmt.Errorf("context job id %q does not match launch job id %q", ctx.Job.ID, spec.JobID)
	}
	return nil
}

func WriteMetadataAtomic(path string, metadata Metadata) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create metadata dir: %w", err)
	}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create metadata temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write metadata temp: %w", err)
	}
	if _, err := tmp.Write([]byte("\n")); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write metadata newline: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync metadata temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close metadata temp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename metadata: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		if syncErr := dir.Sync(); syncErr != nil {
			_ = dir.Close()
			return fmt.Errorf("sync metadata dir: %w", syncErr)
		}
		if closeErr := dir.Close(); closeErr != nil {
			return fmt.Errorf("close metadata dir: %w", closeErr)
		}
	}
	return nil
}

func LoadMetadata(path string) (Metadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Metadata{}, fmt.Errorf("read metadata: %w", err)
	}
	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return Metadata{}, fmt.Errorf("parse metadata: %w", err)
	}
	if metadata.Version != SpecVersion {
		return Metadata{}, fmt.Errorf("unsupported metadata version %d", metadata.Version)
	}
	return metadata, nil
}

func ObservationFromMetadata(metadata Metadata, expectedJobID, expectedLaunchToken string) supervisor.Observation {
	if expectedJobID != "" && metadata.JobID != expectedJobID {
		return supervisor.Observation{State: supervisor.RunUnknown, Error: "metadata launch identity mismatch"}
	}
	if metadata.LaunchToken != expectedLaunchToken {
		return supervisor.Observation{State: supervisor.RunUnknown, Error: "metadata launch identity mismatch"}
	}
	obs := supervisor.Observation{
		ExitCode:    metadata.ExitCode,
		HasExitCode: !metadata.TimedOut && !metadata.Cancelled && metadata.ExitCode >= 0,
		Error:       metadata.Error,
		StartedAt:   metadata.StartedAt,
		FinishedAt:  metadata.FinishedAt,
		ResultPath:  metadata.ResultPath,
		StdoutPath:  metadata.StdoutPath,
		StderrPath:  metadata.StderrPath,
	}
	switch {
	case metadata.Cancelled:
		obs.State = supervisor.RunCancelled
	case metadata.TimedOut:
		obs.State = supervisor.RunTimedOut
	case metadata.Error != "" && metadata.ExitCode < 0:
		obs.State = supervisor.RunFailed
	default:
		obs.State = supervisor.RunExited
		obs.HasExitCode = true
	}
	return obs
}
