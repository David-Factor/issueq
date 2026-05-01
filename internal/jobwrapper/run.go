package jobwrapper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type Options struct {
	Cancel <-chan os.Signal
}

func Run(ctx context.Context, spec Spec, opts Options) (Metadata, error) {
	started := time.Now().UTC()
	metadata := Metadata{
		Version:     SpecVersion,
		JobID:       spec.JobID,
		LaunchToken: spec.LaunchToken,
		ExitCode:    -1,
		StartedAt:   started,
		ContextPath: spec.ContextPath,
		ResultPath:  spec.ResultPath,
		StdoutPath:  spec.StdoutPath,
		StderrPath:  spec.StderrPath,
	}
	if err := spec.Validate(); err != nil {
		metadata.Error = err.Error()
		metadata.FinishedAt = time.Now().UTC()
		_ = WriteMetadataAtomic(spec.MetadataPath, metadata)
		return metadata, err
	}
	if err := ValidateContext(spec); err != nil {
		metadata.Error = err.Error()
		metadata.FinishedAt = time.Now().UTC()
		_ = WriteMetadataAtomic(spec.MetadataPath, metadata)
		return metadata, err
	}
	if err := os.MkdirAll(dirOf(spec.StdoutPath), 0o700); err != nil {
		metadata.Error = fmt.Sprintf("create stdout dir: %v", err)
		metadata.FinishedAt = time.Now().UTC()
		_ = WriteMetadataAtomic(spec.MetadataPath, metadata)
		return metadata, errors.New(metadata.Error)
	}
	if err := os.MkdirAll(dirOf(spec.StderrPath), 0o700); err != nil {
		metadata.Error = fmt.Sprintf("create stderr dir: %v", err)
		metadata.FinishedAt = time.Now().UTC()
		_ = WriteMetadataAtomic(spec.MetadataPath, metadata)
		return metadata, errors.New(metadata.Error)
	}
	stdout, err := os.OpenFile(spec.StdoutPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		metadata.Error = fmt.Sprintf("open stdout: %v", err)
		metadata.FinishedAt = time.Now().UTC()
		_ = WriteMetadataAtomic(spec.MetadataPath, metadata)
		return metadata, errors.New(metadata.Error)
	}
	defer stdout.Close()
	stderr, err := os.OpenFile(spec.StderrPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		metadata.Error = fmt.Sprintf("open stderr: %v", err)
		metadata.FinishedAt = time.Now().UTC()
		_ = WriteMetadataAtomic(spec.MetadataPath, metadata)
		return metadata, errors.New(metadata.Error)
	}
	defer stderr.Close()

	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	prepareCommand(cmd)
	cmd.Env = spec.Env
	cmd.Dir = spec.Workdir
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		metadata.Error = fmt.Sprintf("start command: %v", err)
		metadata.FinishedAt = time.Now().UTC()
		_ = WriteMetadataAtomic(spec.MetadataPath, metadata)
		return metadata, errors.New(metadata.Error)
	}
	metadata.PID = cmd.Process.Pid
	metadata.PGID = processGroupID(cmd.Process.Pid)

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	timeout := time.NewTimer(time.Duration(spec.TimeoutSeconds) * time.Second)
	defer timeout.Stop()

	var waitErr error
	decision := "wait"
	select {
	case waitErr = <-waitCh:
	case <-timeout.C:
		decision = "timeout"
		metadata.TimedOut = true
		_ = killProcessGroup(cmd.Process)
		waitErr = <-waitCh
	case <-opts.Cancel:
		decision = "cancel"
		metadata.Cancelled = true
		_ = killProcessGroup(cmd.Process)
		waitErr = <-waitCh
	case <-ctx.Done():
		decision = "cancel"
		metadata.Cancelled = true
		_ = killProcessGroup(cmd.Process)
		waitErr = <-waitCh
	}
	metadata.FinishedAt = time.Now().UTC()
	if waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			metadata.ExitCode = exitErr.ExitCode()
		}
		if decision == "timeout" {
			metadata.Error = fmt.Sprintf("subprocess timed out after %s", time.Duration(spec.TimeoutSeconds)*time.Second)
		} else if decision == "cancel" {
			metadata.Error = "subprocess cancelled"
		} else {
			metadata.Error = waitErr.Error()
		}
	} else {
		metadata.ExitCode = 0
	}
	if decision == "timeout" && metadata.Error == "" {
		metadata.Error = fmt.Sprintf("subprocess timed out after %s", time.Duration(spec.TimeoutSeconds)*time.Second)
	} else if decision == "cancel" && metadata.Error == "" {
		metadata.Error = "subprocess cancelled"
	}
	if err := WriteMetadataAtomic(spec.MetadataPath, metadata); err != nil {
		return metadata, err
	}
	if metadata.TimedOut || metadata.Cancelled || (waitErr != nil && metadata.ExitCode < 0) || metadata.ExitCode != 0 {
		return metadata, errors.New(metadata.Error)
	}
	return metadata, nil
}

func dirOf(path string) string {
	return filepath.Dir(path)
}
