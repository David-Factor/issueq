// Package supervisor defines execution-supervision contracts.
package supervisor

import (
	"context"
	"time"
)

const (
	KindAttached = "attached"
	KindWrapper  = "wrapper"
	KindSystemd  = "systemd"
)

type LaunchSpec struct {
	JobID        string
	LaunchToken  string
	Command      []string
	Env          []string
	Workdir      string
	ContextPath  string
	ResultPath   string
	StdoutPath   string
	StderrPath   string
	MetadataPath string
	Timeout      time.Duration
}

type LaunchRecord struct {
	Kind         string
	ID           string
	LaunchToken  string
	PID          int
	PGID         int
	MetadataPath string
	StartedAt    time.Time
	TimeoutAt    time.Time
}

type RunState string

const (
	RunStarting  RunState = "starting"
	RunRunning   RunState = "running"
	RunExited    RunState = "exited"
	RunFailed    RunState = "failed"
	RunTimedOut  RunState = "timed_out"
	RunCancelled RunState = "cancelled"
	RunUnknown   RunState = "unknown"
)

type CancelReason string

const (
	CancelShutdown CancelReason = "shutdown"
	CancelTimeout  CancelReason = "timeout"
	CancelOperator CancelReason = "operator"
)

type Observation struct {
	State       RunState
	ExitCode    int
	HasExitCode bool
	Error       string
	StartedAt   time.Time
	FinishedAt  time.Time
	ResultPath  string
	StdoutPath  string
	StderrPath  string
}

type Supervisor interface {
	Launch(context.Context, LaunchSpec) (LaunchRecord, error)
	Inspect(context.Context, LaunchRecord) (Observation, error)
	Cancel(context.Context, LaunchRecord, CancelReason) error
}
