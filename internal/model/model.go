// Package model defines shared domain types for issueq.
package model

import (
	"strconv"
	"time"
)

const (
	JobStatusPending   = "pending"
	JobStatusRunning   = "running"
	JobStatusSucceeded = "succeeded"
	JobStatusFailed    = "failed"
	JobStatusSkipped   = "skipped"
	JobStatusDead      = "dead"
	JobStatusCancelled = "cancelled"
)

const (
	LaunchStatePreparing = "preparing"
	LaunchStateLaunching = "launching"
	LaunchStateRunning   = "running"
	LaunchStateUnknown   = "unknown"
)

type IssueSnapshot struct {
	IssueKey        string    `json:"key"`
	NodeID          string    `json:"node_id,omitempty"`
	Host            string    `json:"host"`
	Owner           string    `json:"owner"`
	Repo            string    `json:"repo"`
	Number          int       `json:"number"`
	Title           string    `json:"title"`
	Body            string    `json:"body,omitempty"`
	Labels          []string  `json:"labels"`
	State           string    `json:"state"`
	GitHubUpdatedAt time.Time `json:"github_updated_at,omitempty"`
	SyncedAt        time.Time `json:"synced_at,omitempty"`
}

type Handoff struct {
	ID                string
	IssueKey          string
	RouteName         string
	Decision          string
	NextRoute         string
	SourceKind        string
	SourceKey         string
	SourceFingerprint string
	TargetKind        string
	TargetKey         string
	PayloadJSON       string
	CreatedAt         time.Time
}

type HandoffQuery struct {
	IssueKey   string
	RouteNames []string
	Decisions  []string
	NextRoute  string
	TargetKind string
	TargetKey  string
}

type IssueComment struct {
	ID        string
	IssueKey  string
	Body      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type JobCreate struct {
	IssueKey    string
	RouteName   string
	Kind        string
	Priority    int
	DedupeKey   string
	AvailableAt time.Time
}

type Job struct {
	ID               string
	IssueKey         string
	RouteName        string
	Kind             string
	Status           string
	Priority         int
	Attempts         int
	DedupeKey        string
	AvailableAt      time.Time
	LockedBy         string
	RunnerInstanceID string
	LeaseUntil       *time.Time
	PID              int
	PGID             int
	SupervisorKind   string
	SupervisorID     string
	LaunchToken      string
	LaunchState      string
	ProcessStartedAt *time.Time
	RunMetadataPath  string
	LaunchSpecPath   string
	ContextPath      string
	ResultPath       string
	StdoutPath       string
	StderrPath       string
	TimeoutAt        *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StartedAt        *time.Time
	FinishedAt       *time.Time
	LastError        string
}

type JobEvent struct {
	ID        string
	JobID     string
	IssueKey  string
	EventType string
	Message   string
	DataJSON  string
	CreatedAt time.Time
}

type RunnerInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type RunnerIdentity struct {
	RunnerID   string
	InstanceID string
}

type LaunchSpecRecord struct {
	SupervisorKind  string
	LaunchToken     string
	LaunchState     string
	LaunchSpecPath  string
	ContextPath     string
	ResultPath      string
	StdoutPath      string
	StderrPath      string
	RunMetadataPath string
	TimeoutAt       time.Time
}

type LaunchRecord struct {
	SupervisorKind   string
	SupervisorID     string
	LaunchToken      string
	LaunchState      string
	PID              int
	PGID             int
	ProcessStartedAt time.Time
	RunMetadataPath  string
	LaunchSpecPath   string
	ContextPath      string
	ResultPath       string
	StdoutPath       string
	StderrPath       string
	TimeoutAt        time.Time
}

type JobFinalize struct {
	Status     string
	LastError  string
	ResultPath string
	StdoutPath string
	StderrPath string
	FinishedAt time.Time
}

func IssueKey(host, owner, repo string, number int) string {
	return host + "/" + owner + "/" + repo + "#" + strconv.Itoa(number)
}
