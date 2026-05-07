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
	AutomationEventStatusReady      = "ready"
	AutomationEventStatusRunning    = "running"
	AutomationEventStatusSucceeded  = "succeeded"
	AutomationEventStatusFailed     = "failed"
	AutomationEventStatusStale      = "stale"
	AutomationEventStatusNeedsHuman = "needs_human"
	AutomationEventStatusCancelled  = "cancelled"
)

func IsTerminalAutomationEventStatus(status string) bool {
	switch status {
	case AutomationEventStatusSucceeded, AutomationEventStatusFailed, AutomationEventStatusStale, AutomationEventStatusNeedsHuman, AutomationEventStatusCancelled:
		return true
	default:
		return false
	}
}

type EventRepoRef struct {
	Host  string `json:"host"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type EventSourceRef struct {
	Kind string `json:"kind,omitempty"`
	Key  string `json:"key,omitempty"`
	URL  string `json:"url,omitempty"`
}

type EventTargetRef struct {
	Kind        string `json:"kind"`
	Key         string `json:"key"`
	Fingerprint string `json:"fingerprint"`
}

type AutomationEvent struct {
	EventKey          string
	Kind              string
	RouteName         string
	Status            string
	Priority          int
	RepoHost          string
	Owner             string
	Repo              string
	SourceKind        string
	SourceKey         string
	SourceURL         string
	TargetKind        string
	TargetKey         string
	TargetFingerprint string
	Subscope          string
	PayloadJSON       string
	ResultJSON        string
	AttemptCount      int
	LeaseOwner        string
	LeaseExpiresAt    *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type EventHandoff struct {
	ID                string
	ProducerEventKey  string
	ProducerRoute     string
	Decision          string
	NextEventKind     string
	NextRoute         string
	TargetKind        string
	TargetKey         string
	TargetFingerprint string
	Subscope          string
	PayloadJSON       string
	CreatedAt         time.Time
}

const (
	GateBlockReasonMissingHandoff     = "missing_handoff"
	GateBlockReasonDecisionNotAllowed = "decision_not_allowed"
	GateBlockReasonNextRouteMismatch  = "next_route_mismatch"
	GateBlockReasonSourceStale        = "source_stale"
	GateBlockReasonTargetStale        = "target_stale"
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

type GateBlock struct {
	IssueKey        string
	Generation      int
	RouteName       string
	Reason          string
	ScopeHash       string
	Count           int
	ActionAppliedAt *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
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
	ID                string
	IssueKey          string
	RouteName         string
	Kind              string
	Status            string
	Priority          int
	Attempts          int
	AttemptGeneration int
	AttemptScopeHash  string
	DedupeKey         string
	AvailableAt       time.Time
	LockedBy          string
	RunnerInstanceID  string
	LeaseUntil        *time.Time
	PID               int
	PGID              int
	SupervisorKind    string
	SupervisorID      string
	LaunchToken       string
	LaunchState       string
	ProcessStartedAt  *time.Time
	RunMetadataPath   string
	LaunchSpecPath    string
	ContextPath       string
	ResultPath        string
	StdoutPath        string
	StderrPath        string
	TimeoutAt         *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
	StartedAt         *time.Time
	FinishedAt        *time.Time
	LastError         string
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
	Status      string
	LastError   string
	ResultPath  string
	StdoutPath  string
	StderrPath  string
	FinishedAt  time.Time
	WorkStarted *bool
}

func IssueKey(host, owner, repo string, number int) string {
	return host + "/" + owner + "/" + repo + "#" + strconv.Itoa(number)
}
