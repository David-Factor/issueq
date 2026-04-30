// Package model defines shared domain types for issueq.
package model

import "time"

const (
	JobStatusPending   = "pending"
	JobStatusRunning   = "running"
	JobStatusSucceeded = "succeeded"
	JobStatusFailed    = "failed"
	JobStatusSkipped   = "skipped"
	JobStatusDead      = "dead"
	JobStatusCancelled = "cancelled"
)

type IssueSnapshot struct {
	IssueKey        string
	NodeID          string
	Host            string
	Owner           string
	Repo            string
	Number          int
	Title           string
	Body            string
	Labels          []string
	State           string
	GitHubUpdatedAt time.Time
	SyncedAt        time.Time
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
	ID          string
	IssueKey    string
	RouteName   string
	Kind        string
	Status      string
	Priority    int
	Attempts    int
	DedupeKey   string
	AvailableAt time.Time
	LockedBy    string
	LeaseUntil  *time.Time
	PID         int
	ContextPath string
	ResultPath  string
	StdoutPath  string
	StderrPath  string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
	LastError   string
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
