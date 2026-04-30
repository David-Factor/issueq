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

type RunnerInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
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
