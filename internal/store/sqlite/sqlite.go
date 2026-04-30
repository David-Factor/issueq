// Package sqlite implements issueq storage on SQLite.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"issueq/internal/model"

	"github.com/oklog/ulid/v2"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("sqlite path is required")
	}
	if path != ":memory:" {
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return nil, fmt.Errorf("create sqlite db: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close sqlite db: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}
	store := &Store{db: db}
	if err := store.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		path := "migrations/" + entry.Name()
		sqlText, err := migrationFS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		if _, err := s.db.ExecContext(ctx, string(sqlText)); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func (s *Store) UpsertIssue(ctx context.Context, issue model.IssueSnapshot) error {
	labels, err := json.Marshal(issue.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	if issue.SyncedAt.IsZero() {
		issue.SyncedAt = time.Now().UTC()
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO issues (issue_key, node_id, host, owner, repo, number, title, body, labels_json, state, github_updated_at, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(issue_key) DO UPDATE SET
  node_id = excluded.node_id,
  host = excluded.host,
  owner = excluded.owner,
  repo = excluded.repo,
  number = excluded.number,
  title = excluded.title,
  body = excluded.body,
  labels_json = excluded.labels_json,
  state = excluded.state,
  github_updated_at = excluded.github_updated_at,
  synced_at = excluded.synced_at
`, issue.IssueKey, issue.NodeID, issue.Host, issue.Owner, issue.Repo, issue.Number, issue.Title, issue.Body, string(labels), issue.State, formatTime(issue.GitHubUpdatedAt), formatTime(issue.SyncedAt))
	if err != nil {
		return fmt.Errorf("upsert issue: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO issue_state (issue_key, node_id, created_at, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(issue_key) DO UPDATE SET node_id = excluded.node_id, updated_at = excluded.updated_at
`, issue.IssueKey, issue.NodeID, formatTime(issue.SyncedAt), formatTime(issue.SyncedAt))
	if err != nil {
		return fmt.Errorf("upsert issue state: %w", err)
	}
	return nil
}

func (s *Store) GetIssue(ctx context.Context, issueKey string) (model.IssueSnapshot, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT issue_key, node_id, host, owner, repo, number, title, body, labels_json, state, github_updated_at, synced_at
FROM issues WHERE issue_key = ?`, issueKey)
	return scanIssue(row)
}

func (s *Store) ClaimNextJob(ctx context.Context, runnerID string, allowedKinds []string, maxGlobal int, perRouteLimit map[string]int, leaseDuration time.Duration) (*model.Job, error) {
	if maxGlobal <= 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer tx.Rollback()

	var running int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE status = ?`, model.JobStatusRunning).Scan(&running); err != nil {
		return nil, fmt.Errorf("count running jobs: %w", err)
	}
	if running >= maxGlobal {
		return nil, nil
	}

	rows, err := tx.QueryContext(ctx, `
SELECT id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, locked_by, lease_until, pid,
       context_path, result_path, stdout_path, stderr_path, created_at, updated_at, started_at, finished_at, last_error
FROM jobs
WHERE status = ? AND available_at <= ?
ORDER BY priority DESC, created_at ASC, id ASC`, model.JobStatusPending, formatTime(now))
	if err != nil {
		return nil, fmt.Errorf("select pending jobs: %w", err)
	}
	defer rows.Close()

	allowed := stringSet(allowedKinds)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		if len(allowed) > 0 {
			if _, ok := allowed[job.Kind]; !ok {
				continue
			}
		}
		limit := perRouteLimit[job.RouteName]
		if limit <= 0 {
			limit = perRouteLimit[job.Kind]
		}
		if limit > 0 {
			var routeRunning int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE status = ? AND (route_name = ? OR kind = ?)`, model.JobStatusRunning, job.RouteName, job.Kind).Scan(&routeRunning); err != nil {
				return nil, fmt.Errorf("count route running jobs: %w", err)
			}
			if routeRunning >= limit {
				continue
			}
		}

		res, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, locked_by = ?, lease_until = ?, started_at = COALESCE(started_at, ?), updated_at = ?
WHERE id = ? AND status = ?`, model.JobStatusRunning, runnerID, formatTime(leaseUntil), formatTime(now), formatTime(now), job.ID, model.JobStatusPending)
		if err != nil {
			return nil, fmt.Errorf("claim job: %w", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("claim rows affected: %w", err)
		}
		if affected == 0 {
			continue
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit claim: %w", err)
		}
		claimed, err := s.jobByID(ctx, job.ID)
		if err != nil {
			return nil, err
		}
		return &claimed, nil
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pending job rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit empty claim: %w", err)
	}
	return nil, nil
}

func (s *Store) ReleaseExpiredLeases(ctx context.Context, now time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET status = ?, locked_by = NULL, lease_until = NULL, pid = NULL, updated_at = ?, last_error = 'lease expired'
WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?`, model.JobStatusPending, formatTime(now.UTC()), model.JobStatusRunning, formatTime(now.UTC()))
	if err != nil {
		return 0, fmt.Errorf("release expired leases: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("release rows affected: %w", err)
	}
	return int(affected), nil
}

func (s *Store) UpdateJobArtifacts(ctx context.Context, jobID, contextPath, resultPath, stdoutPath, stderrPath string, pid int) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET context_path = ?, result_path = ?, stdout_path = ?, stderr_path = ?, pid = ?, updated_at = ?
WHERE id = ?`, contextPath, resultPath, stdoutPath, stderrPath, pid, formatTime(time.Now().UTC()), jobID)
	if err != nil {
		return fmt.Errorf("update job artifacts: %w", err)
	}
	return nil
}

func (s *Store) FinalizeJob(ctx context.Context, jobID string, result model.JobFinalize) error {
	finished := result.FinishedAt
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET status = ?, locked_by = NULL, lease_until = NULL, pid = NULL, result_path = COALESCE(NULLIF(?, ''), result_path),
    stdout_path = COALESCE(NULLIF(?, ''), stdout_path), stderr_path = COALESCE(NULLIF(?, ''), stderr_path),
    finished_at = ?, updated_at = ?, last_error = ?
WHERE id = ?`, result.Status, result.ResultPath, result.StdoutPath, result.StderrPath, formatTime(finished), formatTime(finished), nullString(result.LastError), jobID)
	if err != nil {
		return fmt.Errorf("finalize job: %w", err)
	}
	return nil
}

func (s *Store) jobByID(ctx context.Context, id string) (model.Job, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, locked_by, lease_until, pid,
       context_path, result_path, stdout_path, stderr_path, created_at, updated_at, started_at, finished_at, last_error
FROM jobs WHERE id = ?`, id)
	return scanJob(row)
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func (s *Store) ListRoutableIssues(ctx context.Context) ([]model.IssueSnapshot, error) {
	return s.listIssues(ctx, "WHERE state = 'open' ORDER BY number ASC")
}

func (s *Store) ListIssues(ctx context.Context) ([]model.IssueSnapshot, error) {
	return s.listIssues(ctx, "ORDER BY number ASC")
}

func (s *Store) listIssues(ctx context.Context, suffix string) ([]model.IssueSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT issue_key, node_id, host, owner, repo, number, title, body, labels_json, state, github_updated_at, synced_at
FROM issues `+suffix)
	if err != nil {
		return nil, fmt.Errorf("list issues: %w", err)
	}
	defer rows.Close()
	var issues []model.IssueSnapshot
	for rows.Next() {
		issue, err := scanIssue(rows)
		if err != nil {
			return nil, err
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list issues rows: %w", err)
	}
	return issues, nil
}

func (s *Store) EnqueueJob(ctx context.Context, create model.JobCreate) (model.Job, bool, error) {
	now := time.Now().UTC()
	if create.AvailableAt.IsZero() {
		create.AvailableAt = now
	}
	job := model.Job{
		ID:          newID("job"),
		IssueKey:    create.IssueKey,
		RouteName:   create.RouteName,
		Kind:        create.Kind,
		Status:      model.JobStatusPending,
		Priority:    create.Priority,
		DedupeKey:   create.DedupeKey,
		AvailableAt: create.AvailableAt,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	res, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO jobs (id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
`, job.ID, job.IssueKey, job.RouteName, job.Kind, job.Status, job.Priority, job.DedupeKey, formatTime(job.AvailableAt), formatTime(job.CreatedAt), formatTime(job.UpdatedAt))
	if err != nil {
		return model.Job{}, false, fmt.Errorf("enqueue job: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return model.Job{}, false, fmt.Errorf("enqueue job rows affected: %w", err)
	}
	if affected == 1 {
		return job, true, nil
	}
	existing, err := s.jobByDedupeKey(ctx, create.DedupeKey)
	if err != nil {
		return model.Job{}, false, err
	}
	return existing, false, nil
}

func (s *Store) ListJobs(ctx context.Context) ([]model.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, locked_by, lease_until, pid,
       context_path, result_path, stdout_path, stderr_path, created_at, updated_at, started_at, finished_at, last_error
FROM jobs
ORDER BY priority DESC, created_at ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	var jobs []model.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list jobs rows: %w", err)
	}
	return jobs, nil
}

func (s *Store) InsertJobEvent(ctx context.Context, event model.JobEvent) (model.JobEvent, error) {
	now := time.Now().UTC()
	if event.ID == "" {
		event.ID = newID("evt")
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO job_events (id, job_id, issue_key, event_type, message, data_json, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
`, event.ID, nullString(event.JobID), nullString(event.IssueKey), event.EventType, nullString(event.Message), nullString(event.DataJSON), formatTime(event.CreatedAt))
	if err != nil {
		return model.JobEvent{}, fmt.Errorf("insert job event: %w", err)
	}
	return event, nil
}

func (s *Store) ListJobEvents(ctx context.Context, jobID string) ([]model.JobEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, job_id, issue_key, event_type, message, data_json, created_at
FROM job_events
WHERE (? = '' OR job_id = ?)
ORDER BY created_at ASC, id ASC`, jobID, jobID)
	if err != nil {
		return nil, fmt.Errorf("list job events: %w", err)
	}
	defer rows.Close()
	var events []model.JobEvent
	for rows.Next() {
		var event model.JobEvent
		var jobID, issueKey, message, dataJSON sql.NullString
		var createdAt string
		if err := rows.Scan(&event.ID, &jobID, &issueKey, &event.EventType, &message, &dataJSON, &createdAt); err != nil {
			return nil, fmt.Errorf("scan job event: %w", err)
		}
		event.JobID = jobID.String
		event.IssueKey = issueKey.String
		event.Message = message.String
		event.DataJSON = dataJSON.String
		event.CreatedAt = parseTime(createdAt)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list job events rows: %w", err)
	}
	return events, nil
}

func (s *Store) jobByDedupeKey(ctx context.Context, key string) (model.Job, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, locked_by, lease_until, pid,
       context_path, result_path, stdout_path, stderr_path, created_at, updated_at, started_at, finished_at, last_error
FROM jobs WHERE dedupe_key = ?`, key)
	return scanJob(row)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanIssue(row scanner) (model.IssueSnapshot, error) {
	var issue model.IssueSnapshot
	var labelsJSON string
	var ghUpdated, synced string
	if err := row.Scan(&issue.IssueKey, &issue.NodeID, &issue.Host, &issue.Owner, &issue.Repo, &issue.Number, &issue.Title, &issue.Body, &labelsJSON, &issue.State, &ghUpdated, &synced); err != nil {
		return model.IssueSnapshot{}, fmt.Errorf("scan issue: %w", err)
	}
	if err := json.Unmarshal([]byte(labelsJSON), &issue.Labels); err != nil {
		return model.IssueSnapshot{}, fmt.Errorf("unmarshal issue labels: %w", err)
	}
	issue.GitHubUpdatedAt = parseTime(ghUpdated)
	issue.SyncedAt = parseTime(synced)
	return issue, nil
}

func scanJob(row scanner) (model.Job, error) {
	var job model.Job
	var lockedBy, leaseUntil, contextPath, resultPath, stdoutPath, stderrPath, startedAt, finishedAt, lastError sql.NullString
	var pid sql.NullInt64
	var availableAt, createdAt, updatedAt string
	if err := row.Scan(&job.ID, &job.IssueKey, &job.RouteName, &job.Kind, &job.Status, &job.Priority, &job.Attempts, &job.DedupeKey, &availableAt, &lockedBy, &leaseUntil, &pid, &contextPath, &resultPath, &stdoutPath, &stderrPath, &createdAt, &updatedAt, &startedAt, &finishedAt, &lastError); err != nil {
		return model.Job{}, fmt.Errorf("scan job: %w", err)
	}
	job.AvailableAt = parseTime(availableAt)
	job.CreatedAt = parseTime(createdAt)
	job.UpdatedAt = parseTime(updatedAt)
	job.LockedBy = lockedBy.String
	if leaseUntil.Valid {
		t := parseTime(leaseUntil.String)
		job.LeaseUntil = &t
	}
	job.PID = int(pid.Int64)
	job.ContextPath = contextPath.String
	job.ResultPath = resultPath.String
	job.StdoutPath = stdoutPath.String
	job.StderrPath = stderrPath.String
	if startedAt.Valid {
		t := parseTime(startedAt.String)
		job.StartedAt = &t
	}
	if finishedAt.Valid {
		t := parseTime(finishedAt.String)
		job.FinishedAt = &t
	}
	job.LastError = lastError.String
	return job, nil
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func newID(prefix string) string {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	return prefix + "_" + ulid.MustNew(ulid.Timestamp(time.Now().UTC()), entropy).String()
}
