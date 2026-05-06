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
	"issueq/internal/store"

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
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db}
	if err := store.configureConnection(ctx, path); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) configureConnection(ctx context.Context, path string) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA busy_timeout = 5000`); err != nil {
		return fmt.Errorf("configure sqlite busy timeout: %w", err)
	}
	if path != ":memory:" {
		if _, err := s.db.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
			return fmt.Errorf("configure sqlite wal: %w", err)
		}
	}
	return nil
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
	if err := s.ensureColumn(ctx, "jobs", "runner_instance_id", "text"); err != nil {
		return err
	}
	for _, column := range []struct{ name, typ string }{
		{"supervisor_kind", "text"},
		{"supervisor_id", "text"},
		{"launch_token", "text"},
		{"launch_state", "text"},
		{"pgid", "integer"},
		{"process_started_at", "text"},
		{"run_metadata_path", "text"},
		{"launch_spec_path", "text"},
		{"timeout_at", "text"},
		{"attempt_generation", "integer not null default 0"},
		{"attempt_scope_hash", "text"},
	} {
		if err := s.ensureColumn(ctx, "jobs", column.name, column.typ); err != nil {
			return err
		}
	}
	for name, query := range map[string]string{
		"idx_jobs_status_lease_runner":    `CREATE INDEX IF NOT EXISTS idx_jobs_status_lease_runner ON jobs(status, lease_until, runner_instance_id)`,
		"idx_jobs_status_route_name":      `CREATE INDEX IF NOT EXISTS idx_jobs_status_route_name ON jobs(status, route_name)`,
		"idx_jobs_running_owner":          `CREATE INDEX IF NOT EXISTS idx_jobs_running_owner ON jobs(status, runner_instance_id, lease_until)`,
		"idx_jobs_running_launch":         `CREATE INDEX IF NOT EXISTS idx_jobs_running_launch ON jobs(status, supervisor_kind, launch_state, launch_token)`,
		"idx_jobs_running_route_capacity": `CREATE INDEX IF NOT EXISTS idx_jobs_running_route_capacity ON jobs(status, route_name)`,
		"idx_jobs_running_timeout":        `CREATE INDEX IF NOT EXISTS idx_jobs_running_timeout ON jobs(status, timeout_at)`,
		"idx_jobs_stale_durable_recovery": `CREATE INDEX IF NOT EXISTS idx_jobs_stale_durable_recovery ON jobs(status, lease_until, runner_instance_id, supervisor_kind, launch_token)`,
	} {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("create %s: %w", name, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS runner_heartbeats (
  runner_instance_id text primary key,
  runner_id text not null,
  pid integer,
  heartbeat_at text not null,
  created_at text not null,
  updated_at text not null
)`); err != nil {
		return fmt.Errorf("create runner heartbeats table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_runner_heartbeats_instance_heartbeat ON runner_heartbeats(runner_instance_id, heartbeat_at)`); err != nil {
		return fmt.Errorf("create runner heartbeats index: %w", err)
	}
	if err := s.ensureColumn(ctx, "gate_blocks", "action_applied_at", "text"); err != nil {
		return err
	}
	if err := s.ensureRouteAttemptsScope(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureRouteAttemptsScope(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(route_attempts)`)
	if err != nil {
		return fmt.Errorf("inspect route_attempts columns: %w", err)
	}
	hasScope := false
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan route_attempts column: %w", err)
		}
		if name == "scope_hash" {
			hasScope = true
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close route_attempts columns: %w", err)
	}
	if hasScope {
		return nil
	}
	_, err = s.db.ExecContext(ctx, `
CREATE TABLE route_attempts_scoped (
  issue_key text not null,
  generation integer not null,
  route_name text not null,
  scope_hash text not null default 'legacy',
  attempts integer not null default 0,
  updated_at text not null,
  primary key (issue_key, generation, route_name, scope_hash)
);
INSERT INTO route_attempts_scoped (issue_key, generation, route_name, scope_hash, attempts, updated_at)
  SELECT issue_key, generation, route_name, 'legacy', attempts, updated_at FROM route_attempts;
DROP TABLE route_attempts;
ALTER TABLE route_attempts_scoped RENAME TO route_attempts;`)
	if err != nil {
		return fmt.Errorf("migrate route_attempts scope: %w", err)
	}
	return nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, columnType string) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("scan %s column: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect %s columns rows: %w", table, err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, column, columnType)); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
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

func (s *Store) ClaimNextJob(ctx context.Context, identity model.RunnerIdentity, allowedKinds []string, maxGlobal int, perRouteLimit map[string]int, leaseDuration time.Duration) (*model.Job, error) {
	return s.claimNextJob(ctx, identity, allowedKinds, maxGlobal, perRouteLimit, leaseDuration, nil)
}

func (s *Store) ClaimNextJobInFrontier(ctx context.Context, identity model.RunnerIdentity, allowedKinds []string, maxGlobal int, perRouteLimit map[string]int, leaseDuration time.Duration, frontierJobIDs []string) (*model.Job, error) {
	if len(frontierJobIDs) == 0 {
		return nil, nil
	}
	frontier := stringSet(frontierJobIDs)
	return s.claimNextJob(ctx, identity, allowedKinds, maxGlobal, perRouteLimit, leaseDuration, frontier)
}

func (s *Store) claimNextJob(ctx context.Context, identity model.RunnerIdentity, allowedKinds []string, maxGlobal int, perRouteLimit map[string]int, leaseDuration time.Duration, frontier map[string]struct{}) (*model.Job, error) {
	if maxGlobal <= 0 {
		return nil, nil
	}
	if strings.TrimSpace(identity.RunnerID) == "" {
		return nil, errors.New("runner id is required")
	}
	if strings.TrimSpace(identity.InstanceID) == "" {
		return nil, errors.New("runner instance id is required")
	}
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)
	tx, err := s.beginImmediate(ctx)
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
SELECT id, issue_key, route_name, kind, status, priority, attempts, attempt_generation, attempt_scope_hash, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid, pgid,
       supervisor_kind, supervisor_id, launch_token, launch_state, process_started_at, run_metadata_path, launch_spec_path,
       context_path, result_path, stdout_path, stderr_path, timeout_at, created_at, updated_at, started_at, finished_at, last_error
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
		if frontier != nil {
			if _, ok := frontier[job.ID]; !ok {
				continue
			}
		}
		if len(allowed) > 0 {
			if _, ok := allowed[job.Kind]; !ok {
				continue
			}
		}
		limit := perRouteLimit[job.RouteName]
		if limit > 0 {
			var routeRunning int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE status = ? AND route_name = ?`, model.JobStatusRunning, job.RouteName).Scan(&routeRunning); err != nil {
				return nil, fmt.Errorf("count route running jobs: %w", err)
			}
			if routeRunning >= limit {
				continue
			}
		}

		res, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, locked_by = ?, runner_instance_id = ?, lease_until = ?, started_at = COALESCE(started_at, ?), updated_at = ?, last_error = NULL,
    launch_state = ?, supervisor_kind = NULL, supervisor_id = NULL, launch_token = NULL, pgid = NULL, process_started_at = NULL,
    run_metadata_path = NULL, launch_spec_path = NULL, timeout_at = NULL
WHERE id = ? AND status = ?`, model.JobStatusRunning, identity.RunnerID, identity.InstanceID, formatTime(leaseUntil), formatTime(now), formatTime(now), model.LaunchStatePreparing, job.ID, model.JobStatusPending)
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
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close pending job rows: %w", err)
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

func (s *Store) ReleaseExpiredLeases(ctx context.Context, now time.Time, staleHeartbeatBefore time.Time, currentRunnerInstanceID string, activeJobIDs []string) (int, error) {
	now = now.UTC()
	active := stringSet(activeJobIDs)
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin release expired leases: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
SELECT j.id, COALESCE(j.runner_instance_id, '')
FROM jobs j
LEFT JOIN runner_heartbeats h ON h.runner_instance_id = j.runner_instance_id
WHERE j.status = ? AND j.lease_until IS NOT NULL AND j.lease_until < ? AND (
  j.runner_instance_id IS NULL OR j.runner_instance_id = '' OR
  h.runner_instance_id IS NULL OR h.heartbeat_at < ?
) AND (j.supervisor_kind IS NULL OR j.supervisor_kind = '' OR j.launch_token IS NULL OR j.launch_token = '' OR j.launch_state = ?)`, model.JobStatusRunning, formatTime(now), formatTime(staleHeartbeatBefore.UTC()), model.LaunchStatePreparing)
	if err != nil {
		return 0, fmt.Errorf("select expired leases: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id, ownerInstanceID string
		if err := rows.Scan(&id, &ownerInstanceID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired lease: %w", err)
		}
		if _, ok := active[id]; ok && currentRunnerInstanceID != "" && ownerInstanceID == currentRunnerInstanceID {
			continue
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close expired leases rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("expired lease rows: %w", err)
	}
	if len(ids) == 0 {
		return 0, tx.Commit()
	}

	updated := 0
	for _, id := range ids {
		res, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, locked_by = NULL, runner_instance_id = NULL, lease_until = NULL, pid = NULL, pgid = NULL,
    supervisor_kind = NULL, supervisor_id = NULL, launch_token = NULL, launch_state = NULL, process_started_at = NULL,
    run_metadata_path = NULL, launch_spec_path = NULL, timeout_at = NULL,
    updated_at = ?, last_error = 'lease expired'
WHERE id = ? AND status = ?`, model.JobStatusPending, formatTime(now), id, model.JobStatusRunning)
		if err != nil {
			return 0, fmt.Errorf("release expired lease %s: %w", id, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("release expired lease rows: %w", err)
		}
		updated += int(affected)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit release expired leases: %w", err)
	}
	return updated, nil
}

func validateLaunchSpecRecord(record model.LaunchSpecRecord) error {
	if record.SupervisorKind == "" {
		return errors.New("supervisor kind is required")
	}
	if record.LaunchToken == "" {
		return errors.New("launch token is required")
	}
	if record.LaunchSpecPath == "" {
		return errors.New("launch spec path is required")
	}
	if record.RunMetadataPath == "" {
		return errors.New("run metadata path is required")
	}
	if record.TimeoutAt.IsZero() {
		return errors.New("timeout_at is required")
	}
	return nil
}

func (s *Store) PersistLaunchSpecOwned(ctx context.Context, jobID, runnerInstanceID string, record model.LaunchSpecRecord) error {
	now := time.Now().UTC()
	if err := validateLaunchSpecRecord(record); err != nil {
		return err
	}
	return s.execOwned(ctx, jobID, runnerInstanceID, `
UPDATE jobs
SET supervisor_kind = ?, launch_token = ?, launch_state = ?, launch_spec_path = ?, context_path = ?, result_path = ?, stdout_path = ?, stderr_path = ?, run_metadata_path = ?, timeout_at = ?, updated_at = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND launch_state = ? AND (lease_until IS NULL OR lease_until >= ?)`, record.SupervisorKind, record.LaunchToken, model.LaunchStatePreparing, record.LaunchSpecPath, record.ContextPath, record.ResultPath, record.StdoutPath, record.StderrPath, record.RunMetadataPath, formatTime(record.TimeoutAt), formatTime(now), jobID, runnerInstanceID, model.JobStatusRunning, model.LaunchStatePreparing, formatTime(now))
}

func (s *Store) MarkJobLaunchingOwned(ctx context.Context, jobID, runnerInstanceID, launchToken string) error {
	now := time.Now().UTC()
	return s.execOwned(ctx, jobID, runnerInstanceID, `
UPDATE jobs
SET launch_state = ?, updated_at = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND launch_token = ? AND launch_state = ? AND (lease_until IS NULL OR lease_until >= ?)`, model.LaunchStateLaunching, formatTime(now), jobID, runnerInstanceID, model.JobStatusRunning, launchToken, model.LaunchStatePreparing, formatTime(now))
}

func validateLaunchRecord(record model.LaunchRecord) error {
	if record.SupervisorKind == "" {
		return errors.New("supervisor kind is required")
	}
	if record.SupervisorID == "" && record.RunMetadataPath == "" {
		return errors.New("supervisor id or run metadata path is required")
	}
	if record.LaunchToken == "" {
		return errors.New("launch token is required")
	}
	return nil
}

func (s *Store) PersistLaunchRecordOwned(ctx context.Context, jobID, runnerInstanceID string, record model.LaunchRecord) error {
	now := time.Now().UTC()
	if err := validateLaunchRecord(record); err != nil {
		return err
	}
	processStartedAt := nullTime(record.ProcessStartedAt)
	timeoutAt := nullTime(record.TimeoutAt)
	return s.execOwned(ctx, jobID, runnerInstanceID, `
UPDATE jobs
SET supervisor_kind = COALESCE(NULLIF(?, ''), supervisor_kind), supervisor_id = ?, launch_token = COALESCE(NULLIF(?, ''), launch_token), launch_state = ?, pid = ?, pgid = ?, process_started_at = COALESCE(?, process_started_at), run_metadata_path = COALESCE(NULLIF(?, ''), run_metadata_path), launch_spec_path = COALESCE(NULLIF(?, ''), launch_spec_path), context_path = COALESCE(NULLIF(?, ''), context_path), result_path = COALESCE(NULLIF(?, ''), result_path), stdout_path = COALESCE(NULLIF(?, ''), stdout_path), stderr_path = COALESCE(NULLIF(?, ''), stderr_path), timeout_at = COALESCE(?, timeout_at), updated_at = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND launch_token = ? AND launch_state = ? AND (lease_until IS NULL OR lease_until >= ?)`, record.SupervisorKind, record.SupervisorID, record.LaunchToken, model.LaunchStateRunning, record.PID, record.PGID, processStartedAt, record.RunMetadataPath, record.LaunchSpecPath, record.ContextPath, record.ResultPath, record.StdoutPath, record.StderrPath, timeoutAt, formatTime(now), jobID, runnerInstanceID, model.JobStatusRunning, record.LaunchToken, model.LaunchStateLaunching, formatTime(now))
}

func (s *Store) ListOwnedRunningJobs(ctx context.Context, runnerInstanceID string) ([]model.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, issue_key, route_name, kind, status, priority, attempts, attempt_generation, attempt_scope_hash, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid, pgid,
       supervisor_kind, supervisor_id, launch_token, launch_state, process_started_at, run_metadata_path, launch_spec_path,
       context_path, result_path, stdout_path, stderr_path, timeout_at, created_at, updated_at, started_at, finished_at, last_error
FROM jobs
WHERE status = ? AND runner_instance_id = ?
ORDER BY started_at ASC, id ASC`, model.JobStatusRunning, runnerInstanceID)
	if err != nil {
		return nil, fmt.Errorf("list owned running jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows, "owned running jobs")
}

func (s *Store) CountRunningJobs(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE status = ?`, model.JobStatusRunning).Scan(&count); err != nil {
		return 0, fmt.Errorf("count running jobs: %w", err)
	}
	return count, nil
}

func (s *Store) CountRunningJobsByRoute(ctx context.Context, routeName string) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE status = ? AND route_name = ?`, model.JobStatusRunning, routeName).Scan(&count); err != nil {
		return 0, fmt.Errorf("count running jobs by route: %w", err)
	}
	return count, nil
}

func (s *Store) FinalizeJobOwned(ctx context.Context, jobID string, runnerInstanceID string, result model.JobFinalize) error {
	finished := result.FinishedAt
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin job finalize: %w", err)
	}
	defer tx.Rollback()
	if err := assertJobOwnedTx(ctx, tx, jobID, runnerInstanceID); err != nil {
		return err
	}
	if result.WorkStarted != nil && !*result.WorkStarted {
		if err := reverseAttemptTx(ctx, tx, jobID); err != nil {
			return err
		}
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, locked_by = NULL, runner_instance_id = NULL, lease_until = NULL, pid = NULL, pgid = NULL,
    supervisor_kind = NULL, supervisor_id = NULL, launch_token = NULL, launch_state = NULL, process_started_at = NULL,
    run_metadata_path = NULL, launch_spec_path = NULL, timeout_at = NULL,
    result_path = COALESCE(NULLIF(?, ''), result_path),
    stdout_path = COALESCE(NULLIF(?, ''), stdout_path), stderr_path = COALESCE(NULLIF(?, ''), stderr_path),
    finished_at = ?, updated_at = ?, last_error = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND (lease_until IS NULL OR lease_until >= ?)`, result.Status, result.ResultPath, result.StdoutPath, result.StderrPath, formatTime(finished), formatTime(finished), nullString(result.LastError), jobID, runnerInstanceID, model.JobStatusRunning, formatTime(now))
	if err != nil {
		return fmt.Errorf("finalize owned job: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("finalize owned job rows affected: %w", err)
	}
	if affected == 0 {
		return store.ErrNotOwner
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job finalize: %w", err)
	}
	return nil
}

func (s *Store) IncrementAttemptsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string, generation int, routeName, scopeHash string) (int, error) {
	if strings.TrimSpace(scopeHash) == "" {
		scopeHash = "legacy"
	}
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin job attempt increment: %w", err)
	}
	defer tx.Rollback()
	if err := assertJobOwnedTx(ctx, tx, jobID, runnerInstanceID); err != nil {
		return 0, err
	}
	attempts, err := incrementAttemptsTx(ctx, tx, issueKey, generation, routeName, scopeHash)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
UPDATE jobs SET attempts = ?, attempt_generation = ?, attempt_scope_hash = ?, updated_at = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND (lease_until IS NULL OR lease_until >= ?)`, attempts, generation, scopeHash, formatTime(now), jobID, runnerInstanceID, model.JobStatusRunning, formatTime(now))
	if err != nil {
		return 0, fmt.Errorf("update owned job attempts: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("update owned job attempts rows affected: %w", err)
	}
	if affected == 0 {
		return 0, store.ErrNotOwner
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit job attempt increment: %w", err)
	}
	return attempts, nil
}

func reverseAttemptTx(ctx context.Context, tx *immediateTx, jobID string) error {
	var issueKey, routeName string
	var attempts, generation int
	var scopeHash sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT issue_key, route_name, attempts, attempt_generation, attempt_scope_hash FROM jobs WHERE id = ?`, jobID).Scan(&issueKey, &routeName, &attempts, &generation, &scopeHash); err != nil {
		return fmt.Errorf("load job attempt provenance: %w", err)
	}
	if attempts <= 0 || !scopeHash.Valid || strings.TrimSpace(scopeHash.String) == "" {
		return nil
	}
	now := formatTime(time.Now().UTC())
	res, err := tx.ExecContext(ctx, `
UPDATE route_attempts SET attempts = attempts - 1, updated_at = ?
WHERE issue_key = ? AND generation = ? AND route_name = ? AND scope_hash = ? AND attempts > 0`, now, issueKey, generation, routeName, scopeHash.String)
	if err != nil {
		return fmt.Errorf("reverse route attempt: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("reverse route attempt rows affected: %w", err)
	}
	if affected == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET attempts = attempts - 1, updated_at = ? WHERE id = ? AND attempts > 0`, now, jobID); err != nil {
		return fmt.Errorf("reverse job attempt: %w", err)
	}
	return nil
}

func incrementAttemptsTx(ctx context.Context, tx *immediateTx, issueKey string, generation int, routeName, scopeHash string) (int, error) {
	if strings.TrimSpace(scopeHash) == "" {
		scopeHash = "legacy"
	}
	now := formatTime(time.Now().UTC())
	_, err := tx.ExecContext(ctx, `
INSERT INTO route_attempts (issue_key, generation, route_name, scope_hash, attempts, updated_at)
VALUES (?, ?, ?, ?, 0, ?)
ON CONFLICT(issue_key, generation, route_name, scope_hash) DO NOTHING`, issueKey, generation, routeName, scopeHash, now)
	if err != nil {
		return 0, fmt.Errorf("insert route attempts: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
UPDATE route_attempts SET attempts = attempts + 1, updated_at = ?
WHERE issue_key = ? AND generation = ? AND route_name = ? AND scope_hash = ?`, now, issueKey, generation, routeName, scopeHash)
	if err != nil {
		return 0, fmt.Errorf("update route attempts: %w", err)
	}
	var attempts int
	if err := tx.QueryRowContext(ctx, `SELECT attempts FROM route_attempts WHERE issue_key = ? AND generation = ? AND route_name = ? AND scope_hash = ?`, issueKey, generation, routeName, scopeHash).Scan(&attempts); err != nil {
		return 0, fmt.Errorf("select route attempts: %w", err)
	}
	return attempts, nil
}

func (s *Store) IncrementTransitionsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string) (int, error) {
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin job transition increment: %w", err)
	}
	defer tx.Rollback()
	if err := assertJobOwnedTx(ctx, tx, jobID, runnerInstanceID); err != nil {
		return 0, err
	}
	now := formatTime(time.Now().UTC())
	_, err = tx.ExecContext(ctx, `UPDATE issue_state SET transition_count = transition_count + 1, updated_at = ? WHERE issue_key = ?`, now, issueKey)
	if err != nil {
		return 0, fmt.Errorf("increment owned transitions: %w", err)
	}
	var transitions int
	if err := tx.QueryRowContext(ctx, `SELECT transition_count FROM issue_state WHERE issue_key = ?`, issueKey).Scan(&transitions); err != nil {
		return 0, fmt.Errorf("select owned transitions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit owned transition increment: %w", err)
	}
	return transitions, nil
}

func (s *Store) GetIssueState(ctx context.Context, issueKey string) (generation int, transitions int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT generation, transition_count FROM issue_state WHERE issue_key = ?`, issueKey).Scan(&generation, &transitions)
	if err != nil {
		return 0, 0, fmt.Errorf("get issue state: %w", err)
	}
	return generation, transitions, nil
}

func (s *Store) beginImmediate(ctx context.Context) (*immediateTx, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &immediateTx{ctx: ctx, conn: conn}, nil
}

type immediateTx struct {
	ctx  context.Context
	conn *sql.Conn
	done bool
}

func (tx *immediateTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.conn.ExecContext(ctx, query, args...)
}

func (tx *immediateTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.conn.QueryContext(ctx, query, args...)
}

func (tx *immediateTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.conn.QueryRowContext(ctx, query, args...)
}

func (tx *immediateTx) Commit() error {
	if tx.done {
		return nil
	}
	tx.done = true
	_, err := tx.conn.ExecContext(tx.ctx, `COMMIT`)
	closeErr := tx.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (tx *immediateTx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	_, err := tx.conn.ExecContext(tx.ctx, `ROLLBACK`)
	closeErr := tx.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (s *Store) execOwned(ctx context.Context, jobID, runnerInstanceID, query string, args ...any) error {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute owned job update: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("owned job update rows affected: %w", err)
	}
	if affected == 1 {
		return nil
	}
	return s.ownershipError(ctx, jobID, runnerInstanceID)
}

func (s *Store) ownershipError(ctx context.Context, jobID, runnerInstanceID string) error {
	var status string
	var owner sql.NullString
	var leaseUntil sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT status, runner_instance_id, lease_until FROM jobs WHERE id = ?`, jobID).Scan(&status, &owner, &leaseUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return store.ErrNotOwner
	}
	if err != nil {
		return fmt.Errorf("check job ownership: %w", err)
	}
	if status == model.JobStatusRunning && owner.String == runnerInstanceID && leaseUntil.Valid && parseTime(leaseUntil.String).Before(time.Now().UTC()) {
		return store.ErrLostLease
	}
	return store.ErrNotOwner
}

func assertJobOwnedTx(ctx context.Context, tx *immediateTx, jobID, runnerInstanceID string) error {
	var status string
	var owner sql.NullString
	var leaseUntil sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT status, runner_instance_id, lease_until FROM jobs WHERE id = ?`, jobID).Scan(&status, &owner, &leaseUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return store.ErrNotOwner
	}
	if err != nil {
		return fmt.Errorf("check job ownership: %w", err)
	}
	if status != model.JobStatusRunning || owner.String != runnerInstanceID {
		return store.ErrNotOwner
	}
	if leaseUntil.Valid && parseTime(leaseUntil.String).Before(time.Now().UTC()) {
		return store.ErrLostLease
	}
	return nil
}

func (s *Store) HeartbeatRunner(ctx context.Context, identity model.RunnerIdentity, pid int, now time.Time) error {
	if strings.TrimSpace(identity.RunnerID) == "" {
		return errors.New("runner id is required")
	}
	if strings.TrimSpace(identity.InstanceID) == "" {
		return errors.New("runner instance id is required")
	}
	nowText := formatTime(now.UTC())
	_, err := s.db.ExecContext(ctx, `
INSERT INTO runner_heartbeats (runner_instance_id, runner_id, pid, heartbeat_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(runner_instance_id) DO UPDATE SET
  runner_id = excluded.runner_id,
  pid = excluded.pid,
  heartbeat_at = excluded.heartbeat_at,
  updated_at = excluded.updated_at`, identity.InstanceID, identity.RunnerID, pid, nowText, nowText, nowText)
	if err != nil {
		return fmt.Errorf("heartbeat runner: %w", err)
	}
	return nil
}

func (s *Store) DeleteRunnerHeartbeat(ctx context.Context, runnerInstanceID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM runner_heartbeats WHERE runner_instance_id = ?`, runnerInstanceID)
	if err != nil {
		return fmt.Errorf("delete runner heartbeat: %w", err)
	}
	return nil
}

func (s *Store) PruneStaleRunnerHeartbeats(ctx context.Context, before time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM runner_heartbeats WHERE heartbeat_at < ?`, formatTime(before.UTC()))
	if err != nil {
		return 0, fmt.Errorf("prune runner heartbeats: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune runner heartbeats rows affected: %w", err)
	}
	return int(affected), nil
}

func (s *Store) AssertJobOwned(ctx context.Context, jobID, runnerInstanceID string) error {
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin assert job owned: %w", err)
	}
	defer tx.Rollback()
	if err := assertJobOwnedTx(ctx, tx, jobID, runnerInstanceID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RenewJobLease(ctx context.Context, jobID, runnerInstanceID string, leaseDuration time.Duration) error {
	if leaseDuration <= 0 {
		return errors.New("lease duration must be positive")
	}
	now := time.Now().UTC()
	return s.execOwned(ctx, jobID, runnerInstanceID, `
UPDATE jobs
SET lease_until = ?, updated_at = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND (lease_until IS NULL OR lease_until >= ?)`, formatTime(now.Add(leaseDuration)), formatTime(now), jobID, runnerInstanceID, model.JobStatusRunning, formatTime(now))
}

func (s *Store) ListEligibleJobIDs(ctx context.Context, now time.Time) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id FROM jobs
WHERE status = ? AND available_at <= ?
ORDER BY priority DESC, created_at ASC, id ASC`, model.JobStatusPending, formatTime(now.UTC()))
	if err != nil {
		return nil, fmt.Errorf("list eligible job ids: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan eligible job id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eligible job id rows: %w", err)
	}
	return ids, nil
}

func (s *Store) jobByID(ctx context.Context, id string) (model.Job, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, issue_key, route_name, kind, status, priority, attempts, attempt_generation, attempt_scope_hash, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid, pgid,
       supervisor_kind, supervisor_id, launch_token, launch_state, process_started_at, run_metadata_path, launch_spec_path,
       context_path, result_path, stdout_path, stderr_path, timeout_at, created_at, updated_at, started_at, finished_at, last_error
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

func (s *Store) ListStaleDurableRunningJobs(ctx context.Context, now, staleHeartbeatBefore time.Time) ([]model.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT j.id, j.issue_key, j.route_name, j.kind, j.status, j.priority, j.attempts, j.attempt_generation, j.attempt_scope_hash, j.dedupe_key, j.available_at, j.locked_by, j.runner_instance_id, j.lease_until, j.pid, j.pgid,
       j.supervisor_kind, j.supervisor_id, j.launch_token, j.launch_state, j.process_started_at, j.run_metadata_path, j.launch_spec_path,
       j.context_path, j.result_path, j.stdout_path, j.stderr_path, j.timeout_at, j.created_at, j.updated_at, j.started_at, j.finished_at, j.last_error
FROM jobs j
LEFT JOIN runner_heartbeats h ON h.runner_instance_id = j.runner_instance_id
WHERE j.status = ? AND j.lease_until IS NOT NULL AND j.lease_until < ? AND j.launch_state != ? AND j.launch_state != ? AND j.supervisor_kind IS NOT NULL AND j.supervisor_kind != '' AND j.launch_token IS NOT NULL AND j.launch_token != ''
  AND (j.supervisor_id IS NOT NULL AND j.supervisor_id != '' OR j.run_metadata_path IS NOT NULL AND j.run_metadata_path != '')
  AND (j.runner_instance_id IS NULL OR j.runner_instance_id = '' OR h.runner_instance_id IS NULL OR h.heartbeat_at < ?)
ORDER BY j.lease_until ASC, j.id ASC`, model.JobStatusRunning, formatTime(now.UTC()), model.LaunchStatePreparing, model.LaunchStateUnknown, formatTime(staleHeartbeatBefore.UTC()))
	if err != nil {
		return nil, fmt.Errorf("list stale durable running jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows, "stale durable running jobs")
}

func (s *Store) AdoptStaleRunningJob(ctx context.Context, jobID, oldRunnerInstanceID string, newIdentity model.RunnerIdentity, leaseDuration time.Duration, now, staleHeartbeatBefore time.Time) (*model.Job, error) {
	if strings.TrimSpace(newIdentity.RunnerID) == "" {
		return nil, errors.New("runner id is required")
	}
	if strings.TrimSpace(newIdentity.InstanceID) == "" {
		return nil, errors.New("runner instance id is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("lease duration must be positive")
	}
	leaseUntil := now.UTC().Add(leaseDuration)
	res, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET locked_by = ?, runner_instance_id = ?, lease_until = ?, updated_at = ?
WHERE id = ? AND status = ? AND runner_instance_id = ? AND lease_until IS NOT NULL AND lease_until < ?
  AND launch_state != ? AND launch_state != ? AND supervisor_kind IS NOT NULL AND supervisor_kind != '' AND launch_token IS NOT NULL AND launch_token != ''
  AND (supervisor_id IS NOT NULL AND supervisor_id != '' OR run_metadata_path IS NOT NULL AND run_metadata_path != '')
  AND NOT EXISTS (SELECT 1 FROM runner_heartbeats h WHERE h.runner_instance_id = ? AND h.heartbeat_at >= ?)`, newIdentity.RunnerID, newIdentity.InstanceID, formatTime(leaseUntil), formatTime(now.UTC()), jobID, model.JobStatusRunning, oldRunnerInstanceID, formatTime(now.UTC()), model.LaunchStatePreparing, model.LaunchStateUnknown, oldRunnerInstanceID, formatTime(staleHeartbeatBefore.UTC()))
	if err != nil {
		return nil, fmt.Errorf("adopt stale running job: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("adopt stale running rows affected: %w", err)
	}
	if affected == 0 {
		return nil, store.ErrNotOwner
	}
	job, err := s.jobByID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *Store) MarkStaleRunningJobUnknown(ctx context.Context, jobID, oldRunnerInstanceID string, now, staleHeartbeatBefore time.Time) error {
	res, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET launch_state = ?, updated_at = ?
WHERE id = ? AND status = ? AND runner_instance_id = ? AND lease_until IS NOT NULL AND lease_until < ?
  AND launch_state != ? AND launch_state != ? AND supervisor_kind IS NOT NULL AND supervisor_kind != '' AND launch_token IS NOT NULL AND launch_token != ''
  AND (supervisor_id IS NOT NULL AND supervisor_id != '' OR run_metadata_path IS NOT NULL AND run_metadata_path != '')
  AND NOT EXISTS (SELECT 1 FROM runner_heartbeats h WHERE h.runner_instance_id = ? AND h.heartbeat_at >= ?)`, model.LaunchStateUnknown, formatTime(now.UTC()), jobID, model.JobStatusRunning, oldRunnerInstanceID, formatTime(now.UTC()), model.LaunchStatePreparing, model.LaunchStateUnknown, oldRunnerInstanceID, formatTime(staleHeartbeatBefore.UTC()))
	if err != nil {
		return fmt.Errorf("mark stale running job unknown: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark stale running unknown rows affected: %w", err)
	}
	if affected == 0 {
		return store.ErrNotOwner
	}
	return nil
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

func (s *Store) UpsertHandoff(ctx context.Context, handoff model.Handoff) (bool, error) {
	if strings.TrimSpace(handoff.ID) == "" {
		return false, errors.New("handoff id is required")
	}
	if strings.TrimSpace(handoff.IssueKey) == "" {
		return false, errors.New("handoff issue key is required")
	}
	if strings.TrimSpace(handoff.RouteName) == "" {
		return false, errors.New("handoff route name is required")
	}
	if strings.TrimSpace(handoff.Decision) == "" {
		return false, errors.New("handoff decision is required")
	}
	if strings.TrimSpace(handoff.PayloadJSON) == "" {
		return false, errors.New("handoff payload JSON is required")
	}
	if handoff.CreatedAt.IsZero() {
		handoff.CreatedAt = time.Now().UTC()
	}
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return false, fmt.Errorf("begin handoff upsert: %w", err)
	}
	defer tx.Rollback()
	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM handoffs WHERE id = ?`, handoff.ID).Scan(&existing); err != nil {
		return false, fmt.Errorf("check handoff existence: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO handoffs (id, issue_key, route_name, decision, next_route, source_kind, source_key, source_fingerprint, target_kind, target_key, payload_json, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  issue_key = excluded.issue_key,
  route_name = excluded.route_name,
  decision = excluded.decision,
  next_route = excluded.next_route,
  source_kind = excluded.source_kind,
  source_key = excluded.source_key,
  source_fingerprint = excluded.source_fingerprint,
  target_kind = excluded.target_kind,
  target_key = excluded.target_key,
  payload_json = excluded.payload_json,
  created_at = excluded.created_at
`, handoff.ID, handoff.IssueKey, handoff.RouteName, handoff.Decision, nullString(handoff.NextRoute), nullString(handoff.SourceKind), nullString(handoff.SourceKey), nullString(handoff.SourceFingerprint), nullString(handoff.TargetKind), nullString(handoff.TargetKey), handoff.PayloadJSON, formatTime(handoff.CreatedAt))
	if err != nil {
		return false, fmt.Errorf("upsert handoff: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit handoff upsert: %w", err)
	}
	return existing == 0, nil
}

func (s *Store) ListHandoffsForIssue(ctx context.Context, issueKey string) ([]model.Handoff, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, issue_key, route_name, decision, next_route, source_kind, source_key, source_fingerprint, target_kind, target_key, payload_json, created_at
FROM handoffs
WHERE issue_key = ?
ORDER BY created_at ASC, id ASC`, issueKey)
	if err != nil {
		return nil, fmt.Errorf("list handoffs: %w", err)
	}
	defer rows.Close()
	return scanHandoffs(rows, "handoffs")
}

func (s *Store) LatestMatchingHandoff(ctx context.Context, query model.HandoffQuery) (*model.Handoff, error) {
	var clauses []string
	var args []any
	if strings.TrimSpace(query.IssueKey) != "" {
		clauses = append(clauses, "issue_key = ?")
		args = append(args, query.IssueKey)
	}
	if len(query.RouteNames) > 0 {
		placeholders := make([]string, 0, len(query.RouteNames))
		for _, route := range query.RouteNames {
			if strings.TrimSpace(route) == "" {
				continue
			}
			placeholders = append(placeholders, "?")
			args = append(args, route)
		}
		if len(placeholders) > 0 {
			clauses = append(clauses, "route_name IN ("+strings.Join(placeholders, ",")+")")
		}
	}
	if len(query.Decisions) > 0 {
		placeholders := make([]string, 0, len(query.Decisions))
		for _, decision := range query.Decisions {
			if strings.TrimSpace(decision) == "" {
				continue
			}
			placeholders = append(placeholders, "?")
			args = append(args, decision)
		}
		if len(placeholders) > 0 {
			clauses = append(clauses, "decision IN ("+strings.Join(placeholders, ",")+")")
		}
	}
	if strings.TrimSpace(query.NextRoute) != "" {
		clauses = append(clauses, "next_route = ?")
		args = append(args, query.NextRoute)
	}
	if strings.TrimSpace(query.TargetKind) != "" {
		clauses = append(clauses, "target_kind = ?")
		args = append(args, query.TargetKind)
	}
	if strings.TrimSpace(query.TargetKey) != "" {
		clauses = append(clauses, "target_key = ?")
		args = append(args, query.TargetKey)
	}
	where := "1=1"
	if len(clauses) > 0 {
		where = strings.Join(clauses, " AND ")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT id, issue_key, route_name, decision, next_route, source_kind, source_key, source_fingerprint, target_kind, target_key, payload_json, created_at
FROM handoffs
WHERE `+where+`
ORDER BY created_at DESC, id DESC
LIMIT 1`, args...)
	handoff, err := scanHandoff(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &handoff, nil
}

func (s *Store) RecordGateBlock(ctx context.Context, block model.GateBlock) (bool, int, bool, error) {
	if strings.TrimSpace(block.IssueKey) == "" {
		return false, 0, false, errors.New("gate block issue key is required")
	}
	if strings.TrimSpace(block.RouteName) == "" {
		return false, 0, false, errors.New("gate block route name is required")
	}
	if strings.TrimSpace(block.Reason) == "" {
		return false, 0, false, errors.New("gate block reason is required")
	}
	if strings.TrimSpace(block.ScopeHash) == "" {
		return false, 0, false, errors.New("gate block scope hash is required")
	}
	now := time.Now().UTC()
	if block.CreatedAt.IsZero() {
		block.CreatedAt = now
	}
	if block.UpdatedAt.IsZero() {
		block.UpdatedAt = now
	}
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return false, 0, false, fmt.Errorf("begin gate block record: %w", err)
	}
	defer tx.Rollback()
	var existing int
	var actionAppliedAt sql.NullString
	err = tx.QueryRowContext(ctx, `
SELECT count, action_applied_at FROM gate_blocks
WHERE issue_key = ? AND generation = ? AND route_name = ? AND reason = ? AND scope_hash = ?`, block.IssueKey, block.Generation, block.RouteName, block.Reason, block.ScopeHash).Scan(&existing, &actionAppliedAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, 0, false, fmt.Errorf("check gate block: %w", err)
	}
	inserted := errors.Is(err, sql.ErrNoRows)
	actionApplied := actionAppliedAt.Valid && strings.TrimSpace(actionAppliedAt.String) != ""
	count := 1
	if inserted {
		_, err = tx.ExecContext(ctx, `
INSERT INTO gate_blocks (issue_key, generation, route_name, reason, scope_hash, count, action_applied_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, block.IssueKey, block.Generation, block.RouteName, block.Reason, block.ScopeHash, count, nil, formatTime(block.CreatedAt), formatTime(block.UpdatedAt))
	} else {
		count = existing + 1
		_, err = tx.ExecContext(ctx, `
UPDATE gate_blocks
SET count = ?, updated_at = ?
WHERE issue_key = ? AND generation = ? AND route_name = ? AND reason = ? AND scope_hash = ?`, count, formatTime(block.UpdatedAt), block.IssueKey, block.Generation, block.RouteName, block.Reason, block.ScopeHash)
	}
	if err != nil {
		return false, 0, false, fmt.Errorf("record gate block: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, 0, false, fmt.Errorf("commit gate block record: %w", err)
	}
	return inserted, count, actionApplied, nil
}

func (s *Store) MarkGateBlockActionApplied(ctx context.Context, block model.GateBlock) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
UPDATE gate_blocks
SET action_applied_at = COALESCE(action_applied_at, ?), updated_at = ?
WHERE issue_key = ? AND generation = ? AND route_name = ? AND reason = ? AND scope_hash = ?`,
		formatTime(now), formatTime(now), block.IssueKey, block.Generation, block.RouteName, block.Reason, block.ScopeHash)
	if err != nil {
		return fmt.Errorf("mark gate block action applied: %w", err)
	}
	return nil
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
INSERT OR IGNORE INTO jobs (id, issue_key, route_name, kind, status, priority, attempts, attempt_generation, attempt_scope_hash, dedupe_key, available_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, 0, 0, NULL, ?, ?, ?, ?)
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
SELECT id, issue_key, route_name, kind, status, priority, attempts, attempt_generation, attempt_scope_hash, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid, pgid,
       supervisor_kind, supervisor_id, launch_token, launch_state, process_started_at, run_metadata_path, launch_spec_path,
       context_path, result_path, stdout_path, stderr_path, timeout_at, created_at, updated_at, started_at, finished_at, last_error
FROM jobs
ORDER BY priority DESC, created_at ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows, "jobs")
}

func scanJobs(rows *sql.Rows, label string) ([]model.Job, error) {
	var jobs []model.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s rows: %w", label, err)
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
SELECT id, issue_key, route_name, kind, status, priority, attempts, attempt_generation, attempt_scope_hash, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid, pgid,
       supervisor_kind, supervisor_id, launch_token, launch_state, process_started_at, run_metadata_path, launch_spec_path,
       context_path, result_path, stdout_path, stderr_path, timeout_at, created_at, updated_at, started_at, finished_at, last_error
FROM jobs WHERE dedupe_key = ?`, key)
	return scanJob(row)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanHandoffs(rows *sql.Rows, label string) ([]model.Handoff, error) {
	var handoffs []model.Handoff
	for rows.Next() {
		handoff, err := scanHandoff(rows)
		if err != nil {
			return nil, err
		}
		handoffs = append(handoffs, handoff)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %s rows: %w", label, err)
	}
	return handoffs, nil
}

func scanHandoff(row scanner) (model.Handoff, error) {
	var handoff model.Handoff
	var nextRoute, sourceKind, sourceKey, sourceFingerprint, targetKind, targetKey sql.NullString
	var createdAt string
	if err := row.Scan(&handoff.ID, &handoff.IssueKey, &handoff.RouteName, &handoff.Decision, &nextRoute, &sourceKind, &sourceKey, &sourceFingerprint, &targetKind, &targetKey, &handoff.PayloadJSON, &createdAt); err != nil {
		return model.Handoff{}, fmt.Errorf("scan handoff: %w", err)
	}
	handoff.NextRoute = nextRoute.String
	handoff.SourceKind = sourceKind.String
	handoff.SourceKey = sourceKey.String
	handoff.SourceFingerprint = sourceFingerprint.String
	handoff.TargetKind = targetKind.String
	handoff.TargetKey = targetKey.String
	handoff.CreatedAt = parseTime(createdAt)
	return handoff, nil
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
	var lockedBy, runnerInstanceID, leaseUntil sql.NullString
	var supervisorKind, supervisorID, launchToken, launchState, processStartedAt, runMetadataPath, launchSpecPath sql.NullString
	var attemptScopeHash, contextPath, resultPath, stdoutPath, stderrPath, timeoutAt, startedAt, finishedAt, lastError sql.NullString
	var pid, pgid sql.NullInt64
	var attemptGeneration sql.NullInt64
	var availableAt, createdAt, updatedAt string
	if err := row.Scan(&job.ID, &job.IssueKey, &job.RouteName, &job.Kind, &job.Status, &job.Priority, &job.Attempts, &attemptGeneration, &attemptScopeHash, &job.DedupeKey, &availableAt, &lockedBy, &runnerInstanceID, &leaseUntil, &pid, &pgid, &supervisorKind, &supervisorID, &launchToken, &launchState, &processStartedAt, &runMetadataPath, &launchSpecPath, &contextPath, &resultPath, &stdoutPath, &stderrPath, &timeoutAt, &createdAt, &updatedAt, &startedAt, &finishedAt, &lastError); err != nil {
		return model.Job{}, fmt.Errorf("scan job: %w", err)
	}
	job.AvailableAt = parseTime(availableAt)
	job.CreatedAt = parseTime(createdAt)
	job.UpdatedAt = parseTime(updatedAt)
	job.LockedBy = lockedBy.String
	job.RunnerInstanceID = runnerInstanceID.String
	job.AttemptGeneration = int(attemptGeneration.Int64)
	job.AttemptScopeHash = attemptScopeHash.String
	if leaseUntil.Valid {
		t := parseTime(leaseUntil.String)
		job.LeaseUntil = &t
	}
	job.PID = int(pid.Int64)
	job.PGID = int(pgid.Int64)
	job.SupervisorKind = supervisorKind.String
	job.SupervisorID = supervisorID.String
	job.LaunchToken = launchToken.String
	job.LaunchState = launchState.String
	if processStartedAt.Valid {
		t := parseTime(processStartedAt.String)
		job.ProcessStartedAt = &t
	}
	job.RunMetadataPath = runMetadataPath.String
	job.LaunchSpecPath = launchSpecPath.String
	job.ContextPath = contextPath.String
	job.ResultPath = resultPath.String
	job.StdoutPath = stdoutPath.String
	job.StderrPath = stderrPath.String
	if timeoutAt.Valid {
		t := parseTime(timeoutAt.String)
		job.TimeoutAt = &t
	}
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

const sqliteTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(sqliteTimeLayout)
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(sqliteTimeLayout, s)
	if err == nil {
		return t
	}
	t, err = time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return formatTime(t)
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
