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
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_status_lease_runner ON jobs(status, lease_until, runner_instance_id)`); err != nil {
		return fmt.Errorf("create jobs lease runner index: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_status_route_name ON jobs(status, route_name)`); err != nil {
		return fmt.Errorf("create jobs status route index: %w", err)
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
SELECT id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid,
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
SET status = ?, locked_by = ?, runner_instance_id = ?, lease_until = ?, started_at = COALESCE(started_at, ?), updated_at = ?, last_error = NULL
WHERE id = ? AND status = ?`, model.JobStatusRunning, identity.RunnerID, identity.InstanceID, formatTime(leaseUntil), formatTime(now), formatTime(now), job.ID, model.JobStatusPending)
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
)`, model.JobStatusRunning, formatTime(now), formatTime(staleHeartbeatBefore.UTC()))
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
SET status = ?, locked_by = NULL, runner_instance_id = NULL, lease_until = NULL, pid = NULL, updated_at = ?, last_error = 'lease expired'
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

func (s *Store) UpdateJobArtifactsOwned(ctx context.Context, jobID, runnerInstanceID, contextPath, resultPath, stdoutPath, stderrPath string, pid int) error {
	now := time.Now().UTC()
	return s.execOwned(ctx, jobID, runnerInstanceID, `
UPDATE jobs
SET context_path = ?, result_path = ?, stdout_path = ?, stderr_path = ?, pid = ?, updated_at = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND (lease_until IS NULL OR lease_until >= ?)`, contextPath, resultPath, stdoutPath, stderrPath, pid, formatTime(now), jobID, runnerInstanceID, model.JobStatusRunning, formatTime(now))
}

func (s *Store) UpdateJobAttempts(ctx context.Context, jobID string, attempts int) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET attempts = ?, updated_at = ?
WHERE id = ?`, attempts, formatTime(time.Now().UTC()), jobID)
	if err != nil {
		return fmt.Errorf("update job attempts: %w", err)
	}
	return nil
}

func (s *Store) UpdateJobAttemptsOwned(ctx context.Context, jobID, runnerInstanceID string, attempts int) error {
	now := time.Now().UTC()
	return s.execOwned(ctx, jobID, runnerInstanceID, `
UPDATE jobs
SET attempts = ?, updated_at = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND (lease_until IS NULL OR lease_until >= ?)`, attempts, formatTime(now), jobID, runnerInstanceID, model.JobStatusRunning, formatTime(now))
}

func (s *Store) FinalizeJob(ctx context.Context, jobID string, result model.JobFinalize) error {
	finished := result.FinishedAt
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET status = ?, locked_by = NULL, runner_instance_id = NULL, lease_until = NULL, pid = NULL, result_path = COALESCE(NULLIF(?, ''), result_path),
    stdout_path = COALESCE(NULLIF(?, ''), stdout_path), stderr_path = COALESCE(NULLIF(?, ''), stderr_path),
    finished_at = ?, updated_at = ?, last_error = ?
WHERE id = ?`, result.Status, result.ResultPath, result.StdoutPath, result.StderrPath, formatTime(finished), formatTime(finished), nullString(result.LastError), jobID)
	if err != nil {
		return fmt.Errorf("finalize job: %w", err)
	}
	return nil
}

func (s *Store) FinalizeJobOwned(ctx context.Context, jobID string, runnerInstanceID string, result model.JobFinalize) error {
	finished := result.FinishedAt
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	return s.execOwned(ctx, jobID, runnerInstanceID, `
UPDATE jobs
SET status = ?, locked_by = NULL, runner_instance_id = NULL, lease_until = NULL, pid = NULL, result_path = COALESCE(NULLIF(?, ''), result_path),
    stdout_path = COALESCE(NULLIF(?, ''), stdout_path), stderr_path = COALESCE(NULLIF(?, ''), stderr_path),
    finished_at = ?, updated_at = ?, last_error = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND (lease_until IS NULL OR lease_until >= ?)`, result.Status, result.ResultPath, result.StdoutPath, result.StderrPath, formatTime(finished), formatTime(finished), nullString(result.LastError), jobID, runnerInstanceID, model.JobStatusRunning, formatTime(time.Now().UTC()))
}

func (s *Store) IncrementAttempts(ctx context.Context, issueKey string, generation int, routeName string) (int, error) {
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin increment attempts: %w", err)
	}
	defer tx.Rollback()
	attempts, err := incrementAttemptsTx(ctx, tx, issueKey, generation, routeName)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit increment attempts: %w", err)
	}
	return attempts, nil
}

func (s *Store) IncrementAttemptsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string, generation int, routeName string) (int, error) {
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin job attempt increment: %w", err)
	}
	defer tx.Rollback()
	if err := assertJobOwnedTx(ctx, tx, jobID, runnerInstanceID); err != nil {
		return 0, err
	}
	attempts, err := incrementAttemptsTx(ctx, tx, issueKey, generation, routeName)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
UPDATE jobs SET attempts = ?, updated_at = ?
WHERE id = ? AND runner_instance_id = ? AND status = ? AND (lease_until IS NULL OR lease_until >= ?)`, attempts, formatTime(now), jobID, runnerInstanceID, model.JobStatusRunning, formatTime(now))
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

func incrementAttemptsTx(ctx context.Context, tx *immediateTx, issueKey string, generation int, routeName string) (int, error) {
	now := formatTime(time.Now().UTC())
	_, err := tx.ExecContext(ctx, `
INSERT INTO route_attempts (issue_key, generation, route_name, attempts, updated_at)
VALUES (?, ?, ?, 0, ?)
ON CONFLICT(issue_key, generation, route_name) DO NOTHING`, issueKey, generation, routeName, now)
	if err != nil {
		return 0, fmt.Errorf("insert route attempts: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
UPDATE route_attempts SET attempts = attempts + 1, updated_at = ?
WHERE issue_key = ? AND generation = ? AND route_name = ?`, now, issueKey, generation, routeName)
	if err != nil {
		return 0, fmt.Errorf("update route attempts: %w", err)
	}
	var attempts int
	if err := tx.QueryRowContext(ctx, `SELECT attempts FROM route_attempts WHERE issue_key = ? AND generation = ? AND route_name = ?`, issueKey, generation, routeName).Scan(&attempts); err != nil {
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

func (s *Store) IncrementTransitions(ctx context.Context, issueKey string) (int, error) {
	now := formatTime(time.Now().UTC())
	_, err := s.db.ExecContext(ctx, `UPDATE issue_state SET transition_count = transition_count + 1, updated_at = ? WHERE issue_key = ?`, now, issueKey)
	if err != nil {
		return 0, fmt.Errorf("increment transitions: %w", err)
	}
	_, transitions, err := s.GetIssueState(ctx, issueKey)
	return transitions, err
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
SELECT id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid,
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
SELECT id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid,
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
SELECT id, issue_key, route_name, kind, status, priority, attempts, dedupe_key, available_at, locked_by, runner_instance_id, lease_until, pid,
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
	var lockedBy, runnerInstanceID, leaseUntil, contextPath, resultPath, stdoutPath, stderrPath, startedAt, finishedAt, lastError sql.NullString
	var pid sql.NullInt64
	var availableAt, createdAt, updatedAt string
	if err := row.Scan(&job.ID, &job.IssueKey, &job.RouteName, &job.Kind, &job.Status, &job.Priority, &job.Attempts, &job.DedupeKey, &availableAt, &lockedBy, &runnerInstanceID, &leaseUntil, &pid, &contextPath, &resultPath, &stdoutPath, &stderrPath, &createdAt, &updatedAt, &startedAt, &finishedAt, &lastError); err != nil {
		return model.Job{}, fmt.Errorf("scan job: %w", err)
	}
	job.AvailableAt = parseTime(availableAt)
	job.CreatedAt = parseTime(createdAt)
	job.UpdatedAt = parseTime(updatedAt)
	job.LockedBy = lockedBy.String
	job.RunnerInstanceID = runnerInstanceID.String
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
