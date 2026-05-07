package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"issueq/internal/model"
	"issueq/internal/store"
)

func (s *Store) UpsertAutomationEvent(ctx context.Context, event model.AutomationEvent) (model.AutomationEvent, bool, bool, error) {
	if strings.TrimSpace(event.EventKey) == "" {
		return model.AutomationEvent{}, false, false, errors.New("event key is required")
	}
	if strings.TrimSpace(event.Kind) == "" {
		return model.AutomationEvent{}, false, false, errors.New("event kind is required")
	}
	if strings.TrimSpace(event.RouteName) == "" {
		return model.AutomationEvent{}, false, false, errors.New("event route is required")
	}
	if strings.TrimSpace(event.TargetFingerprint) == "" {
		return model.AutomationEvent{}, false, false, errors.New("target fingerprint is required")
	}
	if strings.TrimSpace(event.PayloadJSON) == "" {
		event.PayloadJSON = "{}"
	}
	now := time.Now().UTC()
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	event.UpdatedAt = now
	if event.Status == "" {
		event.Status = model.AutomationEventStatusReady
	}
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return model.AutomationEvent{}, false, false, fmt.Errorf("begin event upsert: %w", err)
	}
	defer tx.Rollback()
	var existingStatus string
	err = tx.QueryRowContext(ctx, `SELECT status FROM automation_events WHERE event_key = ?`, event.EventKey).Scan(&existingStatus)
	inserted := errors.Is(err, sql.ErrNoRows)
	if err != nil && !inserted {
		return model.AutomationEvent{}, false, false, fmt.Errorf("check event: %w", err)
	}
	terminalProtected := false
	if inserted {
		_, err = tx.ExecContext(ctx, `INSERT INTO automation_events (event_key, kind, route_name, status, priority, repo_host, owner, repo, source_kind, source_key, source_url, target_kind, target_key, target_fingerprint, subscope, payload_json, attempt_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`, event.EventKey, event.Kind, event.RouteName, event.Status, event.Priority, event.RepoHost, event.Owner, event.Repo, nullString(event.SourceKind), nullString(event.SourceKey), nullString(event.SourceURL), event.TargetKind, event.TargetKey, event.TargetFingerprint, nullString(event.Subscope), event.PayloadJSON, formatTime(event.CreatedAt), formatTime(event.UpdatedAt))
	} else if model.IsTerminalAutomationEventStatus(existingStatus) {
		terminalProtected = true
		_, err = tx.ExecContext(ctx, `UPDATE automation_events SET priority = ?, payload_json = ?, updated_at = ? WHERE event_key = ?`, event.Priority, event.PayloadJSON, formatTime(now), event.EventKey)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE automation_events SET kind = ?, route_name = ?, priority = ?, repo_host = ?, owner = ?, repo = ?, source_kind = ?, source_key = ?, source_url = ?, target_kind = ?, target_key = ?, target_fingerprint = ?, subscope = ?, payload_json = ?, updated_at = ? WHERE event_key = ?`, event.Kind, event.RouteName, event.Priority, event.RepoHost, event.Owner, event.Repo, nullString(event.SourceKind), nullString(event.SourceKey), nullString(event.SourceURL), event.TargetKind, event.TargetKey, event.TargetFingerprint, nullString(event.Subscope), event.PayloadJSON, formatTime(now), event.EventKey)
	}
	if err != nil {
		return model.AutomationEvent{}, false, false, fmt.Errorf("upsert event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.AutomationEvent{}, false, false, fmt.Errorf("commit event upsert: %w", err)
	}
	loaded, err := s.GetAutomationEvent(ctx, event.EventKey)
	if err != nil {
		return model.AutomationEvent{}, false, false, err
	}
	return loaded, inserted, terminalProtected, nil
}

func (s *Store) GetAutomationEvent(ctx context.Context, key string) (model.AutomationEvent, error) {
	row := s.db.QueryRowContext(ctx, `SELECT event_key, kind, route_name, status, priority, repo_host, owner, repo, source_kind, source_key, source_url, target_kind, target_key, target_fingerprint, subscope, payload_json, result_json, attempt_count, lease_owner, lease_expires_at, created_at, updated_at FROM automation_events WHERE event_key = ?`, key)
	return scanAutomationEvent(row)
}

func (s *Store) ListAutomationEvents(ctx context.Context) ([]model.AutomationEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT event_key, kind, route_name, status, priority, repo_host, owner, repo, source_kind, source_key, source_url, target_kind, target_key, target_fingerprint, subscope, payload_json, result_json, attempt_count, lease_owner, lease_expires_at, created_at, updated_at FROM automation_events ORDER BY priority DESC, updated_at ASC, event_key ASC`)
	if err != nil {
		return nil, fmt.Errorf("list automation events: %w", err)
	}
	defer rows.Close()
	var out []model.AutomationEvent
	for rows.Next() {
		ev, err := scanAutomationEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("automation events rows: %w", err)
	}
	return out, nil
}

func (s *Store) CancelAutomationEvent(ctx context.Context, key string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `UPDATE automation_events SET status = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ? WHERE event_key = ? AND status NOT IN (?, ?, ?, ?, ?, ?)`, model.AutomationEventStatusCancelled, formatTime(now), key, model.AutomationEventStatusBlocked, model.AutomationEventStatusSucceeded, model.AutomationEventStatusFailed, model.AutomationEventStatusStale, model.AutomationEventStatusNeedsHuman, model.AutomationEventStatusCancelled)
	if err != nil {
		return fmt.Errorf("cancel automation event: %w", err)
	}
	return nil
}

func (s *Store) RetryAutomationEvent(ctx context.Context, key string) error {
	now := time.Now().UTC()
	// Operator retry is the only path that deliberately reopens a terminal
	// automation event.  attempt_count is an execution budget guard (the
	// claimer only accepts rows with attempt_count < max_attempts), so reset it
	// to zero to make one fresh run claimable even for routes with
	// max_attempts: 1.  Keep result_json intact until the next finalization so
	// the previous terminal result/block reason remains available for audit.
	_, err := s.db.ExecContext(ctx, `UPDATE automation_events SET status = ?, attempt_count = 0, lease_owner = NULL, lease_expires_at = NULL, updated_at = ? WHERE event_key = ? AND status IN (?, ?, ?, ?)`, model.AutomationEventStatusReady, formatTime(now), key, model.AutomationEventStatusBlocked, model.AutomationEventStatusFailed, model.AutomationEventStatusStale, model.AutomationEventStatusCancelled)
	if err != nil {
		return fmt.Errorf("retry automation event: %w", err)
	}
	return nil
}

func (s *Store) BlockAutomationEvent(ctx context.Context, key string, reason store.EventBlockReason) error {
	now := time.Now().UTC()
	code := strings.TrimSpace(reason.Code)
	if code == "" {
		code = "dependency_not_satisfied"
	}
	message := strings.TrimSpace(reason.Message)
	if message == "" {
		message = code
	}
	result := fmt.Sprintf(`{"schema":"issueq-blocked/v1","reason":{"code":%q,"message":%q}}`, code, message)
	_, err := s.db.ExecContext(ctx, `UPDATE automation_events SET status = ?, result_json = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ? WHERE event_key = ? AND status IN (?, ?)`, model.AutomationEventStatusBlocked, result, formatTime(now), key, model.AutomationEventStatusReady, model.AutomationEventStatusRunning)
	if err != nil {
		return fmt.Errorf("block automation event: %w", err)
	}
	return nil
}

func (s *Store) ReleaseExpiredAutomationEvents(ctx context.Context, now time.Time, maxAttemptsByRoute map[string]int) (int, error) {
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT event_key, route_name, attempt_count FROM automation_events WHERE status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at < ?`, model.AutomationEventStatusRunning, formatTime(now.UTC()))
	if err != nil {
		return 0, fmt.Errorf("select expired automation events: %w", err)
	}
	type item struct {
		key, route string
		attempts   int
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.key, &it.route, &it.attempts); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, it)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	count := 0
	for _, it := range items {
		status := model.AutomationEventStatusReady
		if max := maxAttemptsByRoute[it.route]; max > 0 && it.attempts >= max {
			status = model.AutomationEventStatusFailed
		}
		res, err := tx.ExecContext(ctx, `UPDATE automation_events SET status = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?, result_json = CASE WHEN ? = ? THEN COALESCE(result_json, '{"error":"lease expired; attempts exhausted"}') ELSE result_json END WHERE event_key = ? AND status = ?`, status, formatTime(now.UTC()), status, model.AutomationEventStatusFailed, it.key, model.AutomationEventStatusRunning)
		if err != nil {
			return 0, fmt.Errorf("release expired automation event: %w", err)
		}
		aff, _ := res.RowsAffected()
		count += int(aff)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) ClaimAutomationEvent(ctx context.Context, opts store.EventClaimOptions) (*model.AutomationEvent, error) {
	if strings.TrimSpace(opts.RouteName) == "" || strings.TrimSpace(opts.LeaseOwner) == "" {
		return nil, errors.New("route and lease owner are required")
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 1
	}
	now := opts.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.beginImmediate(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin event claim: %w", err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `SELECT event_key, kind, route_name, status, priority, repo_host, owner, repo, source_kind, source_key, source_url, target_kind, target_key, target_fingerprint, subscope, payload_json, result_json, attempt_count, lease_owner, lease_expires_at, created_at, updated_at FROM automation_events WHERE status = ? AND route_name = ? AND attempt_count < ? ORDER BY priority DESC, updated_at ASC, event_key ASC LIMIT 1`, model.AutomationEventStatusReady, opts.RouteName, opts.MaxAttempts)
	ev, err := scanAutomationEvent(row)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Commit()
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lease := now.Add(opts.LeaseDuration)
	if opts.LeaseDuration <= 0 {
		lease = now.Add(30 * time.Minute)
	}
	res, err := tx.ExecContext(ctx, `UPDATE automation_events SET status = ?, attempt_count = attempt_count + 1, lease_owner = ?, lease_expires_at = ?, updated_at = ? WHERE event_key = ? AND status = ? AND attempt_count < ?`, model.AutomationEventStatusRunning, opts.LeaseOwner, formatTime(lease), formatTime(now), ev.EventKey, model.AutomationEventStatusReady, opts.MaxAttempts)
	if err != nil {
		return nil, fmt.Errorf("claim automation event: %w", err)
	}
	aff, _ := res.RowsAffected()
	if aff == 0 {
		_ = tx.Commit()
		return nil, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	claimed, err := s.GetAutomationEvent(ctx, ev.EventKey)
	if err != nil {
		return nil, err
	}
	return &claimed, nil
}

func (s *Store) FinalizeAutomationEvent(ctx context.Context, key string, fin store.EventFinalize) (bool, error) {
	now := fin.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `UPDATE automation_events SET status = ?, result_json = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ? WHERE event_key = ? AND status = ? AND lease_owner = ? AND (lease_expires_at IS NULL OR lease_expires_at >= ?)`, fin.Status, fin.ResultJSON, formatTime(now), key, model.AutomationEventStatusRunning, fin.LeaseOwner, formatTime(now))
	if err != nil {
		return false, fmt.Errorf("finalize automation event: %w", err)
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return aff == 1, nil
}

func (s *Store) UpsertEventHandoff(ctx context.Context, h model.EventHandoff) (bool, error) {
	if strings.TrimSpace(h.ID) == "" {
		return false, errors.New("handoff id is required")
	}
	if strings.TrimSpace(h.ProducerEventKey) == "" || strings.TrimSpace(h.ProducerRoute) == "" || strings.TrimSpace(h.Decision) == "" {
		return false, errors.New("handoff producer route and decision are required")
	}
	if strings.TrimSpace(h.PayloadJSON) == "" {
		h.PayloadJSON = "{}"
	}
	if h.CreatedAt.IsZero() {
		h.CreatedAt = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO event_handoffs (id, producer_event_key, producer_route, decision, next_event_kind, next_route, target_kind, target_key, target_fingerprint, subscope, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, h.ID, h.ProducerEventKey, h.ProducerRoute, h.Decision, nullString(h.NextEventKind), nullString(h.NextRoute), h.TargetKind, h.TargetKey, h.TargetFingerprint, nullString(h.Subscope), h.PayloadJSON, formatTime(h.CreatedAt))
	if err != nil {
		return false, fmt.Errorf("upsert event handoff: %w", err)
	}
	aff, _ := res.RowsAffected()
	return aff == 1, nil
}

func (s *Store) LatestMatchingEventHandoff(ctx context.Context, producerRoute string, decisions []string, nextRoute string, targetKind string, targetKey string, targetFingerprint string, subscope string) (*model.EventHandoff, error) {
	clauses := []string{"producer_route = ?", "target_kind = ?", "target_key = ?", "target_fingerprint = ?"}
	args := []any{producerRoute, targetKind, targetKey, targetFingerprint}
	if subscope != "" {
		clauses = append(clauses, "subscope = ?")
		args = append(args, subscope)
	} else {
		clauses = append(clauses, "(subscope IS NULL OR subscope = '')")
	}
	if nextRoute != "" {
		clauses = append(clauses, "next_route = ?")
		args = append(args, nextRoute)
	}
	if len(decisions) > 0 {
		ph := make([]string, 0, len(decisions))
		for _, d := range decisions {
			ph = append(ph, "?")
			args = append(args, d)
		}
		clauses = append(clauses, "decision IN ("+strings.Join(ph, ",")+")")
	}
	row := s.db.QueryRowContext(ctx, `SELECT id, producer_event_key, producer_route, decision, next_event_kind, next_route, target_kind, target_key, target_fingerprint, subscope, payload_json, created_at FROM event_handoffs WHERE `+strings.Join(clauses, " AND ")+` ORDER BY created_at DESC, id DESC LIMIT 1`, args...)
	h, err := scanEventHandoff(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func scanAutomationEvent(row scanner) (model.AutomationEvent, error) {
	var ev model.AutomationEvent
	var sourceKind, sourceKey, sourceURL, subscope, resultJSON, leaseOwner, leaseExpires sql.NullString
	var created, updated string
	err := row.Scan(&ev.EventKey, &ev.Kind, &ev.RouteName, &ev.Status, &ev.Priority, &ev.RepoHost, &ev.Owner, &ev.Repo, &sourceKind, &sourceKey, &sourceURL, &ev.TargetKind, &ev.TargetKey, &ev.TargetFingerprint, &subscope, &ev.PayloadJSON, &resultJSON, &ev.AttemptCount, &leaseOwner, &leaseExpires, &created, &updated)
	if err != nil {
		return model.AutomationEvent{}, err
	}
	ev.SourceKind = sourceKind.String
	ev.SourceKey = sourceKey.String
	ev.SourceURL = sourceURL.String
	ev.Subscope = subscope.String
	ev.ResultJSON = resultJSON.String
	ev.LeaseOwner = leaseOwner.String
	if leaseExpires.Valid {
		t := parseTime(leaseExpires.String)
		ev.LeaseExpiresAt = &t
	}
	ev.CreatedAt = parseTime(created)
	ev.UpdatedAt = parseTime(updated)
	return ev, nil
}

func scanEventHandoff(row scanner) (model.EventHandoff, error) {
	var h model.EventHandoff
	var nextKind, nextRoute, subscope sql.NullString
	var created string
	if err := row.Scan(&h.ID, &h.ProducerEventKey, &h.ProducerRoute, &h.Decision, &nextKind, &nextRoute, &h.TargetKind, &h.TargetKey, &h.TargetFingerprint, &subscope, &h.PayloadJSON, &created); err != nil {
		return model.EventHandoff{}, err
	}
	h.NextEventKind = nextKind.String
	h.NextRoute = nextRoute.String
	h.Subscope = subscope.String
	h.CreatedAt = parseTime(created)
	return h, nil
}
