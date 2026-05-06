CREATE TABLE IF NOT EXISTS handoffs (
  id text primary key,
  issue_key text not null,
  route_name text not null,
  decision text not null,
  next_route text,
  source_kind text,
  source_key text,
  source_fingerprint text,
  target_kind text,
  target_key text,
  payload_json text not null,
  created_at text not null
);

CREATE INDEX IF NOT EXISTS handoffs_issue_route_idx
  ON handoffs(issue_key, route_name, created_at);

CREATE INDEX IF NOT EXISTS handoffs_issue_next_route_idx
  ON handoffs(issue_key, next_route, created_at);

CREATE INDEX IF NOT EXISTS handoffs_target_idx
  ON handoffs(target_kind, target_key, created_at);
