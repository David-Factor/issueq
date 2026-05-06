CREATE TABLE IF NOT EXISTS gate_blocks (
  issue_key text not null,
  generation integer not null,
  route_name text not null,
  reason text not null,
  scope_hash text not null,
  count integer not null default 1,
  action_applied_at text,
  created_at text not null,
  updated_at text not null,
  primary key (issue_key, generation, route_name, reason, scope_hash)
);
