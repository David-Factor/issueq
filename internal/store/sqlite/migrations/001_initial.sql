CREATE TABLE IF NOT EXISTS issues (
  issue_key text primary key,
  node_id text,
  host text not null,
  owner text not null,
  repo text not null,
  number integer not null,
  title text not null,
  body text,
  labels_json text not null,
  state text not null,
  github_updated_at text,
  synced_at text not null
);

CREATE TABLE IF NOT EXISTS jobs (
  id text primary key,
  issue_key text not null,
  route_name text not null,
  kind text not null,
  status text not null,
  priority integer not null default 0,
  attempts integer not null default 0,
  dedupe_key text not null unique,
  available_at text not null,
  locked_by text,
  runner_instance_id text,
  lease_until text,
  pid integer,
  context_path text,
  result_path text,
  stdout_path text,
  stderr_path text,
  created_at text not null,
  updated_at text not null,
  started_at text,
  finished_at text,
  last_error text
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_priority_created ON jobs(status, priority DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_issue_key ON jobs(issue_key);
CREATE INDEX IF NOT EXISTS idx_jobs_status_lease_runner ON jobs(status, lease_until, runner_instance_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status_route_name ON jobs(status, route_name);

CREATE TABLE IF NOT EXISTS issue_state (
  issue_key text primary key,
  node_id text,
  generation integer not null default 0,
  transition_count integer not null default 0,
  created_at text not null,
  updated_at text not null
);

CREATE TABLE IF NOT EXISTS route_attempts (
  issue_key text not null,
  generation integer not null,
  route_name text not null,
  attempts integer not null default 0,
  updated_at text not null,
  primary key (issue_key, generation, route_name)
);

CREATE TABLE IF NOT EXISTS runner_heartbeats (
  runner_instance_id text primary key,
  runner_id text not null,
  pid integer,
  heartbeat_at text not null,
  created_at text not null,
  updated_at text not null
);

CREATE INDEX IF NOT EXISTS idx_runner_heartbeats_instance_heartbeat ON runner_heartbeats(runner_instance_id, heartbeat_at);

CREATE TABLE IF NOT EXISTS job_events (
  id text primary key,
  job_id text,
  issue_key text,
  event_type text not null,
  message text,
  data_json text,
  created_at text not null
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_created ON job_events(job_id, created_at ASC);
