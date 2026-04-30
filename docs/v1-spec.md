# issueq v1 Design Spec

## 1. Summary

`issueq` is a small local automation runner for GitHub issues.

It polls GitHub on a configurable interval, stores issue snapshots locally, evaluates simple route predicates, enqueues jobs into SQLite, and dispatches configured subprocesses with explicit concurrency limits.

The intended first use case is lightweight coding-agent automation:

1. A human creates or labels a GitHub issue.
2. `issueq` sees the issue during polling.
3. Labels/predicates route the issue into a local queue.
4. A dispatcher starts a configured subprocess when capacity is available.
5. The subprocess handles triage, coding, review, etc.
6. `issueq` applies configured GitHub label/comment transitions based on the result.

V1 is deliberately local-first and simple. GitHub labels remain the durable, human-visible workflow state. SQLite is the local execution backlog and bookkeeping store.

## 2. Goals

### 2.1 Primary goals

- Run as a local CLI or long-running daemon.
- Poll GitHub issues every few minutes or on demand.
- Use labels and simple predicates to decide which jobs to enqueue.
- Store queue state in local SQLite.
- Spawn configured subprocesses only when matching work exists.
- Enforce global and per-route/per-kind concurrency limits.
- Track attempts and prevent infinite automation loops.
- Update GitHub labels/comments on job start, success, failure, skip, or terminalization.
- Keep GitHub as the public state machine.
- Keep the architecture compatible with a future centralized queue backend.

### 2.2 Non-goals for v1

- No web UI.
- No distributed queue.
- No webhook server requirement.
- No complex workflow engine.
- No need for Redis, Kubernetes, or external workers.
- No requirement to run arbitrary untrusted code safely beyond local process isolation and user-configured commands.
- No attempt to fully solve multi-runner coordination in v1.

## 3. Core mental model

```text
GitHub issues/labels/comments
        ↓ poll
issue snapshots in SQLite
        ↓ route
jobs in SQLite queue
        ↓ dispatch if capacity
configured subprocesses
        ↓ result
GitHub labels/comments + queue state
```

Three layers should remain separate:

1. **GitHub labels** define durable public workflow state.
2. **SQLite queue** defines local execution backlog and operational state.
3. **Subprocess jobs** perform configurable work.

## 4. Workflow labels

Recommended labels for early use:

```text
agent-triage       issue should be triaged
agent-ready        issue is ready for implementation
agent-review       implementation should be reviewed
agent-running      automation currently owns/runs a job
agent-done         automation completed successfully
agent-failed       automation failed terminally
agent-needs-info   issue lacks detail
agent-needs-human  automation stopped; human intervention required
manual-only        automation must ignore this issue
```

V1 should not require these exact labels. They are configured by routes and actions.

Terminal/blocking labels should commonly include:

```text
agent-done
agent-failed
agent-needs-human
manual-only
```

All routes should normally exclude terminal labels.

## 5. Example lifecycle

```text
agent-triage
  → triage job
  → agent-ready
  → code job
  → agent-review
  → review job
  → agent-done
```

If triage fails due to missing details:

```text
agent-triage
  → triage job
  → agent-needs-info
```

If review repeatedly requests changes:

```text
agent-ready
  → code
  → agent-review
  → review says changes needed
  → agent-ready
  → code
  → agent-review
  → review says changes needed
  → ... max attempts exceeded ...
  → agent-needs-human + agent-failed
```

## 6. CLI shape

Initial commands:

```bash
issueq daemon --config issueq.yaml
issueq once --config issueq.yaml
issueq poll --config issueq.yaml
issueq route --config issueq.yaml
issueq dispatch --config issueq.yaml
issueq jobs --config issueq.yaml
issueq issues --config issueq.yaml
```

### 6.1 `daemon`

Runs a loop:

1. release expired local leases
2. poll GitHub if poll interval elapsed
3. route issue snapshots into jobs
4. dispatch pending jobs while capacity exists
5. reap finished child processes
6. sleep briefly

The polling interval can be long. A default of `3m` is acceptable.

### 6.2 `once`

Runs one full reconciliation cycle:

1. poll
2. route
3. dispatch available jobs
4. optionally wait for spawned jobs if `--wait` is supplied

### 6.3 Debug commands

`jobs` and `issues` inspect local SQLite state.

Future useful commands:

```bash
issueq retry <job-id>
issueq cancel <job-id>
issueq run-one --kind code
issueq doctor
```

## 7. Configuration

V1 uses a YAML config file.

Example:

```yaml
runner:
  name: david-exedev
  capabilities: [triage, code, review]

queue:
  backend: sqlite
  sqlite:
    path: ./issueq.db
  max_global_concurrency: 2

polling:
  interval: 3m

github:
  host: github.com
  owner: example-org
  repo: example-repo
  token_env: GITHUB_TOKEN
  query: 'is:issue is:open label:agent-triage,label:agent-ready,label:agent-review'

terminal_labels:
  - agent-done
  - agent-failed
  - agent-needs-human
  - manual-only

workflow:
  max_transitions_per_issue: 10

routes:
  - name: triage
    when:
      labels_include: [agent-triage]
      labels_exclude: [agent-running, agent-done, agent-failed, agent-needs-human, manual-only]
    job:
      kind: triage
      command: ["./tasks/triage.sh"]
      timeout: 10m
      concurrency: 2
      max_attempts: 2
      on_start:
        labels_add: [agent-running]
      on_success:
        labels_remove: [agent-running, agent-triage]
        labels_add: [agent-ready]
        comment: "Triage passed. Issue is ready for automation."
      on_failure:
        labels_remove: [agent-running, agent-triage]
        labels_add: [agent-needs-info]
        comment: "Automation could not triage this issue. More detail is needed."

  - name: code
    when:
      labels_include: [agent-ready]
      labels_exclude: [agent-running, agent-done, agent-failed, agent-needs-human, manual-only]
    job:
      kind: code
      command: ["./tasks/code.sh"]
      timeout: 90m
      concurrency: 1
      max_attempts: 3
      on_start:
        labels_remove: [agent-ready]
        labels_add: [agent-running]
      on_success:
        labels_remove: [agent-running]
        labels_add: [agent-review]
        comment: "Implementation finished. Review queued."
      on_failure:
        labels_remove: [agent-running]
        labels_add: [agent-failed]
        comment: "Automation failed while implementing this issue."
      on_attempts_exceeded:
        labels_remove: [agent-ready, agent-running]
        labels_add: [agent-failed, agent-needs-human]
        comment: "Automation stopped after too many implementation attempts. Human intervention is needed."

  - name: review
    when:
      labels_include: [agent-review]
      labels_exclude: [agent-running, agent-done, agent-failed, agent-needs-human, manual-only]
    job:
      kind: review
      command: ["./tasks/review.sh"]
      timeout: 30m
      concurrency: 1
      max_attempts: 3
      on_start:
        labels_add: [agent-running]
      on_success:
        labels_remove: [agent-running, agent-review]
        labels_add: [agent-done]
        comment: "Automated review passed."
      on_failure:
        labels_remove: [agent-running, agent-review]
        labels_add: [agent-ready]
        comment: "Automated review requested changes. Returning to implementation."
      on_attempts_exceeded:
        labels_remove: [agent-running, agent-review, agent-ready]
        labels_add: [agent-failed, agent-needs-human]
        comment: "Automation stopped after 3 review rounds. This likely needs human direction."
```

## 8. Routing

Routes map issue predicates to job specs.

A route has:

- `name`
- `when`
- `job`

V1 predicates should stay simple:

```yaml
when:
  labels_include: [...]
  labels_exclude: [...]
```

Possible v1.1 predicate additions:

```yaml
when:
  title_matches: "..."
  body_contains: "..."
  author_in: [...]
  assignee_in: [...]
  milestone: "..."
```

But v1 should avoid a rich expression language.

### 8.1 Router idempotency

Routing must be idempotent. Repeated polls must not create unbounded duplicate jobs.

Each enqueued job gets a `dedupe_key`, for example:

```text
{issue_key}:{route_name}:{label_hash}:{issue_updated_at}
```

A simpler first implementation may use:

```text
{issue_key}:{route_name}:{github_updated_at}
```

The queue enforces `unique(dedupe_key)`.

### 8.2 Staleness

Before dispatching a job, the dispatcher fetches the latest issue from GitHub and re-evaluates the route predicate.

If the issue no longer matches, the job is marked `skipped`.

## 9. Queue model

V1 uses SQLite. The schema should still resemble a future distributed queue.

### 9.1 Issue key

Use a stable human-readable key:

```text
github.com/{owner}/{repo}#{number}
```

Store GitHub `node_id` if available for future robustness across repo renames/transfers.

### 9.2 Tables

Conceptual schema:

```sql
issues (
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

jobs (
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
  lease_until text,
  pid integer,
  context_path text,
  result_path text,
  created_at text not null,
  updated_at text not null,
  started_at text,
  finished_at text,
  last_error text
);

issue_state (
  issue_key text primary key,
  node_id text,
  generation integer not null default 0,
  transition_count integer not null default 0,
  created_at text not null,
  updated_at text not null
);

route_attempts (
  issue_key text not null,
  generation integer not null,
  route_name text not null,
  attempts integer not null default 0,
  updated_at text not null,
  primary key (issue_key, generation, route_name)
);

job_events (
  id text primary key,
  job_id text,
  issue_key text,
  event_type text not null,
  message text,
  data_json text,
  created_at text not null
);
```

### 9.3 Job statuses

```text
pending
running
succeeded
failed
skipped
dead
cancelled
```

### 9.4 Leases

Even in local SQLite, jobs use leases:

- `locked_by`
- `lease_until`
- `started_at`

A crashed runner leaves `running` jobs behind. On startup, `issueq` releases expired leases by moving eligible jobs back to `pending` or to `failed`, depending on policy.

For v1, expired `running` jobs may become `pending` if below attempt limits.

## 10. Dispatch model

There are no long-lived idle coding workers.

There is one dispatcher loop that supervises child processes:

```text
pending jobs
  ↓ capacity check
spawn configured subprocess
  ↓ monitor pid, timeout, exit code
apply result actions
```

Capacity limits:

- global max subprocesses
- per-job-kind or per-route max subprocesses
- runner capabilities

Example:

```yaml
queue:
  max_global_concurrency: 2

routes:
  - name: code
    job:
      kind: code
      concurrency: 1
```

If 10 code jobs are queued and `code.concurrency = 1`, only one code subprocess runs at a time.

## 11. Attempts and loop prevention

Infinite loops are expected failure modes and must be handled explicitly.

V1 should implement:

1. Per-route `max_attempts`.
2. Per-issue `max_transitions_per_issue`.
3. Terminal labels that block all routes.
4. Attempts incremented on job start, not completion.

### 11.1 Attempts

Attempt key:

```text
issue_key + generation + route_name
```

Increment before spawning the subprocess.

If incremented attempts exceed `max_attempts`, do not spawn. Apply `on_attempts_exceeded`, mark job `dead`, and comment on GitHub.

### 11.2 Transition count

Every successful application of route actions that changes workflow labels increments `transition_count`.

If `transition_count > max_transitions_per_issue`, terminalize the issue:

```text
add: agent-failed, agent-needs-human
remove: agent-ready, agent-review, agent-running, agent-triage
```

Config should allow customization later. V1 may use route-level `on_attempts_exceeded` plus a default terminal action.

### 11.3 Generation

`generation` allows a human reset later.

V1 may not implement automatic generation reset, but schema should include it.

Future reset triggers:

- human removes `agent-needs-human` and adds `agent-ready`
- command: `issueq reset github.com/org/repo#123`
- structured state comment updated manually or by CLI

## 12. Subprocess contract

Each job command is spawned with context and result paths.

Command from config:

```yaml
command: ["./tasks/code.sh"]
```

Actual invocation:

```bash
./tasks/code.sh /path/to/context.json /path/to/result.json
```

Environment variables should also be set:

```text
ISSUEQ_JOB_ID
ISSUEQ_ROUTE
ISSUEQ_KIND
ISSUEQ_ATTEMPT
ISSUEQ_CONTEXT_PATH
ISSUEQ_RESULT_PATH
ISSUEQ_ISSUE_KEY
GITHUB_HOST
GITHUB_OWNER
GITHUB_REPO
GITHUB_ISSUE_NUMBER
```

### 12.1 Context JSON

Example:

```json
{
  "issue": {
    "key": "github.com/example-org/example-repo#123",
    "node_id": "I_kw...",
    "host": "github.com",
    "owner": "example-org",
    "repo": "example-repo",
    "number": 123,
    "title": "Add CSV export",
    "body": "...",
    "labels": ["agent-ready"],
    "state": "open",
    "github_updated_at": "2026-01-01T00:00:00Z"
  },
  "job": {
    "id": "job_...",
    "route": "code",
    "kind": "code",
    "attempt": 2,
    "max_attempts": 3
  },
  "runner": {
    "id": "runner_...",
    "name": "david-exedev"
  }
}
```

### 12.2 Result JSON

A subprocess may write a result file:

```json
{
  "comment": "Implemented in PR #456.",
  "labels_add": ["agent-review"],
  "labels_remove": ["agent-running"],
  "enqueue": []
}
```

If a result file exists, its actions are merged with or override configured `on_success`/`on_failure` actions. V1 should choose one simple policy:

Recommended v1 policy:

- exit code `0`: apply configured `on_success`, then apply result-file additions/removals/comment if present.
- exit code nonzero: apply configured `on_failure`, then apply result-file additions/removals/comment if present.

This allows jobs to add dynamic comments like PR URLs without controlling the entire workflow.

### 12.3 Direct queue appends

Result JSON may include local follow-up jobs:

```json
{
  "enqueue": [
    {"route": "cleanup", "kind": "cleanup", "delay": "5m"}
  ]
}
```

Guidance:

- User-visible workflow follow-ups should normally be represented by GitHub labels and routed by the router.
- Direct queue appends are for local/internal tasks such as cleanup or log collection.

V1 may defer `enqueue` support if needed.

## 13. GitHub interaction

### 13.1 Authentication

Use token from `github.token_env`, usually:

```text
GITHUB_TOKEN
```

Required scopes depend on repo visibility and operations:

- read issues
- write issues/comments/labels
- possibly read/write pull requests if subprocesses do that separately

### 13.2 Polling

Default polling interval: `3m`.

V1 should support:

- GitHub issue search query from config, or
- list issues for one configured repo and filter locally

Search query is flexible but can be rate-limited. Repo issue listing is simpler and often enough.

Recommended v1 approach:

- list open issues for configured repo
- filter locally using route predicates

Support for multiple repos can come later.

### 13.3 Applying actions

Actions:

```yaml
labels_add: []
labels_remove: []
comment: "..."
```

Application order:

1. refresh issue
2. add/remove labels
3. create comment
4. update local issue snapshot
5. record event

For `on_start`, apply labels before spawning the subprocess.

For `on_success`/`on_failure`, apply labels after subprocess exits.

### 13.4 Claim behavior

Before spawning a job:

1. locally claim job
2. refresh issue from GitHub
3. re-evaluate route predicate
4. check attempts
5. apply `on_start`, commonly adding `agent-running` and removing the source label
6. write context file
7. spawn subprocess

This limits duplicate processing and stale jobs.

## 14. Safety notes

- Issue text must never be interpolated directly into a shell command.
- Pass issue data via JSON files and environment variables.
- Commands should be configured as argv arrays, not shell strings.
- Each job should have a timeout.
- Logs should be captured per job.
- Workspaces should be per issue/job where coding agents modify files.
- V1 does not provide a security sandbox. The user is responsible for what subprocess commands do.

## 15. Go package layout

Proposed layout:

```text
cmd/issueq/main.go
internal/config
internal/github
internal/store
internal/store/sqlite
internal/poller
internal/router
internal/dispatcher
internal/runner
internal/actions
internal/model
internal/logging
migrations
```

### 15.1 Key interfaces

```go
type QueueStore interface {
    UpsertIssue(ctx context.Context, issue IssueSnapshot) error
    ListRoutableIssues(ctx context.Context) ([]IssueSnapshot, error)
    EnqueueJob(ctx context.Context, job JobCreate) (Job, bool, error)
    ClaimNextJob(ctx context.Context, runner RunnerInfo, caps Capabilities) (*Job, error)
    CompleteJob(ctx context.Context, jobID string, result JobResult) error
    FailJob(ctx context.Context, jobID string, failure JobFailure) error
    SkipJob(ctx context.Context, jobID string, reason string) error
    RenewLease(ctx context.Context, jobID string, runnerID string, until time.Time) error
    ReleaseExpiredLeases(ctx context.Context) error
    IncrementAttempts(ctx context.Context, issueKey string, generation int, routeName string) (int, error)
    IncrementTransitions(ctx context.Context, issueKey string) (int, error)
}
```

```go
type GitHubClient interface {
    ListOpenIssues(ctx context.Context, owner, repo string) ([]IssueSnapshot, error)
    GetIssue(ctx context.Context, owner, repo string, number int) (IssueSnapshot, error)
    AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error
    RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error
    CreateComment(ctx context.Context, owner, repo string, number int, body string) error
}
```

Keep these interfaces small and concrete. Avoid over-abstracting beyond the SQLite/Postgres migration path.

## 16. V1 implementation phases

### Phase 1: skeleton

- create Go module
- config loading
- SQLite migrations
- basic CLI
- issue/job tables

### Phase 2: poll and route

- GitHub list issues
- store snapshots
- route predicates
- enqueue jobs with dedupe keys
- inspect `jobs` and `issues`

### Phase 3: dispatch

- claim next pending job
- capacity checks
- write context JSON
- spawn subprocess
- timeout/reap
- mark success/failure

### Phase 4: GitHub actions

- apply on-start labels
- apply on-success/on-failure labels/comments
- re-fetch before dispatch
- skip stale jobs

### Phase 5: loop prevention

- route attempts
- max attempts
- transition count
- terminal actions

### Phase 6: polish

- logs per job
- `once`, `daemon`, `retry`, `cancel`
- systemd example
- sample config and task scripts

## 17. V2 notes

V2 is not needed initially, but v1 should not block it.

### 17.1 Centralized queue

Add a queue backend:

```yaml
queue:
  backend: postgres
  postgres:
    dsn_env: ISSUEQ_DATABASE_URL
```

Postgres enables:

- shared queue for multiple users/runners
- row-level locking
- cluster-wide leases
- global and per-kind concurrency limits
- better audit/history

Use `FOR UPDATE SKIP LOCKED` for job claiming.

### 17.2 Runner roles

Future daemon modes:

```bash
issueq daemon --role all
issueq daemon --role poller
issueq daemon --role dispatcher
```

In distributed mode, prefer one poller and many dispatchers.

### 17.3 Capabilities

Runners advertise capabilities:

```yaml
runner:
  name: ci-reviewer
  capabilities: [review]
```

Jobs can only be claimed by compatible runners.

### 17.4 GitHub-backed durability

Local counters disappear if SQLite is deleted. Future option: maintain a structured GitHub comment:

```md
<!-- issueq-state
{"generation":2,"attempts":{"code":1,"review":2},"transitions":5}
-->
```

This allows rebuilding local state from GitHub.

### 17.5 Webhooks

Polling is fine for v1. V2 may support GitHub webhooks:

```text
GitHub webhook → enqueue/reconcile issue immediately
```

Polling should remain as a reconciliation fallback.

### 17.6 More predicates

Possible future predicates:

- title/body regex
- author allow/deny lists
- assignees
- milestone
- comments contain command
- files/PR state after implementation
- custom external predicate command

Avoid adding a full expression language unless clearly needed.

## 18. Open questions

- Should result JSON override configured actions or only append to them?
- Should route attempts increment on claim or after `on_start` succeeds?
- Should a job failure always apply `on_failure`, or should certain exit codes map to specific outcomes?
- Should v1 support multiple repos or one repo per config?
- Should logs be stored in SQLite, files, or both?
- Should `agent-running` be global or route-specific, e.g. `agent-running-code`?

## 19. Initial recommendation

Build v1 as:

```text
Go + SQLite + YAML config + GitHub labels + bounded subprocess dispatcher
```

Use long polling, e.g. every 3-5 minutes. Keep it boring. Make the queue and store interfaces clean enough that Postgres can be added later, but do not implement distributed coordination until there is real usage pressure.
