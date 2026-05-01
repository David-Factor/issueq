# issueq v1 Design Spec

## 1. Purpose

`issueq` is a small local automation runner for GitHub issues.

It polls one GitHub repository on a configurable interval, stores issue snapshots in SQLite, evaluates simple label-based route predicates, enqueues matching jobs, and dispatches bounded subprocesses such as triage scripts, coding agents, review agents, and cleanup tasks.

V1 is local-first. GitHub labels/comments are the durable, human-visible workflow state. SQLite is the local queue, attempt counter, lease store, and audit log.

This document is the product/technical specification. The phased build plan lives separately in [`v1-implementation-plan.md`](v1-implementation-plan.md).

## 2. Definitions

- **Issue snapshot**: Local SQLite copy of selected GitHub issue fields.
- **Route**: Configured predicate plus job definition. Example: `label agent-ready -> run code job`.
- **Job**: A queued execution request created by a route.
- **Dispatcher**: The long-running supervisor that claims queued jobs and spawns subprocesses.
- **Subprocess**: Configured command invoked for a job. It is not a long-lived worker.
- **Runner**: One running `issueq` process with a stable `runner.id`/`runner.name`.
- **Lease**: Time-bounded local claim on a queued job.
- **Generation**: Per-issue counter used to reset route attempts after human intervention. Schema supports it in v1; automatic reset may come later.
- **Terminal label**: Label that prevents further automation for an issue.

## 3. Goals and non-goals

### 3.1 Goals

V1 must:

- Run as a CLI and as a long-running daemon.
- Poll GitHub issues every few minutes or on demand.
- Support one configured GitHub repository per config file.
- Route issues to jobs using simple label include/exclude predicates.
- Store issue snapshots, jobs, leases, attempts, and events in SQLite.
- Spawn configured subprocesses only when matching work exists and capacity is available.
- Enforce global and per-route concurrency limits.
- Track attempts and prevent infinite automation loops.
- Update GitHub labels/comments on job start, success, failure, and terminal states; record skipped stale jobs locally.
- Pass issue/job context to subprocesses through files and environment variables.
- Avoid shell interpolation of issue content.
- Keep the architecture compatible with a future centralized queue backend.

### 3.2 Non-goals

V1 will not include:

- Web UI.
- Distributed queue.
- Required webhook server.
- Complex workflow engine.
- Redis/Kubernetes/external queue dependency.
- Strong multi-runner coordination.
- Security sandbox for untrusted subprocesses.
- Multiple repositories in a single config.
- Rich predicate expression language.

## 4. System model

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

Design separation:

1. **GitHub labels** define durable public workflow state.
2. **SQLite queue** defines local execution backlog and operational state.
3. **Subprocess jobs** perform configurable work.

The queue is a projection of GitHub state plus local execution bookkeeping. If SQLite is deleted, some counters/history are lost in v1, but routeable work can be rebuilt by polling GitHub.

## 5. Workflow labels

`issueq` does not hardcode label names. Labels are configured in routes and actions.

Recommended starter labels:

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

Recommended terminal/blocking labels:

```text
agent-done
agent-failed
agent-needs-human
manual-only
```

Routes should normally exclude every terminal label.

A single global `agent-running` label is the v1 default recommendation. Route-specific running labels such as `agent-running-code` can be implemented purely through config if desired.

## 6. Example workflows

### 6.1 Happy path

```text
agent-triage
  → triage job
  → agent-ready
  → code job
  → agent-review
  → review job
  → agent-done
```

### 6.2 Needs information

```text
agent-triage
  → triage job exits nonzero
  → agent-needs-info
```

### 6.3 Review loop with circuit breaker

```text
agent-ready
  → code
  → agent-review
  → review exits nonzero, returns to agent-ready
  → code
  → agent-review
  → review exits nonzero again
  → ... max attempts exceeded ...
  → agent-needs-human + agent-failed
```

## 7. CLI specification

All commands accept `--config <path>`. Default config path may be `./issueq.yaml`.

When config is loaded from a file, relative `queue.sqlite.path` and `workdir.path` values are resolved relative to the directory containing that config file. Explicit relative subprocess executables in `job.command[0]` (`./...` or `../...`) are also resolved relative to the config file directory. Bare commands such as `bash`, `python3`, or `code-agent` are left unchanged and resolved through `PATH`; command arguments are not rewritten.

### 7.1 Required v1 commands

```bash
issueq daemon --config issueq.yaml
issueq once --config issueq.yaml
issueq poll --config issueq.yaml
issueq route --config issueq.yaml
issueq dispatch --config issueq.yaml
issueq jobs --config issueq.yaml
issueq issues --config issueq.yaml
```

### 7.2 `daemon`

Runs until interrupted:

1. release expired local leases
2. poll GitHub if poll interval elapsed
3. route issue snapshots into jobs
4. dispatch pending jobs while capacity exists
5. reap finished child processes
6. sleep briefly

Default poll interval: `3m`.

### 7.3 `once`

Runs one reconciliation cycle:

1. poll
2. route
3. dispatch available jobs

Final v1 behavior:

- `issueq once` runs a bounded poll/route/dispatch reconciliation wave and waits for jobs it spawned to finish.
- `issueq once --no-wait` is intentionally unsupported until durable detached CLI semantics are explicitly designed.

### 7.4 Debug commands

`jobs` prints local jobs. `issues` prints local issue snapshots.

Output may start as human-readable tables. A `--json` flag is desirable before v1 is considered complete.

Future commands:

```bash
issueq retry <job-id>
issueq cancel <job-id>
issueq run-one --kind code
issueq reset <issue-key>
issueq doctor
```

## 8. Configuration specification

V1 uses YAML.

```yaml
runner:
  name: david-exedev
  capabilities: [triage, code, review]
  env:
    inherit: false
    pass: [PATH, HOME]

queue:
  backend: sqlite
  sqlite:
    path: ./issueq.db
  max_global_concurrency: 2
  lease_duration: 30m

workdir:
  path: ./.issueq

polling:
  interval: 3m

github:
  host: github.com
  owner: example-org
  repo: example-repo
  token_env: GITHUB_TOKEN

terminal_labels:
  - agent-done
  - agent-failed
  - agent-needs-human
  - manual-only

workflow:
  max_transitions_per_issue: 10
  on_transitions_exceeded:
    labels_remove: [agent-triage, agent-ready, agent-review, agent-running]
    labels_add: [agent-failed, agent-needs-human]
    comment: "Automation stopped after too many state transitions. Human intervention is needed."

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
      priority: 10
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
      on_attempts_exceeded:
        labels_remove: [agent-running, agent-triage]
        labels_add: [agent-needs-human, agent-failed]
        comment: "Automation stopped after too many triage attempts."

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
      priority: 20
      env:
        pass: [AGENT_GITHUB_TOKEN, ANTHROPIC_API_KEY]
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
      priority: 15
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

### 8.1 Config validation

Startup must fail fast if:

- no GitHub owner/repo is configured
- token env var is missing for commands that contact GitHub
- SQLite path is empty
- `runner.env.pass` or route-level `job.env.pass` contains an invalid environment variable name
- `github.token_env` is included in any subprocess env pass-through list
- route names are empty or duplicated
- route kind is empty
- command is empty for a dispatchable job
- timeout is missing or non-positive
- concurrency is missing or non-positive
- max attempts is missing or non-positive
- label include/exclude contains duplicate conflicts within one predicate
- action contains the same label in both add and remove lists

Warnings are acceptable for missing recommended terminal labels.

## 9. Routing specification

Routes map issue predicates to job specs.

V1 predicate shape:

```yaml
when:
  labels_include: [...]
  labels_exclude: [...]
```

A predicate matches when:

- every `labels_include` label is present on the issue
- no `labels_exclude` label is present on the issue
- issue state is `open`
- runner capabilities include the job kind, if capabilities are configured

V1 should avoid a rich expression language.

Possible future predicates:

```yaml
when:
  title_matches: "..."
  body_contains: "..."
  author_in: [...]
  assignee_in: [...]
  milestone: "..."
```

### 9.1 Router idempotency

Repeated polls/routes must not create unbounded duplicates.

Each enqueued job has a unique `dedupe_key`.

Recommended v1 dedupe key:

```text
{issue_key}:{route_name}:{label_hash}:{github_updated_at}
```

Where `label_hash` is a stable hash of sorted labels.

The store enforces `unique(dedupe_key)`.

### 9.2 Stale jobs

Before dispatching, `issueq` must fetch the latest issue from GitHub and re-evaluate the route predicate.

If the latest issue no longer matches, the job is marked `skipped` with a reason. No subprocess is spawned.

## 10. Queue and SQLite model

### 10.1 Issue key

Use:

```text
github.com/{owner}/{repo}#{number}
```

Store GitHub `node_id` when available.

### 10.2 Required tables

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
  stdout_path text,
  stderr_path text,
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

### 10.3 Job statuses

```text
pending     queued and available at/after available_at
running     claimed by a runner and currently active or lease-held
succeeded   subprocess exited 0 and success actions were applied
failed      subprocess exited nonzero or action application failed
skipped     job became stale or ineligible before execution
dead        not retryable, usually attempts/transitions exceeded
cancelled   manually cancelled
```

### 10.4 Leases

Jobs use leases even in local SQLite:

- `locked_by`
- `lease_until`
- `started_at`

On startup and periodically, expired `running` jobs are released.

V1 policy:

- Running jobs also carry `runner_instance_id` and, for wrapper-launched work, durable launch metadata such as supervisor kind, launch token, launch state, artifact paths, and timeout.
- Expired rows without durable launch metadata may be requeued only when the owner heartbeat is stale or missing.
- Expired rows with durable wrapper metadata are inspected/adopted only after verified stale-owner recovery. If launch identity cannot be verified, the row remains `running` and operator-visible, usually with `launch_state = unknown`; issueq must not blindly requeue, PID-kill, or finalize it.

## 11. Dispatch specification

There are no idle coding/review workers. The dispatcher is a supervisor that starts child processes only when work exists.

Dispatch flow:

1. compute currently running counts by global and route
2. select next pending job by priority then creation time
3. ensure runner capability matches job kind
4. ensure capacity is available
5. locally claim job with lease
6. fetch latest issue from GitHub
7. re-evaluate route predicate
8. increment route attempt
9. if attempts exceed max, apply `on_attempts_exceeded`, mark `dead`, stop
10. apply `on_start` actions
11. write context JSON
12. launch the internal `issueq job-wrapper`, which invokes the configured subprocess
13. capture stdout/stderr to files
14. enforce timeout
15. parse optional result JSON
16. apply success/failure actions
17. mark job final status and record event

Capacity limits:

- `queue.max_global_concurrency`
- `route.job.concurrency`
- runner capabilities

## 12. Attempts and loop prevention

V1 must implement loop prevention.

### 12.1 Route attempts

Attempt key:

```text
issue_key + generation + route_name
```

Attempts increment after the latest issue still matches and before `on_start` is applied. This means failures while applying `on_start` still count.

If incremented attempts exceed `max_attempts`, no subprocess is spawned. Apply `on_attempts_exceeded`, mark job `dead`, and comment on GitHub if configured.

### 12.2 Transition count

Every successful action application that changes workflow labels increments `issue_state.transition_count`.

If `transition_count` exceeds `workflow.max_transitions_per_issue`, apply `workflow.on_transitions_exceeded` and mark the current job `dead`.

### 12.3 Generation

V1 schema includes `generation`, but automatic generation reset is optional.

Future reset triggers:

- human removes `agent-needs-human` and adds `agent-ready`
- `issueq reset <issue-key>`
- structured state comment edited/rebuilt

## 13. Subprocess contract

### 13.1 Invocation

Configured command:

```yaml
command: ["./tasks/code.sh"]
```

Actual invocation, when the config file lives beside `tasks/`:

```bash
/path/to/config-dir/tasks/code.sh /path/to/context.json /path/to/result.json
```

Commands are argv arrays, not shell strings. Only an explicit relative executable in `command[0]` is resolved relative to the config file directory; relative arguments are passed through unchanged.

### 13.2 Environment

`issueq` always sets job metadata environment variables for subprocesses. User environment pass-through is controlled by config.

Recommended default:

```yaml
runner:
  env:
    inherit: false
    pass: [PATH, HOME]
```

Route-level additions are allowed:

```yaml
routes:
  - name: code
    job:
      env:
        pass: [AGENT_GITHUB_TOKEN, ANTHROPIC_API_KEY]
```

If `runner.env.inherit` is `false`, subprocesses receive only:

- the explicitly passed env vars
- the `ISSUEQ_*`/`GITHUB_*` metadata variables listed below

If `runner.env.inherit` is `true`, subprocesses inherit the parent process environment. This is convenient for local experimentation but riskier because it may expose tokens to coding agents or scripts.

Set at least these metadata variables:

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

### 13.3 Context JSON

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

### 13.4 Result JSON

Subprocesses may write:

```json
{
  "comment": "Implemented in PR #456.",
  "labels_add": ["agent-review"],
  "labels_remove": ["agent-running"]
}
```

V1 result policy:

- exit code `0`: base action is configured `on_success`
- nonzero exit code or timeout: base action is configured `on_failure`
- if result JSON exists, merge it after the base action
- comments are concatenated with a blank line, base comment first
- label operations are normalized after merge; if a label appears in both add and remove after merge, the result-file operation wins
- malformed result JSON makes the job fail and applies `on_failure` without result-file actions

### 13.5 Direct queue appends

V1 does not support direct queue appends from result JSON.

User-visible workflow follow-ups must be represented by GitHub labels and routed by the router. For example, a successful code job should add `agent-review`; the next poll/route cycle then creates the review job.

Local/internal follow-up jobs such as cleanup may be reconsidered after v1.

## 14. GitHub interaction

### 14.1 Authentication

`issueq` authenticates to GitHub with a token read from `github.token_env`, usually `GITHUB_TOKEN`.

Recommended token type:

- GitHub fine-grained personal access token
- selected repository only
- minimum permissions:
  - Metadata: read
  - Issues: read/write

`issueq` uses this token only for issue automation:

- list/fetch issues
- add/remove issue labels
- create issue comments

`issueq` must not write the token to SQLite, context JSON, result JSON, stdout/stderr logs, or job events. Logs must not print request authorization headers.

Subprocesses that create branches, push code, or open PRs manage their own credentials. Recommended pattern:

```yaml
routes:
  - name: code
    job:
      env:
        pass: [AGENT_GITHUB_TOKEN]
```

The `issueq` GitHub token and coding-agent GitHub credentials are separate credentials. `github.token_env` must not be passed to subprocesses. If a coding agent needs GitHub access, configure a different env var such as `AGENT_GITHUB_TOKEN`.

### 14.2 Polling

Recommended v1 implementation:

- list open issues for the configured repo
- store snapshots
- filter locally through route predicates

GitHub search query support is optional for v1.

### 14.3 Applying actions

Actions:

```yaml
labels_add: []
labels_remove: []
comment: "..."
```

Application order:

1. refresh issue
2. remove labels
3. add labels
4. create comment
5. refresh/update local issue snapshot
6. record event

Removal before addition makes state transitions like `agent-ready -> agent-running` predictable.

If label removal fails because the label is absent, treat it as success.

### 14.4 Start claim

`on_start` is the GitHub-visible claim. For code jobs it should usually remove source labels such as `agent-ready` and add `agent-running`.

Because v1 is not a distributed queue, this is best-effort coordination only. Dispatch must always re-fetch and re-check before starting.

## 15. Safety and filesystem behavior

- Issue text must never be interpolated directly into a shell command.
- Commands must be argv arrays.
- Issue/job data is passed via JSON files and environment variables.
- Each job must have a timeout.
- `issueq`'s GitHub token must not be persisted or logged.
- Subprocess env pass-through should default to a minimal allowlist rather than full environment inheritance.
- Local DB and workdir should be treated as private because context, logs, and result files may contain sensitive issue or agent output.
- Recommended file permissions: SQLite DB `0600`; workdir `0700` where practical.
- Stdout/stderr are captured to files under `workdir.path`.
- Context/result files are written under `workdir.path/jobs/<job-id>/`.
- Coding-agent task scripts should create per-issue or per-job worktrees/clones if they edit repositories.
- V1 does not provide a security sandbox.

## 16. Go architecture

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

### 16.1 Initial Go dependencies

Recommended v1 dependencies:

```text
CLI:        github.com/spf13/cobra
Config:     gopkg.in/yaml.v3
SQLite:     modernc.org/sqlite
GitHub:     github.com/google/go-github/v66/github
IDs:        github.com/oklog/ulid/v2
Logging:    standard library log/slog
```

Use `modernc.org/sqlite` to avoid CGO for local builds. Keep dependency use modest and hidden behind internal interfaces where practical.

### 16.2 Interfaces

```go
type QueueStore interface {
    UpsertIssue(ctx context.Context, issue IssueSnapshot) error
    ListRoutableIssues(ctx context.Context) ([]IssueSnapshot, error)
    EnqueueJob(ctx context.Context, job JobCreate) (Job, bool, error)
    ClaimNextJob(ctx context.Context, runner RunnerInfo, caps Capabilities) (*Job, error)
    CompleteJob(ctx context.Context, jobID string, result JobResult) error
    FailJob(ctx context.Context, jobID string, failure JobFailure) error
    SkipJob(ctx context.Context, jobID string, reason string) error
    MarkDead(ctx context.Context, jobID string, reason string) error
    RenewLease(ctx context.Context, jobID string, runnerID string, until time.Time) error
    ReleaseExpiredLeases(ctx context.Context) error
    IncrementAttemptsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string, generation int, routeName string) (int, error)
    IncrementTransitionsForJob(ctx context.Context, jobID, runnerInstanceID, issueKey string) (int, error)
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

These interfaces exist to keep GitHub, routing, dispatch, and storage testable and to avoid blocking a future Postgres backend.

## 17. Observability

V1 should provide enough introspection to debug local automation:

- structured logs to stderr
- job events in SQLite
- stdout/stderr files per job
- `issueq jobs` and `issueq issues`
- job status, attempts, timestamps, paths, and last error visible in CLI output

## 18. V2 notes

V2 is not part of the initial build but should remain feasible.

### 18.1 Centralized queue

Add backend config:

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
- global/per-route concurrency limits
- stronger audit/history

Use `FOR UPDATE SKIP LOCKED` for job claiming.

### 18.2 Runner roles

Future daemon modes:

```bash
issueq daemon --role all
issueq daemon --role poller
issueq daemon --role dispatcher
```

In distributed mode, prefer one poller and many dispatchers.

### 18.3 Capabilities

Runners advertise capabilities:

```yaml
runner:
  name: ci-reviewer
  capabilities: [review]
```

Jobs can only be claimed by compatible runners.

### 18.4 GitHub-backed durability

Future option: maintain a structured GitHub comment:

```md
<!-- issueq-state
{"generation":2,"attempts":{"code":1,"review":2},"transitions":5}
-->
```

This allows rebuilding local counters from GitHub.

### 18.5 Webhooks

Polling is enough for v1. V2 may add:

```text
GitHub webhook → enqueue/reconcile issue immediately
```

Polling should remain as a reconciliation fallback.

### 18.6 More predicates and outcomes

Possible future additions:

- title/body regex
- author allow/deny lists
- assignees/milestone
- comments contain command
- custom external predicate command
- specific exit-code outcome mapping, e.g. `2 -> needs-info`
- direct queue append support for internal follow-ups

Avoid a full expression language until necessary.

## 19. Requirement checklist

A v1 implementation is complete when it satisfies these requirements:

- **R1 Config**: load, validate, and expose YAML config matching this spec.
- **R2 SQLite**: create and migrate required local tables.
- **R3 Poll**: list open issues for one repo and upsert snapshots.
- **R4 Route**: evaluate label predicates and enqueue deduplicated jobs.
- **R5 Inspect**: list jobs and issues from local state.
- **R6 Dispatch**: claim jobs, enforce leases/capacity, and spawn argv-array subprocesses.
- **R7 Context**: write context/result paths and expected environment variables.
- **R8 Results**: handle exit code, timeout, stdout/stderr capture, and optional result JSON without direct queue append support.
- **R9 GitHub actions**: apply start/success/failure/terminal labels and comments.
- **R10 Staleness**: re-fetch and skip stale jobs before execution.
- **R11 Loop prevention**: enforce route max attempts and max transitions.
- **R12 Safety/Auth**: avoid shell interpolation, require timeouts, do not persist/log tokens, and use explicit subprocess env pass-through.
- **R13 Observability**: log events and provide useful `jobs`/`issues` output.
- **R14 Future compatibility**: keep queue behind an interface and use portable IDs/leases.
