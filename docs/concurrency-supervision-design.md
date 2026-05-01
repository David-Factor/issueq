# Concurrency and supervision design

> **Archived/stale after H5/H6.** This document records the pre-wrapper concurrency-supervision design. The current implementation is wrapper-only durable supervision described in [`execution-supervisor-spec.md`](execution-supervisor-spec.md), with migration status in [`execution-supervisor-migration-plan.md`](execution-supervisor-migration-plan.md). Do not use this document as current implementation guidance; sections about `runner.Start` / `runner.Wait`, daemon-owned handles, and compatibility `runner.Run` are obsolete.


This note closes the remaining pre-Phase 9 design questions for documented follow-ups #5, #6, #7, and #12 in `docs/code-review-followups.md`.

## Goals

- Run subprocess jobs concurrently up to `queue.max_global_concurrency` and per-route `job.concurrency`.
- Avoid duplicate execution of still-running jobs when leases expire.
- Kill whole subprocess trees on timeout, cancellation, and daemon shutdown.
- Keep daemon polling/routing responsive while jobs run.
- Preserve current blocking `issueq once` behavior as a bounded reconciliation command.

## Non-goals for this pass

- Durable detached background execution for `once --no-wait`.
- Cross-host distributed scheduling beyond the SQLite lease/heartbeat model.
- Perfect process liveness detection via PID alone.
- Windows-equivalent process-tree killing semantics.

## CLI semantics

### `issueq once`

`once` is a bounded reconciliation wave:

1. release/recover safely reclaimable expired work,
2. poll GitHub,
3. route local issue snapshots into jobs,
4. spawn eligible jobs up to configured capacity,
5. renew active leases while those spawned jobs run,
6. reap/finalize the jobs spawned by this wave,
7. return.

It does not wait forever for future jobs that become available after the wave starts. The wave frontier is the set of jobs that are eligible at the start of dispatching plus any jobs already claimed by this wave. `once` should refill freed capacity until that initial eligible backlog is drained, not merely spawn one capacity-sized batch and return while initial backlog remains pending.

### `issueq once --no-wait`

Remain disabled for now. It should only be re-enabled after issueq can durably persist enough child supervision state for a later daemon/process to renew, reap, and finalize detached work safely.

### `issueq dispatch`

`dispatch` is a bounded dispatch/supervision wave without poll or route:

1. heartbeat the current runner instance,
2. recover safely reclaimable expired work,
3. claim and spawn eligible queued jobs according to capacity,
4. renew active leases,
5. reap/finalize jobs owned by this runner instance,
6. return when the dispatch wave is complete.

The dispatch wave uses the same frontier rule as `once`: continue refilling capacity until jobs eligible at wave start are either finalized or no longer claimable by this runner.

It remains GitHub-aware by default and keeps the existing explicit `--local-no-github` fixture exception. Local fixture dispatch uses the same supervision machinery but skips GitHub refresh/actions/attempt enforcement.

### `issueq daemon`

Daemon is the long-lived supervisor. It should not block the polling cadence on one synchronous dispatch cycle. Its loop should:

1. heartbeat the current runner instance,
2. recover safely reclaimable expired work,
3. poll GitHub on the configured interval,
4. route issue snapshots,
5. spawn jobs within remaining capacity,
6. renew active leases,
7. reap completed jobs,
8. respond promptly to cancellation/shutdown.

## Runner identity and heartbeat

Use two identifiers:

- `runner_id`: stable logical runner name, currently derived from `runner.name` or `issueq-local`.
- `runner_instance_id`: unique process instance ID generated at process start, e.g. `runner_id` plus ULID.

Add a SQLite table similar to:

```sql
CREATE TABLE IF NOT EXISTS runner_heartbeats (
  runner_instance_id TEXT PRIMARY KEY,
  runner_id TEXT NOT NULL,
  pid INTEGER,
  started_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL
);
```

Extend job locks to record `runner_instance_id` in addition to existing `locked_by`. `locked_by` remains the stable runner ID for operator display/backward compatibility.

Heartbeat policy:

- Write heartbeat at process start.
- Refresh heartbeat periodically while daemon/once supervises active work.
- Use the same heartbeat path before lease recovery and before spawning/reaping loops.
- Heartbeat freshness threshold should be based on lease duration. Proposed default: stale if `heartbeat_at < now - lease_duration`.
- Heartbeat rows should be deleted on clean shutdown after active jobs are finalized.
- Stale heartbeat rows should be pruned opportunistically after they are older than a conservative retention window, e.g. `24h`.
- Rollout note: stop old issueq daemons before applying the heartbeat migration. Old `running` jobs with no `runner_instance_id` remain reclaimable for backward compatibility.

## Lease renewal

Every active child should have its lease renewed while it is still supervised by the current process.

Policy:

- Renewal interval: `lease_duration / 3`.
- Clamp renewal interval to a minimum of `5s` and maximum of `1m`.
- Renew to `now + lease_duration`.
- Renewal should match both `job_id` and `runner_instance_id` so one process cannot accidentally renew another process's claim.
- On renewal failure:
  - record/log the error,
  - keep supervising the child,
  - surface the error from `once`/daemon loop if it persists or prevents correct finalization,
  - do not intentionally requeue the active job from the same process.

Add a store method similar to:

```go
RenewJobLease(ctx context.Context, jobID, runnerInstanceID string, leaseDuration time.Duration) error
```

The implementation must update only rows matching `id = ? AND status = 'running' AND runner_instance_id = ?`. A missing row match means ownership was lost.

## Ownership-guarded mutations

All post-claim job mutations must be ownership-guarded, not just lease renewal. This includes:

- persisting PID/artifact paths after start,
- persisting attempts,
- renewing leases,
- finalizing jobs,
- marking jobs skipped/dead/cancelled/failed,
- any future heartbeat/reap metadata updates.

Store methods should either accept `runner_instance_id` or have ownership-specific variants. Updates must match `status = 'running' AND runner_instance_id = ?` where appropriate. If no row is affected, return a typed sentinel such as `ErrLostLease` or `ErrNotOwner`.

Ownership loss policy:

- Before any external side effect after claim, including GitHub label/comment actions and route-attempt increment/persistence, re-check or otherwise know that ownership is still held.
- Do not apply `on_start`, terminal GitHub actions, success/failure actions, or finalization after ownership is lost.
- Stop supervising/reaping bookkeeping for that job locally.
- Log/record the lost ownership where possible without mutating the job row.
- The new owner or recovery path is responsible for retry/finalization.


## Expired lease recovery

Use heartbeat-aware recovery rather than PID-only checks.

A `running` job with an expired lease is reclaimable only if:

- it has no `runner_instance_id`, for backward compatibility with old rows; or
- its owning `runner_instance_id` has no heartbeat row; or
- its owning heartbeat is stale.

A job owned by the current process and present in the in-memory active set must not be requeued by that process, even if a lease appears expired because of timing or a transient DB issue.

Recovery behavior:

- Set status back to `pending`.
- Clear `locked_by`, `runner_instance_id`, `lease_until`, and `pid`.
- Preserve artifact paths for audit unless a new run overwrites them.
- Set `last_error = 'lease expired'` or a similarly clear recovery message.
- Do not increment attempts during recovery; attempts increment only when a job is actually dispatched again.

Add a store method similar to:

```go
ReleaseExpiredLeases(ctx context.Context, now time.Time, staleHeartbeatBefore time.Time, currentRunnerInstanceID string, activeJobIDs []string) (int, error)
```

SQL semantics:

- consider only `status = 'running'` rows with `lease_until < now`,
- exclude rows whose `runner_instance_id` equals the current process and whose job ID is in the active set,
- requeue rows with null/empty `runner_instance_id`, missing heartbeat, or stale heartbeat,
- do not requeue rows with fresh heartbeat,
- return rows affected and insert recovery events when practical.

Store writes should be safe under one process with multiple active children and under multiple issueq processes sharing a SQLite DB:

- Configure SQLite for practical local concurrency, e.g. WAL mode and a busy timeout.
- Keep write transactions short.
- Use an atomic claim transaction with a write lock (`BEGIN IMMEDIATE` or equivalent) so capacity checks and status updates are serialized.
- Capacity checks must happen inside the same locked transaction that claims the job.
- Prefer a small number of supervisor goroutines and funnel store writes through clear methods rather than ad hoc concurrent SQL.

## Migration strategy

Current embedded SQLite migrations live under `internal/store/sqlite/migrations`. The migration mechanism applies embedded SQL files directly and does not yet track `schema_migrations`, so new migrations must be idempotent.

Options:

- Add a real `schema_migrations` table and versioned migration runner before adding non-idempotent DDL; or
- implement idempotent schema updates in Go using `PRAGMA table_info` checks before `ALTER TABLE`.

For this refactor, do not add a plain repeatable `ALTER TABLE jobs ADD COLUMN ...` migration unless the migrator can safely skip already-applied changes.

Expected indexes:

- `jobs(status, lease_until, runner_instance_id)` for lease recovery,
- `jobs(status, route_name)` for capacity checks,
- `runner_heartbeats(runner_instance_id, heartbeat_at)` for stale heartbeat lookup.

## Dispatcher architecture

Split the current synchronous runner path into start/wait/reap primitives while preserving `runner.Run` as a compatibility wrapper during refactor.

Suggested runner API shape:

```go
type Handle struct {
  Job model.Job
  Issue model.IssueSnapshot
  Paths runner.Paths
  PID int
  Done <-chan runner.Result
  Cancel func(cause CancelCause)
}

type CancelCause string

const (
  CancelCauseShutdown CancelCause = "shutdown"
  CancelCauseUser     CancelCause = "user"
)

Start(ctx context.Context, cfg config.Config, route config.RouteConfig, job model.Job, issue model.IssueSnapshot, runnerInfo model.RunnerInfo) (*Handle, error)
Wait(ctx context.Context, handle *Handle) runner.Result
Run(ctx context.Context, ...) runner.Result // implemented via Start + Wait
```

`runner.Result` should distinguish timeout from cancellation, e.g. with `TimedOut bool`, `Cancelled bool`, and/or a typed `Cause`. Shutdown cancellation should skip normal success/failure GitHub actions and finalize as `cancelled`.

Dispatcher/supervisor responsibilities:

- Claim and start jobs until no capacity remains.
- Hold global/per-route capacity from claim through finalization, including GitHub refresh, `on_start`, subprocess execution, result actions, and terminalization.
- Track active job state by job ID, including pre-child and post-child phases as well as subprocess handles.
- Renew leases for active running jobs.
- Reap completed handles and apply success/failure actions.
- Finalize jobs exactly once and only while ownership is still held.
- Keep per-route and global capacity counts based on active jobs plus database-visible running jobs.

Per-route concurrency is route-name based. Remove the current kind fallback/counting behavior so two routes that share the same job kind do not block each other unless global capacity is exhausted.

After a successful subprocess `Start`, the supervisor must immediately persist context/result/stdout/stderr paths and PID with an ownership guard so operators can inspect running jobs. Start failures should finalize through the common post-claim failure path. Log files should be owned/closed by the runner handle; the supervisor only persists their paths and reaps the final result.

`Dispatch` should remain available for local fixture dispatch, but internally it can use the same supervisor machinery with a nil GitHub client and no GitHub attempt enforcement.

## GitHub-aware dispatch flow per job

The existing correctness rules remain:

1. claim job,
2. load local issue,
3. refresh GitHub issue,
4. upsert refreshed issue,
5. re-check route predicate,
6. increment/persist route attempts,
7. enforce max attempts,
8. apply `on_start`,
9. check transition limit if labels changed,
10. start subprocess,
11. reap subprocess,
12. parse/merge result JSON,
13. apply success/failure actions,
14. check transition limit if labels changed,
15. finalize job.

Any error after claim must finalize through the common post-claim failure path unless the job has already been finalized as `skipped`, `dead`, `cancelled`, or `succeeded`.

## Process tree handling

Unix/Linux implementation:

- Start subprocesses in a new process group/session via a Unix build-tag helper using `exec.Cmd.SysProcAttr`.
- Do not rely on `exec.CommandContext`'s default cancellation to clean up the tree; explicitly signal `-pgid` on timeout/cancel.
- Prefer graceful termination first if practical, then force kill after a short grace period; or use immediate kill for the first implementation if simpler and tests document it.

Non-Unix implementation:

- Keep current direct process kill fallback.
- Ensure the code compiles.
- Unix process-tree behavior is the tested/guaranteed path for v1.

Timeout vs cancellation:

- Job timeout => `failed` with timeout error.
- Runner shutdown/context cancellation => `cancelled` with `last_error = 'runner shutting down'` or equivalent.
- Process start/wait failures => `failed`.

## Shutdown behavior

Daemon should be an event loop with independent timers rather than a single poll-ordered sequence:

- heartbeat timer,
- lease-renewal timer,
- reap/refill timer,
- poll/route timer,
- shutdown signal.

Renewal and reaping must be able to run more frequently than `polling.interval`, and reaping should refill freed capacity promptly. Shutdown finalization should use a bounded cleanup context derived from `context.Background()`, not the already-cancelled daemon context.

On SIGINT/SIGTERM:

1. stop polling, routing, and claiming new jobs,
2. cancel active subprocess contexts,
3. kill active process groups,
4. reap children,
5. finalize active jobs as `cancelled`,
6. stop heartbeating after active jobs are finalized.

No indefinite drain in v1. A configurable drain timeout can be added later.

## Store changes

Expected schema/store additions:

- `jobs.runner_instance_id TEXT` column.
- `runner_heartbeats` table.
- `HeartbeatRunner(ctx, runnerInstanceID, runnerID string, pid int, now time.Time) error`.
- `RenewJobLease(ctx, jobID, runnerInstanceID string, leaseDuration time.Duration) error`.
- Update `ClaimNextJob` to store `runner_instance_id`.
- Update `ReleaseExpiredLeases` to requeue only expired jobs whose owner heartbeat is absent/stale.

Function signatures may change to pass a `RunnerIdentity` struct instead of just `runnerID`.

## Acceptance tests

Testing hooks required for fast deterministic tests:

- injectable clock/timers for heartbeat and renewal intervals,
- fake process starter/handles for dispatcher and daemon supervisor tests,
- store tests using two store instances where practical to exercise SQLite locking/ownership.

Minimum automated coverage before Phase 9 E2E:

- With global concurrency `2`, two long-running jobs overlap in time.
- With global concurrency `1`, jobs remain serial.
- With route concurrency `1`, same-route jobs do not overlap while another route can run if global capacity allows.
- Active jobs renew leases before expiry.
- Expired jobs owned by stale/missing runner heartbeats are requeued.
- Expired jobs owned by a fresh heartbeat are not requeued.
- Current process does not requeue its own active job even if lease timing is tight.
- Timeout kills a child and grandchild on Unix.
- Daemon shutdown cancels active jobs and finalizes them as `cancelled`.
- `once` returns after the jobs spawned in its reconciliation wave complete.
- Lost ownership prevents artifact updates, GitHub actions, and finalization by the stale owner.
- Two concurrent stores/process simulators cannot overclaim beyond global/per-route capacity.
- Shutdown finalization succeeds using a cleanup context even after parent daemon context is cancelled.
- Run `go test -race ./...` where practical after the concurrency refactor.
- Existing gates still pass: `go test ./...`, `go vet ./...`, `go run ./cmd/issueq --help`, `go build ./cmd/issueq`, `gofmt`, and `git diff --check`.

## Implementation stages

1. Add Unix process group helpers and timeout/cancel process-tree tests.
2. Split runner start/wait/reap while preserving `runner.Run` behavior.
3. Add runner instance heartbeat schema/store methods and lease renewal methods.
4. Refactor dispatcher into a supervisor that can spawn, renew, and reap concurrently.
5. Update daemon to use the supervisor loop without blocking polling cadence.
6. Reconsider whether `once --no-wait` can be safely re-enabled; keep disabled unless durable detached supervision is complete.
