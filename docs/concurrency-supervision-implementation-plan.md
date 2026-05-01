# Concurrency supervision implementation plan

> **Archived/stale after H5/H6.** This plan describes the pre-wrapper direct-runner concurrency refactor. The current production runtime is the wrapper-only supervisor path. Use [`execution-supervisor-spec.md`](execution-supervisor-spec.md) and [`execution-supervisor-migration-plan.md`](execution-supervisor-migration-plan.md) for current architecture/status. References here to direct runner process APIs are historical.


This plan breaks `docs/concurrency-supervision-design.md` into reviewable implementation phases. It addresses the remaining documented follow-ups before Phase 9 manual E2E:

- #5 true configured concurrency,
- #6 lease renewal and duplicate-running-job avoidance,
- #7 process-tree killing,
- #12 daemon nonblocking supervisor loop.

Each phase should end with a commit and the standard gates unless explicitly noted.

## Standard gates

Run after every phase:

```bash
gofmt -w .
go test ./...
go vet ./...
go run ./cmd/issueq --help
go build ./cmd/issueq
git diff --check
```

Run targeted smoke tests when CLI behavior changes:

```bash
go run ./cmd/issueq --config examples/issueq.yaml config-check
go run ./cmd/issueq --config examples/issueq.yaml jobs --json
go run ./cmd/issueq --config examples/issueq.yaml issues --json
go run ./cmd/issueq --config examples/issueq.yaml once --no-wait # expected unsupported failure; do not re-enable in this pass
```

Run `go test -race ./...` near the end of the refactor if practical.

## Phase C0 — SQLite/store supervision foundation

### Goals

Create durable ownership, heartbeat, claim, and recovery primitives before concurrent supervision touches subprocesses.

### Work items

- Add an idempotent migration strategy compatible with the current embedded migrator.
  - Either introduce `schema_migrations`, or implement Go-side idempotent DDL checks with `PRAGMA table_info` before `ALTER TABLE`.
  - Do not add repeat-failing `ALTER TABLE` SQL.
  - Embedded migrations live under `internal/store/sqlite/migrations`; also check whether the top-level `migrations/` copy should be updated, removed, or documented as obsolete.
- Add `jobs.runner_instance_id`.
- Update all job model/scan/list paths:
  - `model.Job`,
  - all `SELECT` column lists,
  - `scanJob`,
  - `ListJobs` / `jobs --json` output.
- Add `runner_heartbeats` table.
- Add useful indexes:
  - `jobs(status, lease_until, runner_instance_id)`,
  - `jobs(status, route_name)`,
  - `runner_heartbeats(runner_instance_id, heartbeat_at)`.
- Configure SQLite for practical local concurrency:
  - WAL mode where appropriate,
  - busy timeout,
  - short write transactions.
- Update claim locking to be atomic under a write lock, e.g. `BEGIN IMMEDIATE` or equivalent.
- Switch claim capacity semantics to route-name-based concurrency; remove the current kind fallback/counting behavior.
- Introduce runner identity model, e.g. `RunnerIdentity{RunnerID, InstanceID}`.
- Store methods:
  - `HeartbeatRunner(ctx, identity, pid, now)`
  - `DeleteRunnerHeartbeat(ctx, runnerInstanceID)`
  - `PruneStaleRunnerHeartbeats(ctx, before)`
  - `AssertJobOwned(ctx, jobID, runnerInstanceID)` or equivalent ownership-token check
  - `RenewJobLease(ctx, jobID, runnerInstanceID, leaseDuration)`
  - ownership-guarded artifact update
  - ownership-guarded attempt update
  - ownership-guarded finalization/status update
  - atomic ownership-guarded route-attempt increment + job-attempt persistence helper, or a documented transaction boundary that provides equivalent safety
  - heartbeat-aware `ReleaseExpiredLeases(ctx, now, staleHeartbeatBefore, currentRunnerInstanceID, activeJobIDs)`
  - wave-frontier helper to list eligible job IDs at a given `waveStart`
  - claim helper restricted to a frontier set of job IDs
- Define and use typed ownership errors such as `ErrLostLease` or `ErrNotOwner`.

### Tests

- Migration is repeatable on a fresh DB and an already-migrated DB.
- Heartbeat insert/update works.
- Heartbeat delete/prune works.
- Claim stores `runner_instance_id`.
- Renew lease succeeds for owner and fails for wrong instance.
- Artifact/finalize/attempt updates fail for wrong instance.
- Ownership assertion fails for wrong/stale instance.
- Atomic attempt helper does not mutate attempts if ownership is lost.
- Expired job with stale/missing heartbeat is requeued.
- Expired job with null/empty `runner_instance_id` is requeued for backward compatibility.
- Expired job with fresh heartbeat is not requeued.
- Current active job exclusion prevents self-requeue.
- Recovery clears `locked_by`, `runner_instance_id`, `lease_until`, and `pid`.
- Recovery preserves artifact paths and does not increment attempts.
- Two store instances cannot overclaim beyond configured global/per-route capacity.
- Two different routes sharing the same kind can be claimed concurrently when global capacity allows.

### Commit message

```text
Add runner heartbeat and ownership store primitives
```

## Phase C1 — process-tree killing and cancellation result semantics

### Goals

Make timeout/cancellation kill whole subprocess trees on Unix and distinguish timeout from shutdown cancellation.

### Work items

- Add Unix build-tag helper using `exec.Cmd.SysProcAttr` to start each subprocess in a new process group/session.
- Add non-Unix fallback that preserves compilation and direct-child kill behavior.
- Stop relying on `exec.CommandContext` default cancellation for process-tree cleanup.
- Explicitly signal `-pgid` on timeout/cancel.
- Decide first implementation kill mode:
  - immediate kill is acceptable if tests document it,
  - optional graceful then force-kill can come later.
- Extend `runner.Result` with cancellation semantics, e.g. `Cancelled bool` and/or typed cause.
- Ensure job timeout remains `failed`; shutdown cancellation maps to future `cancelled` finalization.

### Tests

- Existing runner success/failure/timeout tests still pass.
- Unix-only test proves timeout kills child and grandchild.
- Cancellation returns a cancelled result distinct from timeout.
- Non-Unix fallback compiles; concrete gate: `GOOS=windows go test ./internal/runner` if feasible.

### Commit message

```text
Kill subprocess groups on timeout and cancellation
```

## Phase C2 — runner start/wait/reap split

### Goals

Expose runner primitives needed by a concurrent supervisor while preserving existing `runner.Run` behavior.

### Work items

- Add `runner.Handle` containing job, issue, paths, PID, done channel, and cancel function/cause.
- Add `runner.Start(...) (*Handle, error)`.
- Add `runner.Wait(...) Result` or equivalent reaping primitive.
- Re-implement `runner.Run(...)` through start/wait for compatibility.
- Make PID and artifact paths available immediately after successful start.
- Clarify ownership of log file handles; runner handle should close them after wait.
- Ensure context JSON/env still match current behavior.
- Ensure start failures have enough result information for dispatcher finalization.
- Add testability hook for a fake process starter/handle so dispatcher/daemon tests do not rely only on real subprocess timing.

### Tests

- Existing `runner.Run` tests remain green.
- `Start` exposes context/result/stdout/stderr paths and PID before wait completes.
- `Wait` returns expected exit code/stdout/stderr/result artifacts.
- Start failure path is deterministic and does not leak files.
- Fake process starter can simulate success, failure, timeout, cancellation, and blocked jobs.

### Commit message

```text
Split runner start and wait primitives
```

## Phase C3 — local concurrent supervisor skeleton

### Goals

Introduce the concurrent supervisor loop with local/no-GitHub jobs first, before re-integrating full GitHub lifecycle side effects.

### Work items

- Preserve the existing GitHub-aware `DispatchWithGitHub` path unchanged and serial until Phase C5. Standard gates and existing GitHub dispatch tests must continue to pass after C3/C4.
- Route only `Dispatch` / `dispatch --local-no-github` / local fixture dispatch through the new local supervisor in this phase.
- Create dispatcher supervisor state for active local jobs from claim through finalization.
- Create one `RunnerIdentity` per command process or daemon process.
- Move expired lease recovery inside the supervisor so it always has current `runner_instance_id` and active job IDs.
- Heartbeat before recovery, before claiming, and during active supervision.
- Implement injectable clock/timer hooks for fast deterministic supervisor tests.
- Implement wave frontier concretely:
  - capture eligible job IDs at `waveStart` using store helper,
  - only claim jobs in that frontier for `dispatch`/`once`,
  - refill freed capacity until the frontier is drained or unclaimable,
  - exclude jobs enqueued or becoming available after `waveStart`.
- Claim and start local jobs until capacity is full.
- Reap local jobs and refill capacity.
- Hold global/per-route capacity from claim through finalization.
- Immediately persist PID/artifact paths after successful start using ownership guard.
- Preserve `dispatch --local-no-github` behavior on top of this supervisor.
- Ensure the default GitHub-backed `issueq dispatch` continues to use the existing serial GitHub-aware path until C5.

### Tests

- Global concurrency `2` overlaps two local long-running jobs.
- Global concurrency `1` remains serial.
- Same-route concurrency `1` prevents same-route overlap.
- Route-name concurrency from C0 is respected by the supervisor.
- `dispatch --local-no-github` drains the initial eligible backlog, not just one capacity-sized batch.
- Jobs enqueued or becoming available after `waveStart` are not claimed in that wave.
- PID/artifact paths are visible while a fake/real job is running.
- Existing local dispatch fixture tests still pass.

### Commit message

```text
Add local concurrent dispatch supervisor
```

## Phase C4 — lease renewal and lost-ownership handling in supervisor

### Goals

Wire the C0 ownership/heartbeat primitives into active supervision and define behavior for transient DB errors vs lost ownership.

### Work items

- Renew active leases on renewal timer using ownership guard.
- Distinguish transient renewal/store errors from typed `ErrLostLease`/`ErrNotOwner`.
- On transient renewal failure:
  - log/record where possible,
  - keep supervising child,
  - retry on next renewal,
  - surface persistent/finalization-blocking errors from the command/daemon.
- On `ErrLostLease`/`ErrNotOwner`:
  - cancel/kill the active subprocess if still running,
  - reap only for local cleanup,
  - do not apply GitHub actions,
  - do not update artifacts or finalize the job,
  - remove it from local active state.
- Ensure recovery never requeues the current process's active job.
- Add ownership guard to all local supervisor metadata/finalization writes.

### Tests

- Active jobs renew leases before expiry.
- Transient renewal failure is retried without immediate duplicate/requeue.
- Lost ownership cancels/kills the active subprocess and prevents finalization by stale owner.
- Lost ownership prevents artifact updates by stale owner.
- Current process does not requeue its own active job even under tight lease timing.
- Fresh heartbeat prevents another process from reclaiming the job.
- Stale heartbeat allows another process to reclaim after expiry.

### Commit message

```text
Renew leases and enforce dispatcher ownership
```

## Phase C5 — GitHub-aware concurrent dispatch integration

### Goals

Move the existing GitHub-aware dispatch lifecycle onto the concurrent supervisor without reintroducing stale ownership side effects.

### Work items

- Reintegrate GitHub-aware per-job flow:
  - load local issue,
  - refresh GitHub issue,
  - upsert refreshed issue,
  - re-check route predicate,
  - increment/persist route attempts through ownership-safe helper,
  - enforce max attempts,
  - apply `on_start`,
  - check transition limit,
  - start/reap subprocess,
  - parse/merge result JSON,
  - apply success/failure actions,
  - check transition limit,
  - finalize.
- Check/hold ownership before every external side effect:
  - route-attempt increment/persist,
  - `on_start`,
  - terminal actions,
  - success/failure actions,
  - finalization.
- If ownership is lost, skip side effects and local finalization.
- Keep common post-claim failure handling ownership-aware.
- Preserve GitHub-backed `issueq dispatch` by default.
- Preserve stale route skip, action application, attempt limit, transition limit, and malformed result handling.

### Tests

- Existing GitHub staleness/actions/attempt tests still pass.
- Lost ownership before attempt increment prevents route/job attempt mutation.
- Lost ownership before `on_start` prevents GitHub label/comment action.
- Lost ownership before terminal action prevents terminal GitHub side effects.
- Lost ownership before success/failure action prevents result GitHub side effects.
- Post-claim GitHub/API failures still finalize as `failed` when ownership is held.
- Attempts and transition guardrails still behave correctly under concurrent supervision.

### Commit message

```text
Integrate GitHub lifecycle with concurrent dispatcher
```

## Phase C6 — daemon event loop and shutdown

### Goals

Make daemon a long-lived nonblocking supervisor with independent poll, heartbeat, renew, reap/refill, and shutdown behavior.

### Work items

- Replace blocking full `Once` dispatch cycle inside daemon with event loop timers:
  - heartbeat timer,
  - lease-renew timer,
  - reap/refill timer,
  - poll/route timer,
  - shutdown signal/cancellation.
- Ensure renew/reap/refill can run more frequently than `polling.interval`.
- Create one daemon `RunnerIdentity` for the process and reuse it across loop iterations.
- On shutdown:
  - stop poll/route/claim,
  - cancel active subprocesses with shutdown cause,
  - kill process groups,
  - reap children,
  - finalize jobs as `cancelled` while ownership is held,
  - delete current heartbeat after active jobs finalize.
- Use a bounded cleanup context derived from `context.Background()`, not the already-cancelled daemon context.
- Prune stale heartbeat rows opportunistically.
- Keep `once` as bounded wave using same supervisor machinery.
- Keep `once --no-wait` disabled; durable detached supervision is a non-goal for this pass.

### Tests

- Daemon polls/routes on cadence while a long-running job is active.
- Reap/refill happens without waiting for next poll interval.
- Shutdown cancels active job and finalizes as `cancelled`.
- Shutdown finalization succeeds after parent context is cancelled.
- Heartbeat is deleted on clean shutdown.
- Stale heartbeat pruning works if implemented in this phase.
- `once` returns after the wave completes.
- `once --no-wait` remains an unsupported error.

### Commit message

```text
Refactor daemon into nonblocking supervisor loop
```

## Phase C7 — documentation, hardening, and pre-E2E gates

### Goals

Close the documented follow-ups and prepare for Phase 9 manual E2E.

### Work items

- Update `docs/code-review-followups.md`:
  - mark #5 fixed,
  - mark #6 fixed,
  - mark #7 fixed,
  - mark #12 fixed.
- Update or annotate stale docs:
  - `docs/v1-spec.md`, especially any `once --no-wait` language,
  - `docs/v1-implementation-plan.md`, especially phase status and concurrency semantics.
- Update examples or CLI help if behavior changed.
- Add any missing operator notes:
  - stop old daemons before heartbeat migration,
  - `once --no-wait` remains unsupported,
  - meaning of `cancelled` jobs.
- Run full gates and local smoke tests.
- Run `go test -race ./...` if practical.
- Ask a `custom-4427efca` subagent for final pre-Phase 9 review.

### Tests/gates

- Standard gates.
- `go test -race ./...` if practical.
- Targeted local smoke:
  - `route`,
  - `dispatch`,
  - `dispatch --local-no-github`,
  - `once`,
  - `once --no-wait` unsupported failure,
  - `jobs --json`,
  - `issues --json`.

### Commit message

```text
Document completed concurrency supervision follow-ups
```

## Phase 9 handoff

After C0-C7 are complete and reviewed:

- perform manual GitHub E2E with a test repository,
- validate poll/route/dispatch lifecycle labels/comments,
- validate stale route skip,
- validate attempts and transition guardrails,
- validate daemon shutdown/restart recovery,
- document any final hardening issues discovered during E2E.
