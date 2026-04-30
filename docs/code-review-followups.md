# Code review follow-ups before Phase 9

This document tracks issues found during the post Phase 5-8 code review. Automated tests currently pass, but these items should be addressed or explicitly accepted before manual end-to-end validation.

## Priority 0 — must fix before trusting E2E behavior

### 1. `once --no-wait` is not actually safe/nonblocking

**Where:** `cmd/issueq/main.go`, `onceCommand`

`once --no-wait` starts `daemon.Once` in a goroutine, returns immediately, then deferred cleanup closes the SQLite store. In normal CLI execution the process can also exit before the goroutine completes. Errors are discarded.

**Risk:** work may not start or may race with a closed DB.

**Suggested fix:** either remove/disable `--no-wait` for v1 until the dispatcher has real background child supervision, or refactor dispatch into explicit start/reap phases so `--no-wait` synchronously starts jobs, persists artifacts/PIDs, then returns.

### 2. `issueq dispatch` bypasses GitHub staleness, actions, and attempts

**Where:** `cmd/issueq/main.go`, `dispatchCommand`; `internal/dispatcher`

The CLI `dispatch` command opens only the local store and calls `dispatcher.Dispatch(..., gh=nil)`. That skips:

- GitHub re-fetch before dispatch
- route predicate staleness check
- `on_start`
- success/failure GitHub label/comment actions
- route attempt increment/enforcement
- transition counting

**Risk:** CLI behavior differs from daemon/once behavior and violates the v1 dispatch/action/staleness requirements.

**Suggested fix:** make `dispatch` a GitHub-contacting command by default using `openGitHubStore` and `DispatchWithGitHub`. If local-only fixture dispatch remains useful, gate it behind an explicit flag such as `--local-no-github`.

### 3. Attempt numbers are off by one and not persisted on jobs

**Where:** `internal/dispatcher/dispatcher.go`; `internal/runner/runner.go`; `internal/store/sqlite/sqlite.go`

`IncrementAttempts` returns the current attempt number and dispatcher assigns it to `job.Attempts`, but runner writes `job.Attempts + 1` to context JSON and env. The first GitHub-aware run therefore reports attempt `2`. The `jobs.attempts` column is also not updated when attempts increment.

**Risk:** confusing context/env, misleading CLI state, incorrect downstream agent behavior.

**Suggested fix:** treat `Job.Attempts` as the current attempt. Runner should emit it directly. Add a store method or update path to persist `jobs.attempts` when route attempts increment.

### 4. Claimed jobs can remain `running` after post-claim errors

**Where:** `internal/dispatcher/dispatcher.go`

After a job is claimed, several errors return from dispatch without finalizing/releasing the job, including GitHub refresh errors, issue state errors, attempt increment errors, `on_start` errors, and transition-limit errors.

**Risk:** jobs remain `running` until lease expiry, causing delayed retries and confusing operator state.

**Suggested fix:** once a job is claimed, use a common finalize/release path. For deterministic action/application failures, mark the job `failed` with `last_error`. For transient GitHub fetch failures, choose and document either release-to-pending or failed-with-retry policy.

## Priority 1 — important hardening

### 5. Configured concurrency is not realized as parallel execution

**Where:** `internal/dispatcher/dispatcher.go`; `internal/runner/runner.go`

The dispatcher claims one job, runs it synchronously, finalizes it, then claims the next. `queue.max_global_concurrency` and route `job.concurrency` prevent over-claiming but do not cause multiple subprocesses to run concurrently in one process.

**Risk:** v1 does not deliver promised global/per-route concurrent supervision.

**Suggested fix:** split runner into start/wait/reap primitives. Dispatcher should claim/spawn up to available capacity, track active children, renew leases, and reap completed jobs. `once` can wait by default; `once --no-wait` can return after spawn.

### 6. Lease expiry can duplicate still-running work

**Where:** `internal/store/sqlite/sqlite.go`, `ReleaseExpiredLeases`; `internal/daemon/daemon.go`

There is no lease renewal while subprocesses run. `ReleaseExpiredLeases` blindly returns expired `running` jobs to `pending` without checking whether the PID is still alive.

**Risk:** long jobs can be reclaimed while still executing, creating duplicate automation.

**Suggested fix:** renew leases for active children. On startup/expiry, only requeue jobs whose process is known not alive or whose runner is no longer active. At minimum document that `lease_duration` must exceed job timeout until renewal lands.

### 7. Timeout kills only the direct subprocess

**Where:** `internal/runner/runner.go`

`exec.CommandContext` kills the immediate child on timeout/cancel, but grandchildren may survive.

**Risk:** bounded-job guarantee is incomplete; runaway child process trees can remain.

**Suggested fix:** start jobs in their own process group/session and kill the process group on timeout/cancel. Implement platform-specific handling for Unix first.

### 8. Action application does not refresh after mutations

**Where:** `internal/actions/actions.go`, `Apply`

The current code refreshes before actions, locally edits labels after remove/add, and upserts that synthetic snapshot. The spec calls for refreshing/updating local state after actions.

**Risk:** local snapshots may miss GitHub-normalized labels, `updated_at`, concurrent edits, or other server-side changes.

**Suggested fix:** after remove/add/comment, call `GetIssue` again and upsert that final snapshot.

### 9. Transition count increments even when labels did not change

**Where:** `internal/actions/actions.go`; `internal/dispatcher/dispatcher.go`

`ApplyResult.Changed` is true whenever action label add/remove lists are non-empty, even if removing an absent label or adding an already-present label produces no actual state change.

**Risk:** workflows can hit `max_transitions_per_issue` prematurely.

**Suggested fix:** compare pre/post label sets and set `Changed` only if labels actually differ.

### 10. Terminal action errors are ignored

**Where:** `internal/dispatcher/dispatcher.go`

Errors from `actions.Apply` are discarded for `on_attempts_exceeded` and workflow terminalization.

**Risk:** jobs may be marked `dead` even though GitHub was not updated with terminal labels/comments.

**Suggested fix:** capture terminal action errors and persist them in `last_error`/events. Decide whether terminalization failure should leave the job `failed`, `dead`, or retryable.

## Priority 2 — polish and robustness

### 11. GitHub HTTP client has no default timeout

**Where:** `internal/github/github.go`

The REST client uses `http.DefaultClient` when no client is provided. That client has no timeout.

**Risk:** GitHub calls can hang indefinitely unless the caller context is canceled.

**Suggested fix:** use a default `http.Client{Timeout: ...}` or ensure all CLI/daemon GitHub calls run under bounded contexts.

### 12. `daemon` currently blocks inside full synchronous dispatch cycles

**Where:** `internal/daemon/daemon.go`

The daemon loop calls `Once`, and `Once` blocks until all dispatch work in the current synchronous dispatcher completes. This is acceptable for local smoke tests but does not yet match a mature supervisor/reaper loop.

**Risk:** polling cadence and shutdown responsiveness depend on subprocess duration.

**Suggested fix:** address together with the concurrency/refactor work: daemon should periodically poll/route, spawn within capacity, renew leases, reap children, and respond promptly to shutdown.

## Suggested work order

1. Fix `dispatch` to use GitHub-backed dispatch by default.
2. Fix attempt numbering and persist job attempts.
3. Add common post-claim failure handling so jobs do not remain `running` on errors.
4. Fix action post-refresh and real label-change detection.
5. Decide whether to remove/disable `once --no-wait` or implement real nonblocking spawn semantics.
6. Document or implement lease renewal/process-tree killing/concurrent supervision.
7. Add HTTP client timeout.
8. Re-run automated gates, then start Phase 9 manual E2E.

## Gates for follow-up fixes

Run after each fix batch:

```bash
go test ./...
go vet ./...
gofmt -w .
git diff --check
```

Also run targeted local smoke tests for `route`, `dispatch`, `once`, `jobs --json`, and `issues --json` before real GitHub testing.
