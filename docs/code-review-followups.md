# Code review follow-ups before Phase 9

This document tracks issues found during the post Phase 5-8 code review. Automated tests currently pass, but these items should be addressed or explicitly accepted before manual end-to-end validation.

## Priority 0 — must fix before trusting E2E behavior

### 1. `once --no-wait` is not actually safe/nonblocking — fixed

**Where:** `cmd/issueq/main.go`, `onceCommand`

`once --no-wait` used to start `daemon.Once` in a goroutine, return immediately, then deferred cleanup closed the SQLite store. In normal CLI execution the process could also exit before the goroutine completed. Errors were discarded.

**Fix:** `once --no-wait` is now rejected with a clear unsupported error until real background child supervision exists. No goroutine is started.

**Verification:** `cmd/issueq` has a CLI test for the unsupported error; standard gates include `go test ./...`.

### 2. `issueq dispatch` bypasses GitHub staleness, actions, and attempts — fixed

**Where:** `cmd/issueq/main.go`, `dispatchCommand`; `internal/dispatcher`

The CLI `dispatch` command used to open only the local store and call `dispatcher.Dispatch(..., gh=nil)`, skipping GitHub refresh, stale predicate checks, lifecycle actions, route attempt enforcement, and transition counting.

**Fix:** `issueq dispatch` now requires GitHub credentials by default, opens a GitHub-backed store/client, and calls `DispatchWithGitHub`. Local fixture dispatch remains available only via explicit `--local-no-github`.

**Verification:** CLI tests assert default dispatch requires `github.token_env`; existing dispatcher tests cover GitHub refresh/stale skip/actions/attempts; local fixture dispatch is covered through `dispatch --local-no-github`.

### 3. Attempt numbers are off by one and not persisted on jobs — fixed

**Where:** `internal/dispatcher/dispatcher.go`; `internal/runner/runner.go`; `internal/store/sqlite/sqlite.go`

`IncrementAttempts` returns the current attempt number and dispatcher assigns it to `job.Attempts`, but runner used to write `job.Attempts + 1` to context JSON and env. The first GitHub-aware run therefore reported attempt `2`. The `jobs.attempts` column was also not updated when attempts incremented.

**Fix:** `Job.Attempts` is now treated as the current GitHub-backed attempt once dispatcher increments it. Runner context JSON and `ISSUEQ_ATTEMPT` emit that value directly, clamped to `1` for local fixture jobs that have no GitHub-backed attempt counter. The durable workflow persists attempts through the owned `IncrementAttemptsForJob` path immediately after route-attempt increment.

**Verification:** runner tests assert attempt `1` in env/context for a first attempt and for zero-attempt local fixture jobs; dispatcher tests assert the job row persists `attempts=1` and result-driven comment sees `ISSUEQ_ATTEMPT=1`.

### 4. Claimed jobs can remain `running` after post-claim errors — fixed

**Where:** `internal/dispatcher/dispatcher.go`

After a job is claimed, several errors could return from dispatch without finalizing/releasing the job, including GitHub refresh errors, issue state errors, attempt increment errors, `on_start` errors, and transition-limit errors.

**Fix:** dispatcher now uses a common `failClaimedJob` path for post-claim GitHub refresh/upsert, issue-state, attempt increment/persist, `on_start`, terminal action, and transition-limit errors. These jobs are finalized as `failed`, get `last_error`, and emit a `job_failed` event. This intentionally treats transient GitHub/API errors as failed jobs for now rather than leaving them leased/running.

**Verification:** dispatcher tests inject GitHub refresh and `on_start` failures and assert jobs end as `failed` with useful `last_error`, not `running`.

## Priority 1 — important hardening

### 5. Configured concurrency is not realized as parallel execution — fixed

**Where:** `internal/dispatcher/dispatcher.go`; `internal/runner/runner.go`

The dispatcher claims one job, runs it synchronously, finalizes it, then claims the next. `queue.max_global_concurrency` and route `job.concurrency` prevent over-claiming but do not cause multiple subprocesses to run concurrently in one process.

**Risk:** v1 does not deliver promised global/per-route concurrent supervision.

**Fix:** H5 rewired `dispatch`, `once`, and daemon to wrapper-backed durable running jobs. Bounded waves and the daemon refill loop claim/launch up to DB-derived global/per-route capacity and inspect/finalize running jobs via the supervisor.

**Verification:** wrapper/default smoke and dispatcher/daemon tests cover wrapper-only dispatch, bounded frontier behavior, refill/reap behavior, and absence of attached fallback.

### 6. Lease expiry can duplicate still-running work — fixed

**Where:** `internal/store/sqlite/sqlite.go`, `ReleaseExpiredLeases`; `internal/daemon/daemon.go`

There is no lease renewal while subprocesses run. `ReleaseExpiredLeases` blindly returns expired `running` jobs to `pending` without checking whether the PID is still alive.

**Risk:** long jobs can be reclaimed while still executing, creating duplicate automation.

**Fix:** Running jobs are owned by `runner_instance_id`, renewed while owned, and represented by durable wrapper launch metadata. Expired non-durable rows are requeued only after stale/missing heartbeat; durable wrapper rows require verified stale-owner adoption or remain operator-visible unknown.

**Verification:** store/workflow/daemon tests cover ownership-guarded renewals, stale lease recovery, stale-owner adoption, and conservative unknown handling.

### 7. Timeout kills only the direct subprocess — fixed

**Where:** `internal/runner/runner.go`

`exec.CommandContext` kills the immediate child on timeout/cancel, but grandchildren may survive.

**Risk:** bounded-job guarantee is incomplete; runaway child process trees can remain.

**Fix:** User commands now run under `issueq job-wrapper`, which starts the command in a process group where supported and kills the process group on timeout/cancellation.

**Verification:** job-wrapper/supervisor tests cover timeout, cancellation, and process-tree cleanup behavior.

### 8. Action application does not refresh after mutations — fixed

**Where:** `internal/actions/actions.go`, `Apply`

The code used to refresh before actions, locally edit labels after remove/add, and upsert that synthetic snapshot. The spec calls for refreshing/updating local state after actions.

**Fix:** `Apply` now refreshes before actions, applies remove/add/comment, refreshes once after any mutation, and upserts the final GitHub snapshot.

**Verification:** action tests assert post-action refresh call order and that the stored issue comes from the final GitHub snapshot.

### 9. Transition count increments even when labels did not change — fixed

**Where:** `internal/actions/actions.go`; `internal/dispatcher/dispatcher.go`

`ApplyResult.Changed` used to be true whenever action label add/remove lists were non-empty, even if removing an absent label or adding an already-present label produced no actual state change.

**Fix:** `ApplyResult.Changed` now compares the pre-action label set to the final refreshed label set. Comments do not count as workflow transitions.

**Verification:** action tests assert no-op add/remove plus comment returns `Changed=false`.

### 10. Terminal action errors are ignored — fixed

**Where:** `internal/dispatcher/dispatcher.go`

Errors from `actions.Apply` were still discarded for workflow terminalization (`workflow.on_transitions_exceeded`). `on_attempts_exceeded` action errors had already been captured by the Priority 0 post-claim error handling fix.

**Fix:** workflow terminalization action failures now produce a `terminal_action_failed` event, propagate through the post-claim failure path, and finalize the job as `failed` rather than `dead`.

**Verification:** dispatcher tests inject workflow terminal action failure and assert `failed` status, useful `last_error`, and `terminal_action_failed` event.

## Priority 2 — polish and robustness

### 11. GitHub HTTP client has no default timeout — fixed

**Where:** `internal/github/github.go`

The REST client used to use `http.DefaultClient` when no client was provided. That client has no timeout.

**Fix:** default REST clients now use `http.Client{Timeout: 30 * time.Second}`. Injected clients remain unchanged.

**Verification:** GitHub tests assert `NewRESTClient` installs the default timeout.

### 12. `daemon` currently blocks inside full synchronous dispatch cycles — fixed

**Where:** `internal/daemon/daemon.go`

The daemon loop calls `Once`, and `Once` blocks until all dispatch work in the current synchronous dispatcher completes. This is acceptable for local smoke tests but does not yet match a mature supervisor/reaper loop.

**Risk:** polling cadence and shutdown responsiveness depend on subprocess duration.

**Fix:** Daemon is now a long-lived DB reconciler with independent heartbeat, poll/route, and reconcile/refill loops. It no longer runs as repeated blocking `Once` calls.

**Verification:** daemon tests cover poll responsiveness, reap/refill behavior, shutdown cleanup, heartbeat deletion after clean shutdown, and stale durable adoption.

## Suggested work order

1. ~~Fix `dispatch` to use GitHub-backed dispatch by default.~~
2. ~~Fix attempt numbering and persist job attempts.~~
3. ~~Add common post-claim failure handling so jobs do not remain `running` on errors.~~
4. ~~Fix action post-refresh and real label-change detection.~~
5. ~~Decide whether to remove/disable `once --no-wait` or implement real nonblocking spawn semantics.~~ Disabled for now.
6. ~~Document or implement lease renewal/process-tree killing/concurrent supervision.~~ Implemented through wrapper-only H5/H6 supervision.
7. ~~Add HTTP client timeout.~~
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
