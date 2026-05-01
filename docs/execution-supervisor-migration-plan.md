# Execution supervisor migration plan

This plan implements `docs/execution-supervisor-spec.md` incrementally. The current C0-C6 concurrency supervision work is the correctness baseline; this migration should simplify architecture without regressing behavior.

## Standard gates

Run after each phase unless explicitly scoped smaller:

```bash
gofmt -w .
go test ./...
go vet ./...
go run ./cmd/issueq --help
go build ./cmd/issueq
git diff --check
```

Run CLI smokes when entrypoint behavior changes:

```bash
go run ./cmd/issueq --config examples/issueq.yaml config-check
go run ./cmd/issueq --config examples/issueq.yaml jobs --json
go run ./cmd/issueq --config examples/issueq.yaml issues --json
go run ./cmd/issueq --config examples/issueq.yaml once --no-wait # expected unsupported failure
```

Use review subagents after each implementation phase. Commit each phase separately.

## Phase E0 — planning and boundaries

### Goals

Document the target architecture and make the intended direction explicit before code changes.

### Work items

- Add `docs/execution-supervisor-spec.md`.
- Add this migration plan.
- Cross-reference from future C7 docs if appropriate.

### Tests

Docs only; run `git diff --check`.

### Commit message

```text
Document execution supervisor refactor plan
```

## Phase E0a — harden execution supervisor invariants

### Goals

Tighten the target docs before implementation so the migration preserves crash/restart safety.

### Work items

- Define a launch transaction/crash-window protocol.
- Make per-launch identity (`launch_token`/run ID) mandatory for durable backends.
- Clarify adoption versus stale requeue versus unknown-state behavior.
- Specify wrapper/systemd timeout and cancellation precedence.
- Clarify backend mismatch behavior on restart.
- Split DB-driven attached reconciliation from true durable adoption work.

### Tests

Docs only; run `git diff --check`.

### Commit message

```text
Harden execution supervisor migration invariants
```

## Phase E1 — extract shared workflow primitives

### Goals

Separate queue/GitHub workflow operations from daemon and dispatch loop policy without changing behavior.

### Work items

- Extract primitives from `internal/dispatcher/dispatcher.go` and `internal/daemon/daemon.go` for:
  - heartbeat,
  - expired lease recovery,
  - stale heartbeat pruning,
  - claim one eligible job,
  - prepare claimed job GitHub lifecycle,
  - launch claimed job,
  - reap/finalize observed result,
  - cancel owned jobs.
- Keep current attached runner implementation behind these primitives.
- Preserve `once`, `dispatch`, and daemon observable behavior.
- Add focused tests for extracted primitives where they are not already covered.

### Tests

- Existing dispatcher and daemon tests.
- Standard gates.
- Daemon stress subset.

### Commit message

```text
Extract shared queue workflow primitives
```

## Phase E2 — introduce execution supervisor interface

### Goals

Create the backend-neutral supervisor abstraction from the spec and adapt current attached execution to it.

### Work items

- Add package, e.g. `internal/executor` or `internal/supervisor`.
- Define:
  - `LaunchSpec`,
  - `LaunchRecord`,
  - `Observation`,
  - `RunState`,
  - `CancelReason`,
  - `Supervisor` interface.
- Implement `AttachedSupervisor` using current `runner.Start`, `runner.Wait`, and cancellation behavior.
- Keep any live process-handle map private to `AttachedSupervisor`.
- Convert dispatcher/daemon workflow code to depend on the interface, not directly on `runner.Handle`.
- Keep GitHub lifecycle and queue ownership logic outside the supervisor backend.

### Tests

- Unit tests for `AttachedSupervisor` launch/inspect/cancel behavior.
- Existing runner tests should remain.
- Existing dispatcher/daemon tests should remain green.
- Standard gates.

### Commit message

```text
Introduce execution supervisor interface
```

## Phase E3 — persist backend-neutral launch records

### Goals

Move toward DB-driven running-job reconciliation by storing launch metadata durably.

### Work items

- Add idempotent SQLite migration for required fields such as:
  - `supervisor_kind`,
  - `supervisor_id`,
  - `launch_token`,
  - `launch_state`,
  - `pgid`,
  - `process_started_at`,
  - `run_metadata_path`,
  - `timeout_at`.
- Extend `model.Job`, scan paths, JSON output, and tests.
- Add ownership-guarded store method to persist launch records.
- Update current attached backend path to populate as much launch data as possible.
- Keep backward compatibility for rows without launch metadata.

### Tests

- SQLite migration and scan/list tests.
- Ownership guard tests for launch-record persistence.
- Standard gates.

### Commit message

```text
Persist durable job launch records
```

## Phase E4a — reconcile attached running jobs from durable state

### Goals

Make daemon capacity and owned-running scans DB-driven while acknowledging that the current attached backend is not durably inspectable after daemon crash.

### Work items

- Add store helpers:
  - list running jobs owned by runner instance,
  - count running jobs globally/per route,
  - list launch records needing inspection for the current owner,
  - possibly mark unknown launch state.
- Change daemon reap/refill policy to:
  - inspect owned running DB rows through the attached supervisor when live handles exist,
  - finalize completed observations,
  - launch only when DB-derived capacity exists.
- Keep `once`/`dispatch` bounded frontier semantics by tracking frontier job IDs, not live handles.
- Ensure expired lease recovery uses DB active rows and heartbeat policy, while retaining attached-backend compatibility for live active IDs.
- Document that attached rows cannot truly inspect/adopt jobs after daemon crash; because attached launch records are not durable detached supervision metadata, they fall back to stale lease recovery.

### Tests

- Concurrent capacity tests using DB running counts.
- Existing C6 daemon tests.
- Standard gates.

### Commit message

```text
Reconcile attached jobs from durable launch state
```

## Phase E4b — add durable adoption primitives behind fake backend

### Goals

Implement and test the store-level adoption/unknown-state rules before real wrapper/systemd backends depend on them.

### Work items

- Add store helpers:
  - list stale-owner running jobs with durable launch metadata,
  - atomically adopt stale running jobs after read-only backend verification,
  - mark/report unknown launch state without automatic requeue; any DB mutation for stale-owner unknown rows must use a stale-owner-safe guard and must not imply ownership transfer, cancellation, finalization, or GitHub side effects.
- Add fake durable supervisor tests for:
  - verified completed metadata adoption/finalization,
  - verified still-running adoption/renewal,
  - durable metadata with unverifiable identity remaining running/unknown,
  - no durable launch metadata following stale lease requeue semantics.
- Ensure reconciliation dispatches by persisted `supervisor_kind`, not only configured default backend.

### Tests

- Store adoption and unknown-state tests.
- Daemon restart/recovery tests using fake durable supervisor.
- Standard gates.

### Commit message

```text
Add durable job adoption primitives
```

## Phase E5 — implement job-wrapper command and metadata contract

### Goals

Add the stable wrapper contract without making it the default backend yet.

### Work items

- Add hidden/internal CLI subcommand, e.g. `issueq job-wrapper`, or an internal helper entrypoint.
- Define wrapper spec input format, including job ID, launch token, command, env, workdir, paths, and timeout.
- Validate the existing `context.json` written by the workflow layer before launch.
- Start user command in a process group.
- Redirect stdout/stderr to configured files.
- Enforce timeout.
- Kill process group on timeout/cancellation.
- Write `run.json` atomically.
- Add metadata parser and mapper to `Observation`.

### Tests

- Wrapper success, failure, timeout, cancellation.
- Metadata atomic write/parse tests.
- Process-tree killing tests where practical.
- Standard gates.

### Commit message

```text
Add durable job wrapper execution contract
```

## Phase E6 — add direct wrapper supervisor backend

### Goals

Run jobs through `issueq job-wrapper` directly and inspect durable metadata/PID state.

### Work items

- Implement `WrapperSupervisor`.
- Launch wrapper as child process.
- Record wrapper PID/PGID/process fingerprint, launch token, and metadata path.
- Inspect:
  - `run.json` if present,
  - process existence if metadata absent,
  - timeout deadline,
  - unknown state conservatively.
- Cancel wrapper/user process group.
- Add config to select backend, initially defaulting to attached unless intentionally changed.

### Tests

- Backend launch/inspect/cancel tests.
- Daemon running multiple wrapper-backed jobs.
- Daemon restart/recovery with wrapper metadata.
- Standard gates and smokes.

### Commit message

```text
Add direct wrapper supervisor backend
```

## Phase E7 — add systemd supervisor backend

### Goals

Provide optional systemd transient unit supervision using the same wrapper contract.

### Work items

- Implement `SystemdSupervisor`.
- Launch wrapper via `systemd-run` transient units.
- Record unit name as `supervisor_id`.
- Inspect unit state via documented systemd CLI/DBus path; map to `Observation` plus wrapper metadata.
- Cancel via `systemctl stop/kill`.
- Add config selection, e.g. `execution.supervisor: systemd`.
- Ensure absence of systemd yields clear validation/runtime error.

### Tests

- Unit tests with fake systemd command runner.
- Optional integration test gated by systemd availability.
- Existing standard gates should not require systemd.

### Commit message

```text
Add systemd execution supervisor backend
```

## Phase E8 — simplify daemon and dispatch internals

### Goals

Remove obsolete attached active-map complexity from daemon/workflow once durable supervisor backends cover required behavior.

### Work items

- Make daemon a clear reconciler over DB rows and supervisor observations.
- Keep `AttachedSupervisor` only as a compatibility/test backend or remove it if no longer needed.
- Delete obsolete helpers that mix GitHub lifecycle and process handles.
- Tighten docs around backend selection and operational tradeoffs.

### Tests

- Full standard gates.
- Daemon stress tests.
- Manual smoke with selected default backend.
- If practical, race tests for daemon/supervisor packages.

### Commit message

```text
Simplify daemon around durable execution supervision
```

## Phase E9 — final docs and E2E readiness

### Goals

Prepare the refactored architecture for final manual E2E.

### Work items

- Update README and `docs/v1-spec.md` where user-visible behavior/config changed.
- Update `docs/v1-implementation-plan.md` Phase 9 status notes.
- Document backend tradeoffs:
  - attached,
  - wrapper,
  - systemd.
- Document restart/shutdown recovery behavior.
- Document why `once --no-wait` remains unsupported or define precise criteria for enabling it.

### Tests

- Standard gates.
- Broader manual smokes.
- Final review subagent.

### Commit message

```text
Document execution supervisor backend behavior
```
