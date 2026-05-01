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

- Add idempotent SQLite migration for fields such as:
  - `supervisor_kind`,
  - `supervisor_id`,
  - `pgid`,
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

## Phase E4 — reconcile running jobs from durable state

### Goals

Make daemon concurrency DB-driven: running DB rows are the active set. In-memory maps are implementation details of a backend, not daemon policy.

### Work items

- Add store helpers:
  - list running jobs owned by runner instance,
  - list stale-owner running jobs with durable launch metadata,
  - atomically adopt stale running jobs before inspection/finalization,
  - count running jobs globally/per route,
  - list launch records needing inspection,
  - possibly mark unknown launch state.
- Change daemon reap/refill policy to:
  - inspect owned running DB rows,
  - attempt explicit adoption before inspecting stale-owner durable jobs,
  - finalize completed observations,
  - launch only when DB-derived capacity exists.
- Keep `once`/`dispatch` bounded frontier semantics by tracking frontier job IDs, not live handles.
- Ensure expired lease recovery uses DB active rows and heartbeat policy, not in-memory active IDs except for attached backend compatibility.
- Document that the attached backend cannot truly inspect/adopt jobs after daemon crash; it falls back to stale lease recovery until wrapper/systemd backends land.

### Tests

- Daemon restart/recovery and stale-owner adoption tests.
- Concurrent capacity tests using DB running counts.
- Existing C6 daemon tests.
- Standard gates.

### Commit message

```text
Reconcile running jobs from durable launch state
```

## Phase E5 — implement job-wrapper command and metadata contract

### Goals

Add the stable wrapper contract without making it the default backend yet.

### Work items

- Add hidden/internal CLI subcommand, e.g. `issueq job-wrapper`, or an internal helper entrypoint.
- Define wrapper spec input format.
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
- Record wrapper PID/PGID and metadata path.
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
