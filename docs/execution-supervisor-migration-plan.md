# Execution supervisor hard-cutover plan

This plan implements `docs/execution-supervisor-spec.md` with a hard cutover to the target architecture. The project is pre-production, has no data compatibility requirement, and may be temporarily broken during refactoring if each committed phase is coherent and reviewed.

The goal is architectural simplification, not compatibility with the current daemon-owned process-handle design. Optimize for clear boundaries:

```text
internal/workflow     queue, leases, GitHub lifecycle, job state transitions
internal/supervisor   launch/inspect/cancel execution observations
internal/jobwrapper   durable wrapper process contract
internal/daemon       long-lived reconciliation policy
internal/dispatcher   bounded once/dispatch policy
```

The attached runner path is only a temporary bridge or test aid. Do not spend effort making attached supervision a polished durable backend. The preferred target backend is the direct wrapper supervisor; systemd remains optional later.

## Standard gates

Run after each code phase unless explicitly scoped smaller:

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
go run ./cmd/issueq --config examples/issueq.yaml once --no-wait # expected unsupported failure unless explicitly enabled
```

Use review subagents after each implementation phase. Commit each phase separately.

## Test strategy by boundary

The refactor should move tests toward the same boundaries as the architecture. Avoid giant daemon tests for behavior that belongs to store, workflow, or supervisor contracts.

### Store tests

Scope: SQLite state transitions only. No GitHub and no subprocesses.

Cover:

- claim capacity and per-route concurrency,
- heartbeat insert/update/delete/prune,
- lease renewal ownership guards,
- launch-record persistence and launch-state transitions,
- stale lease requeue for rows without durable launch metadata,
- stale-owner adoption guards for durable launch metadata,
- stale-owner-safe unknown marking/reporting,
- ownership-guarded finalization and lost-ownership errors.

### Supervisor contract tests

Scope: `internal/supervisor` behavior. No queue and no GitHub.

Use a shared contract test style where practical for fake, wrapper, and future systemd backends:

- launch returns a valid `LaunchRecord`,
- inspect `starting`, `running`, exited zero, exited nonzero, timed out, cancelled, and unknown,
- stale launch token or mismatched metadata is rejected/unknown,
- cancellation is idempotent,
- stdout/stderr/result/metadata paths are respected,
- backend mismatch or missing backend support is conservative.

### Job wrapper tests

Scope: `issueq job-wrapper` / `internal/jobwrapper` execution contract.

Cover:

- validates existing `context.json`, job ID, and launch token,
- does not rewrite context,
- captures stdout/stderr,
- records exit code and paths in `run.json`,
- writes metadata atomically,
- enforces timeout and kills process group,
- handles cancellation signals and repeated cancellation,
- preserves timeout-vs-cancel precedence.

### Workflow tests

Scope: queue/GitHub/job lifecycle decisions using fake store, fake GitHub, and fake supervisor observations.

Cover:

- prepare claimed job writes context and launch spec,
- ownership checks before `on_start` and terminal side effects,
- success/failure/cancelled/timeout/unknown observation mapping,
- result JSON merge and malformed result behavior,
- stale route and transition/attempt policy,
- lost ownership drops GitHub actions and finalization,
- finalization after restart reloads current job/config/issue state and verifies launch token.

### Entrypoint policy tests

Scope: `once`, `dispatch`, and daemon orchestration. Prefer fake workflow/supervisor/clock.

Cover:

- `once` and `dispatch` drain only a bounded frontier,
- daemon polls/routes while jobs run,
- daemon reaps/refills promptly,
- daemon shutdown cancels owned jobs and deletes heartbeat only after clean cleanup,
- daemon adopts only verified stale-owner durable jobs,
- unknown durable jobs remain running/operator-visible and do not auto-requeue,
- bounded commands do not adopt or wait on unrelated stale durable jobs.

### End-to-end smokes

Keep these few and meaningful, using real SQLite plus wrapper backend:

- local job succeeds,
- local job fails,
- timeout kills process tree,
- daemon shutdown cancels/finalizes owned jobs,
- daemon restart finalizes a completed wrapper-backed job,
- `once --no-wait` remains unsupported unless durable detached semantics are explicitly implemented.

## Completed setup phases

### Phase E0 — planning and boundaries

Added the initial execution supervisor spec and migration plan.

Commit:

```text
f856a1d Document execution supervisor refactor plan
```

### Phase E0a — harden execution supervisor invariants

Added crash-window, launch-token, adoption/requeue/unknown, wrapper/systemd, and restart-finalization invariants.

Commit:

```text
266dc21 Harden execution supervisor migration invariants
```

## Phase H1 — establish target packages and interfaces

### Goals

Create the architectural seams early, without trying to preserve the old attached design as a polished abstraction.

### Work items

- Add `internal/supervisor` with:
  - `LaunchSpec`,
  - `LaunchRecord`,
  - `Observation`,
  - `RunState`,
  - `CancelReason`,
  - `Supervisor` interface.
- Add `internal/workflow` package skeleton for queue/GitHub lifecycle primitives.
- Move or wrap current dispatcher/daemon workflow operations behind initial workflow functions:
  - heartbeat,
  - recover expired leases,
  - prune stale heartbeats,
  - claim one eligible job,
  - prepare claimed job,
  - finalize observation,
  - cancel owned job.
- Add a minimal attached supervisor adapter only as a bridge so existing commands can compile while wrapper support is built. Keep it small and mark it temporary; do not make it a durable backend.
- Add a simple fake supervisor for workflow/daemon tests.
- Keep current command behavior as much as practical, but do not over-invest in attached backend polish.

### Tests

- Compile-time package boundary checks through existing tests.
- Initial workflow tests around observation-to-status mapping and ownership-drop behavior.
- Standard gates.

### Commit message

```text
Establish execution workflow boundaries
```

## Phase H2 — implement durable job wrapper contract

### Goals

Build the target execution primitive early so later daemon simplification is based on the real durable backend, not the old in-memory handle model.

### Work items

- Add hidden/internal CLI subcommand, e.g. `issueq job-wrapper`.
- Add `internal/jobwrapper` with launch-spec parsing and metadata writing.
- Define wrapper spec input format with job ID, launch token, command, env, workdir, paths, and timeout.
- Validate existing `context.json` before launch.
- Start the user command in a process group.
- Redirect stdout/stderr to configured files.
- Enforce timeout and kill process group.
- Handle cancellation signals deterministically.
- Write `run.json` atomically.
- Add metadata parser and mapper to `supervisor.Observation`.

### Tests

- Job wrapper tests for success, failure, timeout, cancellation, metadata validation, metadata atomic write/parse, and process-tree killing where practical.
- Supervisor metadata mapping tests.
- Standard gates.

### Commit message

```text
Add durable job wrapper execution contract
```

## Phase H3 — add durable launch schema and store primitives

### Goals

Make SQLite able to represent the target running set and launch crash-window protocol. Since there is no production data, prioritize a clean schema over elaborate compatibility.

### Work items

- Add or reset idempotent schema support for required launch fields:
  - `supervisor_kind`,
  - `supervisor_id`,
  - `launch_token`,
  - `launch_state`,
  - `pid`,
  - `pgid`,
  - `process_started_at`,
  - `run_metadata_path`,
  - `launch_spec_path` or equivalent durable launch-spec storage,
  - `context_path`,
  - `result_path`,
  - `stdout_path`,
  - `stderr_path`,
  - `timeout_at`.
- Extend `model.Job`, scan paths, list/JSON output, and tests.
- Add store helpers for:
  - ownership-guarded launch spec persistence, either as a spec file path stored in SQLite or as equivalent durable fields sufficient to reconstruct/verify launch identity,
  - ownership-guarded transition to `launching`,
  - ownership-guarded launch record persistence and transition to `running`,
  - listing owned running jobs,
  - counting running jobs globally/per route,
  - listing stale-owner durable jobs,
  - atomic adoption after read-only verification,
  - stale-owner-safe unknown marking/reporting,
  - stale lease requeue only for rows without durable detached launch metadata.
- Add indexes for running scans, owner scans, route capacity, timeout scans, and stale recovery.

### Tests

- Store tests for schema, launch-state transitions, adoption guards, unknown marking, capacity counts, stale lease requeue, and ownership failures.
- Standard gates.

### Commit message

```text
Add durable launch store primitives
```

## Phase H4 — implement direct wrapper supervisor backend

### Goals

Launch jobs through `issueq job-wrapper` directly and expose durable observations from metadata/PID state.

### Work items

- Implement `supervisor.WrapperSupervisor`.
- Launch wrapper without daemon-context parent-death cancellation.
- Record wrapper PID/PGID/process fingerprint, launch token, and metadata path.
- Inspect:
  - valid matching `run.json`,
  - process existence/fingerprint if metadata is absent,
  - timeout deadline,
  - startup grace,
  - unknown state conservatively.
- Cancel wrapper/user process group with graceful-then-force behavior.
- Add config selection for execution supervisor, but keep the operational default on the existing attached path until H5 rewrites entrypoints around durable reconciliation. H4 may expose wrapper through tests or an explicit experimental config only.
- Keep attached runner only as temporary compatibility/test code if still needed.

### Tests

- Supervisor contract tests against wrapper backend.
- Backend launch/inspect/cancel tests.
- Stale launch token and stale metadata tests.
- Test that elapsed `timeout_at` alone does not authorize blind kill/finalization when launch identity or process termination cannot be verified; such rows remain unknown/operator-visible.
- Standard gates and CLI smokes.

### Commit message

```text
Add direct wrapper supervisor backend
```

## Phase H5 — rewrite workflow and entrypoints around durable reconciliation

### Goals

Move daemon, `once`, and `dispatch` onto workflow primitives plus supervisor observations. This is the main simplification phase.

### Work items

- Implement workflow primitives for:
  - heartbeat/recovery/pruning,
  - claim and prepare job,
  - launch transaction protocol,
  - inspect/finalize owned running jobs,
  - stale-owner verification/adoption,
  - per-row backend dispatch using persisted `supervisor_kind`, including unavailable-backend handling as unknown/operator-visible,
  - timeout/cancel handling,
  - GitHub lifecycle side effects with ownership checks.
- Rewrite daemon as a DB reconciler:
  - heartbeat,
  - poll/route cadence,
  - inspect owned running rows,
  - adopt verified stale-owner durable rows,
  - finalize completed observations,
  - mark/report unknown durable rows conservatively,
  - recover stale non-durable rows,
  - claim/launch by DB-derived capacity,
  - shutdown cleanup with heartbeat deletion only after success.
- Rewrite `once` and `dispatch` as bounded frontier policies using the same workflow primitives.
- Switch new launches and default config to the wrapper supervisor after durable reconciliation paths are in place.
- Keep `once --no-wait` unsupported unless durable detached semantics are explicitly completed.
- Remove or bypass daemon-owned active process map logic.

### Tests

- Workflow tests with fake store/GitHub/supervisor for success, failure, timeout, cancelled, unknown, result JSON, stale route, transition/attempt limits, lost ownership, and restart finalization.
- Entrypoint policy tests for bounded frontier, daemon poll responsiveness, reap/refill, adoption, unknown, and shutdown cleanup.
- E2E smokes with real SQLite plus wrapper backend, including verification that wrapper is the operational default for new launches.
- Standard gates.

### Commit message

```text
Rewrite execution around durable reconciliation
```

## Phase H6 — delete obsolete attached-handle architecture

### Goals

Remove old complexity after wrapper-backed reconciliation is working.

### Work items

- Delete obsolete daemon active-map/process-handle plumbing.
- Delete or reduce attached supervisor/runner compatibility code not needed for tests.
- Simplify dispatcher/daemon tests that only existed for old internals.
- Tighten package boundaries so workflow does not depend on process handles and supervisor does not depend on GitHub/queue policy.
- Run race tests for daemon/workflow/supervisor packages if practical.

### Tests

- Full standard gates.
- Daemon/workflow/supervisor targeted tests.
- Manual local smoke with wrapper backend.
- `go test -race` for targeted packages if practical.

### Commit message

```text
Remove attached process supervision plumbing
```

## Phase H7 — optional systemd backend

### Goals

Provide optional transient unit supervision using the same wrapper contract. This is not required for the core simplification.

### Work items

- Implement `SystemdSupervisor`.
- Launch wrapper via `systemd-run` transient units named with job ID plus launch token.
- Record unit name as `supervisor_id`.
- Inspect unit state via documented systemd CLI/DBus path and wrapper metadata.
- Treat missing unit plus missing/invalid metadata as `RunUnknown`.
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

## Phase H8 — docs and final E2E readiness

### Goals

Document the simplified architecture and prepare for final manual E2E.

### Work items

- Update README and `docs/v1-spec.md` where user-visible behavior/config changed.
- Update `docs/v1-implementation-plan.md` Phase 9 status notes.
- Document wrapper backend behavior, restart recovery, shutdown cleanup, unknown-state operator expectations, and optional systemd tradeoffs.
- Document why `once --no-wait` remains unsupported or define precise criteria for enabling it.

### Tests

- Standard gates.
- Broader manual smokes.
- Final review subagent.

### Commit message

```text
Document durable execution supervisor behavior
```
