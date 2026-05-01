# Execution supervisor architecture spec

This document defines the target architecture for simplifying issueq's concurrent job execution while preserving durable local queue semantics and GitHub lifecycle correctness. Because the project is still pre-production, the migration may hard-cut over to cleaner boundaries instead of preserving every intermediate implementation shape.

It supersedes the implementation shape in `docs/concurrency-supervision-design.md` where the daemon directly owns live Go subprocess handles. That design remains the current baseline; this spec describes the next refactor direction.

## Goals

- Keep configured concurrency: more than one job may run at a time.
- Make SQLite the durable source of truth for queued and running work.
- Share workflow primitives between `once`, `dispatch`, and `daemon`.
- Move child-process supervision behind a small execution-supervisor abstraction.
- Support a stable `issueq job-wrapper` execution contract.
- Allow multiple launch backends:
  - direct wrapper process supervision as the preferred default,
  - systemd transient units running the wrapper as an optional backend,
  - current attached Go process supervision only as a temporary migration/test bridge.
- Keep daemon logic as a reconciler over durable state, not an owner of complex in-memory process maps.
- Preserve existing GitHub lifecycle safeguards: ownership checks before side effects, stale route checks, attempt/transition limits, result action handling, and stale owner drops.
- Preserve graceful shutdown behavior: stop claiming new work, cancel owned running jobs, finalize while ownership is held, and delete heartbeat only after cleanup succeeds.

## Non-goals

- Removing SQLite.
- Replacing SQLite with ZeroMQ or another messaging layer.
- Requiring systemd for all installations.
- Supporting cross-host distributed execution beyond the SQLite ownership/heartbeat model.
- Re-enabling `once --no-wait` before durable detached supervision is fully implemented.
- Perfect portable PID-only supervision without a wrapper or OS supervisor.

## Core idea

Separate issueq into three layers:

```text
workflow/queue layer
  poll GitHub
  route issues
  claim jobs
  apply lifecycle actions
  finalize jobs

execution supervisor layer
  launch job execution
  inspect launched work
  cancel launched work
  expose durable run observations

entrypoint/policy layer
  once
  dispatch
  daemon
```

The workflow layer decides *what should happen* to jobs. The execution layer decides *how a command is launched, observed, and cancelled*. Entrypoints compose shared primitives with different loop policies.

## Shared workflow primitives

The following operations should become explicit package-level primitives. Names are illustrative.

```go
PollIssues(ctx, cfg, gh, queue) (poller.Result, error)
RouteIssues(ctx, cfg, queue) (router.Result, error)
HeartbeatRunner(ctx, queue, identity) error
RecoverExpiredLeases(ctx, queue, identity, activeIDs) (int, error)
PruneStaleHeartbeats(ctx, queue, before) (int, error)
ClaimOne(ctx, cfg, queue, identity, limits) (*model.Job, error)
PrepareClaimedJob(ctx, cfg, queue, gh, identity, job) (PreparedJob, ClaimOutcome, error)
LaunchPreparedJob(ctx, supervisor, prepared) (LaunchRecord, error)
InspectOwnedRunning(ctx, supervisor, job) (Observation, error)
FinalizeObservation(ctx, cfg, queue, gh, identity, job, obs) error
CancelOwnedJob(ctx, supervisor, job, cause) error
DeleteHeartbeatAfterCleanShutdown(ctx, queue, identity) error
```

The current dispatcher code contains many of these concerns already, but they are coupled to attached child handles and daemon loop behavior. The refactor should make them separately testable.

## Entry point policies

### `issueq once`

`once` remains a bounded reconciliation wave:

1. heartbeat runner,
2. recover safely reclaimable expired work,
3. poll GitHub,
4. route issues,
5. capture the initial eligible frontier,
6. claim and launch jobs from that frontier up to capacity,
7. inspect/reap/release/renew until the frontier is drained and jobs launched by this wave are finalized,
8. return.

`once` should not wait forever for future jobs created after the wave frontier is captured.

### `issueq dispatch`

`dispatch` is the same bounded wave without poll/route. It should share the same claim/launch/inspect/finalize primitives as `once` and daemon.

### `issueq daemon`

Daemon is a long-lived reconciler:

1. maintain runner heartbeat,
2. poll/route on cadence,
3. inspect running jobs owned by the current runner frequently,
4. adopt safely recoverable stale-owner jobs when durable launch metadata allows it,
5. finalize completed jobs,
6. cancel timed-out jobs,
7. recover safely reclaimable expired leases,
8. claim and launch new work when capacity is available,
9. shut down by cancelling owned running jobs and finalizing them with a bounded cleanup context.

Daemon should not be implemented as `for { Once(); sleep(...) }`, because a bounded wave can block poll/route/heartbeat/renew behind long-running work. Daemon and `once` should share primitives, not call each other as black boxes.

## Durable running state

The target design should move active execution state into SQLite as much as possible. A running job should carry enough durable launch information for a later daemon process to inspect, cancel, or recover it.

Required per-launch fields should be explicit, not inferred from reused job IDs or artifact paths:

```text
supervisor_kind          attached | wrapper | systemd
supervisor_id            backend-specific ID: unit name, wrapper pid, handle id, etc.
launch_token             unique token for this launch attempt
launch_state             preparing | launching | running | unknown
run_metadata_path        unique path to this launch's run.json / wrapper metadata
context_path             context JSON path
result_path              command result JSON path
stdout_path              stdout log path
stderr_path              stderr log path
timeout_at               wall-clock timeout deadline
```

Backend-specific optional fields may include:

```text
pid                      observed process/wrapper pid, if available
pgid                     process group id, if available
process_started_at       OS process start-time/fingerprint, if available
systemd_unit             transient unit name, if distinct from supervisor_id
```

`launch_token` is mandatory for wrapper and systemd backends and should be included in the launch spec, launch record, wrapper metadata, and systemd unit name. Metadata paths must be unique per launch token. A daemon must never finalize or cancel a run using stale `run.json`, PID, or unit evidence from a previous launch; inspection must verify job ID plus launch token, and should verify attempt/generation where available.

`status = running` plus durable launch metadata is the daemon's observable active set. Jobs owned by the current `runner_instance_id` may be renewed, inspected, cancelled, and finalized directly. Jobs owned by another runner instance require the adoption policy below before any ownership-guarded mutation.

In-memory state may still exist for an attached migration backend, but daemon policy should increasingly reconcile from DB rows rather than from an active handle map. Rows launched by the attached backend may record launch bookkeeping, but that bookkeeping is not durable detached supervision metadata. After daemon crash, attached rows follow the existing stale lease recovery path because no later process can safely inspect their live handles.

### Launch transaction and crash-window invariants

Launching a job crosses a process boundary, so the workflow layer must define crash-safe intermediate states. The exact schema may evolve, but the observable state machine should preserve these invariants:

```text
pending -> running/preparing -> running/launching -> running/running -> terminal
                                      \-> running/unknown
```

Recommended protocol:

1. claim the job with ownership, setting `status = running`, `launch_state = preparing`, and the current `runner_instance_id`,
2. write `context.json` and reserve unique per-launch artifact paths using a fresh `launch_token`,
3. persist the launch specification/paths/token with an ownership guard while still in `preparing`,
4. immediately before any backend side effect, atomically set `launch_state = launching` with an ownership guard,
5. start the backend using that launch token,
6. persist the returned launch record and set `launch_state = running` with an ownership guard,
7. inspect/finalize only observations that match the persisted launch token.

Crash recovery must be deterministic for every boundary:

- claimed/preparing with no launch token/spec: recover by stale lease requeue when the owner heartbeat is stale or missing,
- preparing with launch token/spec but before `launching`: no backend side effect should have occurred yet, so stale lease requeue is allowed after stale/missing heartbeat,
- launching with durable launch spec but no verified backend identity: mark or leave `unknown`; do not finalize or PID-kill blindly,
- spawned backend before launch record persistence: avoided where practical by making the wrapper/systemd identity deterministic from the launch token; otherwise recovery must treat the row as `unknown`, not requeue automatically,
- launch record persisted before backend is observable: inspect returns `starting` until a short backend-defined startup grace expires, then `unknown`,
- completed metadata with mismatched launch token: ignore as stale evidence and report `unknown` or continue inspecting other verified evidence.

Launch failures while ownership is still held should finalize through the normal failure path unless the failure was caused by shutdown/cancellation, which finalizes as `cancelled`. If ownership is lost during launch or launch-record persistence, the stale process must drop local mutations and avoid GitHub side effects; later recovery/adoption handles the row.

## Execution supervisor interface

The workflow layer talks to a backend-neutral interface:

```go
type Supervisor interface {
    Launch(context.Context, LaunchSpec) (LaunchRecord, error)
    Inspect(context.Context, LaunchRecord) (Observation, error)
    Cancel(context.Context, LaunchRecord, CancelReason) error
}
```

Suggested data types:

```go
type LaunchSpec struct {
    JobID        string
    LaunchToken  string
    Command      []string
    Env          []string
    Workdir      string
    ContextPath  string
    ResultPath   string
    StdoutPath   string
    StderrPath   string
    MetadataPath string
    Timeout      time.Duration
}

type LaunchRecord struct {
    Kind         string // attached, wrapper, systemd
    ID           string // handle id, wrapper pid, unit name, etc.
    LaunchToken  string
    PID          int
    PGID         int
    MetadataPath string
    StartedAt    time.Time
    TimeoutAt    time.Time
}

type RunState string

const (
    RunStarting  RunState = "starting"
    RunRunning   RunState = "running"
    RunExited    RunState = "exited"
    RunFailed    RunState = "failed"
    RunTimedOut  RunState = "timed_out"
    RunCancelled RunState = "cancelled"
    RunUnknown   RunState = "unknown"
)

type Observation struct {
    State       RunState
    ExitCode    int
    HasExitCode bool
    Error       string
    StartedAt   time.Time
    FinishedAt  time.Time
    ResultPath   string
    StdoutPath   string
    StderrPath   string
}
```

The workflow layer maps `Observation` to issueq job statuses:

```text
RunStarting            -> keep running and renew if owned
RunRunning             -> keep running and renew if owned
RunExited + exit 0      -> succeeded
RunExited + nonzero     -> failed
RunFailed               -> failed
RunTimedOut             -> failed, timed_out message
RunCancelled            -> cancelled
RunUnknown              -> keep running, mark/audit unknown, require policy or operator action
```

Cancelled observations skip GitHub success/failure/result actions and finalize as `cancelled` while ownership is held.

## Backends

### Attached supervisor backend

The attached backend wraps the current Go `runner.Start` / `runner.Wait` behavior. It is a temporary migration bridge and test aid, not the preferred long-term architecture.

Properties:

- launches user command from the daemon/once process,
- stores process handles in backend-private memory,
- supports current tests and behavior,
- should not leak live handles into daemon/workflow code.

This backend can preserve existing behavior during early refactor phases, but should be removed or minimized once wrapper-backed reconciliation works.

### Wrapper supervisor backend

The wrapper backend launches `issueq job-wrapper` directly as a child process. The wrapper owns the user command execution and writes durable run metadata.

For durable crash recovery, the wrapper must be launched so it can outlive daemon crashes without receiving an accidental parent-death cancellation. Durable wrapper launch must not be tied to the daemon request context in the way `exec.CommandContext` normally kills children. It should use a separate process group/session where appropriate, close inherited resources that would keep daemon pipes/files alive, and persist enough identity to verify the wrapper/process later, e.g. PID plus process start time or PGID plus metadata path. If the platform cannot provide safe identity verification, recovery must treat the launch state as unknown rather than killing/finalizing by PID alone.

Properties:

- daemon records wrapper PID/PGID/process fingerprint/metadata path,
- wrapper starts the user command in its own process group,
- wrapper redirects stdout/stderr to configured files,
- wrapper waits on the command and records exit status,
- wrapper enforces the issueq timeout and writes timeout metadata,
- daemon inspects PID/PGID plus metadata file,
- cancellation should first signal the wrapper/process group, wait a short backend-defined grace period, then force-kill if needed; repeated cancel is idempotent,
- if the wrapper is killed before writing `run.json`, the daemon may synthesize a cancelled/timed-out observation only while ownership is held and after it has verified backend termination under the persisted launch token; otherwise inspection reports `unknown`.

This backend is the preferred portable long-term default if systemd is not required.

### Systemd supervisor backend

The systemd backend launches the same wrapper under a transient systemd unit:

```text
systemd-run --unit issueq-job-<jobID>-<launchToken> --property=KillMode=control-group --property=RuntimeMaxSec=<timeout+grace> --property=CollectMode=inactive-or-failed ... issueq job-wrapper ...
```

Unit names must be sanitized and collision-resistant, e.g. include the job ID and launch token. Timeout ownership should be explicit: the wrapper owns the issueq timeout and writes issueq metadata; systemd `RuntimeMaxSec`, if used, should be a timeout plus grace safety net. The backend should request cleanup/collection where supported and should define stop/kill grace behavior consistently with wrapper cancellation.

Properties:

- systemd owns process lifetime, cgroups, and process-tree killing,
- daemon records unit name as `supervisor_id`,
- inspect maps systemd unit state plus wrapper metadata to `Observation`,
- valid wrapper `run.json` is authoritative for completed issueq semantics,
- missing unit plus valid metadata may still be finalized from metadata,
- missing unit plus missing or invalid metadata is `RunUnknown`, not success or failure,
- cancel stops/kills the unit,
- still uses wrapper metadata for issueq-specific exit/result semantics.

Systemd is a backend, not the core architecture. Core workflow should depend only on the `Supervisor` interface and issueq observation states.

## Job wrapper contract

`issueq job-wrapper` is the stable execution contract for wrapper and systemd backends.

Responsibilities:

1. receive a launch spec via args or a spec file,
2. validate an existing `context.json` written by the workflow layer before launch,
3. open stdout/stderr log files,
4. start the user command in its own process group,
5. wait for completion,
6. enforce timeout or handle cancellation signal,
7. kill process group on timeout/cancellation,
8. write run metadata atomically.

Suggested launch spec file:

```json
{
  "version": 1,
  "job_id": "job_...",
  "launch_token": "01...",
  "command": ["./task.sh"],
  "env": ["A=B"],
  "workdir": "/repo",
  "context_path": ".../context.json",
  "result_path": ".../result.json",
  "stdout_path": ".../stdout.log",
  "stderr_path": ".../stderr.log",
  "metadata_path": ".../run.json",
  "timeout_seconds": 1800
}
```

Wrapper cancellation should be deterministic. On `SIGTERM` or `SIGINT`, the wrapper should signal the user command process group, wait a short grace period, force-kill if needed, and write cancelled metadata when possible. Timeout takes precedence over later cancellation if the timeout decision happened first. Repeated cancellation signals should be safe.

Suggested metadata file, e.g. `run.json`:

```json
{
  "version": 1,
  "job_id": "job_...",
  "launch_token": "01...",
  "pid": 1234,
  "pgid": 1234,
  "started_at": "2026-01-01T00:00:00Z",
  "finished_at": "2026-01-01T00:01:00Z",
  "exit_code": 0,
  "timed_out": false,
  "cancelled": false,
  "error": "",
  "context_path": ".../context.json",
  "result_path": ".../result.json",
  "stdout_path": ".../stdout.log",
  "stderr_path": ".../stderr.log"
}
```

`context.json` has one authoritative writer: the workflow layer prepares it before launch. The wrapper validates that the path exists and matches the requested job ID and launch token if present, but does not rewrite it. This avoids races between DB launch-record persistence and wrapper startup.

The wrapper must write metadata atomically, e.g. write `run.json.tmp`, fsync/close as practical, then rename to `run.json`.

## Concurrency model

Concurrency is determined by durable running jobs:

```text
capacity = queue.max_global_concurrency - count(status = running)
route capacity = route.job.concurrency - count(status = running and route_name = route)
```

A daemon may launch jobs while capacity remains. Each launched job is represented by a running DB row with supervisor launch data.

The daemon may keep small in-flight launch state to avoid double-starting a just-claimed row, but long-lived active execution should be represented durably.

## Ownership and side effects

The ownership rules from `docs/concurrency-supervision-design.md` remain authoritative:

- post-claim mutations are guarded by `runner_instance_id`,
- lease renewal matches job ID and owner instance,
- GitHub side effects are preceded by ownership checks/renewals,
- stale owner writes are dropped rather than converted into command failures,
- ownership loss prevents stale finalization and GitHub side effects.

The supervisor abstraction must not bypass ownership. Launch record persistence and finalization must be ownership-guarded.

## Adoption, restart, and recovery

Durable wrapper/systemd supervision creates a new case: a job can keep running after the issueq daemon that claimed it has exited. A later daemon must not blindly requeue that job, and it also must not mutate/finalize it while it is still owned by the dead runner instance.

Recovery therefore uses explicit adoption.

### Adoption preconditions

A daemon may adopt a `running` job owned by another `runner_instance_id` only if all are true:

- the job lease is expired,
- the owner heartbeat is missing or stale,
- the job has durable supervisor metadata (`supervisor_kind`, `launch_token`, `supervisor_id` or metadata path) sufficient for the selected backend to inspect/cancel it,
- the backend can verify launch identity using read-only evidence before adoption, e.g. completed metadata exists, the systemd unit is known, or the wrapper/process identity is verified.

Rows without durable launch metadata follow the existing stale lease recovery path: requeue only after expired lease plus stale/missing heartbeat. They must not be PID-killed or finalized based on PID alone.

Rows with durable launch metadata but unverifiable launch identity are different: they must not be automatically requeued, because doing so can duplicate a still-running detached wrapper/systemd job. Report them as unknown and keep them `running`. A daemon that has not adopted the job must not perform normal ownership-guarded mutations; it may either report unknown read-only, or use a dedicated stale-owner-safe store method that only marks `launch_state = unknown` when the lease is expired and the old owner heartbeat is stale/missing, without changing ownership, cancelling, finalizing, or applying GitHub side effects.

### Adoption operation

Adoption must be an atomic ownership transfer, not a stale-owner write. Add a store method similar to:

```go
AdoptStaleRunningJob(ctx, jobID string, oldRunnerInstanceID string, newIdentity model.RunnerIdentity, leaseDuration time.Duration) (*model.Job, error)
```

The update must match:

```text
id = jobID
status = running
runner_instance_id = oldRunnerInstanceID
lease_until < now
old heartbeat missing or stale
```

On success it sets:

```text
runner_instance_id = newIdentity.InstanceID
locked_by = newIdentity.RunnerID
lease_until = now + leaseDuration
updated_at = now
```

After adoption, the new daemon may inspect, cancel, renew, or finalize the job through normal ownership-guarded paths.

### Recovery flow

On daemon start and periodically:

- heartbeat current runner instance,
- inspect and renew running jobs already owned by the current runner instance,
- for stale-owner jobs with durable launch metadata, perform a read-only backend verification/probe and attempt atomic adoption only when identity is verified,
- after adoption, inspect the backend observation:
  - completed wrapper/systemd metadata with matching launch token may be finalized,
  - still-running verified work may be renewed and left running,
  - timed-out verified work may be cancelled and finalized,
  - unknown launch state should remain running until an explicit operator intervention/stale-unknown policy handles it; unknown state may continue to consume configured capacity by design,
- release/requeue expired jobs only when they have no durable launch metadata, are not safely inspectable/adoptable, and the owner heartbeat is stale/missing.

On restart, reconciliation must dispatch by each row's persisted `supervisor_kind`, not merely by the currently configured default backend for new launches. If support for an existing row's backend is unavailable, inspection should leave the row `unknown` and operator-visible rather than requeueing or finalizing it.

`once` and `dispatch` remain bounded waves. Full stale-owner adoption is a daemon responsibility. Bounded commands may perform a nonblocking recovery pre-pass for jobs in their captured frontier, but must not wait indefinitely on unrelated stale durable jobs or adopt work outside their bounded policy.

## Shutdown behavior

Daemon shutdown policy:

1. stop starting new poll/route/claim/launch work,
2. cancel owned running jobs via `Supervisor.Cancel`,
3. inspect/finalize cancelled jobs using a bounded cleanup context derived from `context.Background()`,
4. keep heartbeat during cleanup,
5. delete current heartbeat only after owned active jobs are finalized, skipped due ownership loss, or otherwise safely dropped by successful cleanup.

If cleanup fails or times out, heartbeat deletion should be skipped so recovery can distinguish an unclean owner.

## Finalization after restart

After adoption, finalization must reconstruct the same workflow context used by normal dispatch without trusting stale side effects from the dead runner. `FinalizeObservation` should:

1. load the current job, route config, artifact paths, and launch token,
2. validate matching wrapper metadata and result/context paths,
3. refresh the issue when GitHub lifecycle actions are enabled,
4. re-check ownership/renew lease before external side effects,
5. re-run stale-route, attempt, transition, and result-action policy checks,
6. drop GitHub side effects and finalization if ownership is lost,
7. map cancelled observations to `cancelled` without success/failure/result actions.

The persisted `context.json` is subprocess input and audit evidence; it is not sufficient by itself to authorize post-run GitHub actions after restart. Current DB state, route config, and refreshed issue state remain authoritative for side-effect decisions.

## Restart and recovery

Restart behavior is governed by the adoption policy above. The key rule is: inspect/finalize/cancel another runner's `running` job only after atomic adoption. Jobs without durable launch metadata are not inspectable after daemon crash and can only be recovered through stale-lease requeue semantics.

## Why not ZeroMQ as the queue

ZeroMQ may be useful later as a live notification or worker IPC layer, but it does not replace durable job state, process supervision, or GitHub lifecycle ownership. SQLite remains the authoritative queue. If added later, ZeroMQ should be an optional control plane, not the source of truth.

## Testing strategy

Tests should mirror the architecture boundaries. Prefer focused tests at the layer that owns the behavior instead of proving store, GitHub lifecycle, subprocess, and daemon policy indirectly through large daemon tests.

```text
store tests          durable state transitions and ownership guards
supervisor tests     launch/inspect/cancel execution observations
job-wrapper tests    wrapper process contract and metadata
workflow tests       queue/GitHub/job lifecycle decisions
entrypoint tests     once/dispatch/daemon policy and scheduling
E2E smokes           small real SQLite + wrapper confidence checks
```

### Store tests

Scope: SQLite state transitions only. No GitHub and no subprocesses.

Cover:

- claim capacity and per-route concurrency,
- heartbeat insert/update/delete/prune,
- lease renewal ownership guards,
- launch-record persistence and launch-state transitions,
- crash-window state transitions around claim/context/launch-record/spawn,
- stale lease requeue for rows without durable detached launch metadata,
- stale-owner adoption guards for durable launch metadata,
- stale-owner-safe unknown marking/reporting,
- ownership-guarded finalization and lost-ownership errors,
- indexes/scans for running jobs, owner jobs, route capacity, timeout, and recovery.

### Supervisor contract tests

Scope: `internal/supervisor` behavior. No queue and no GitHub.

Use a shared contract test style where practical for fake, wrapper, and future systemd backends.

Cover:

- launch returns a valid `LaunchRecord`,
- inspect maps backend state to `starting`, `running`, exited zero, exited nonzero, timed out, cancelled, and unknown,
- stale `run.json`, stale unit evidence, or mismatched launch token is rejected or reported unknown,
- PID reuse or process start-time mismatch is conservative where the platform exposes enough evidence,
- elapsed `timeout_at` alone does not authorize blind kill/finalization without verified launch identity,
- cancellation is idempotent,
- stdout/stderr/result/metadata paths are respected,
- backend mismatch or missing backend support is conservative and operator-visible.

### Job wrapper tests

Scope: `issueq job-wrapper` / `internal/jobwrapper` execution contract.

Cover:

- validates existing `context.json`, job ID, and launch token,
- does not rewrite context,
- captures stdout/stderr,
- records exit code, timeout, cancellation, and paths in `run.json`,
- writes metadata atomically,
- enforces timeout and kills process group,
- handles cancellation signals and repeated cancellation,
- preserves timeout-vs-cancel precedence,
- parses metadata into `Observation` correctly.

### Workflow tests

Scope: queue/GitHub/job lifecycle decisions using fake store, fake GitHub, and fake supervisor observations.

Cover:

- prepare claimed job writes context and launch spec,
- ownership checks before `on_start` and terminal side effects,
- success/failure/cancelled/timeout/unknown observation mapping,
- cancelled observations skip success/failure/result GitHub actions,
- result JSON merge and malformed result behavior,
- stale route and transition/attempt policy,
- lost ownership drops GitHub actions and finalization,
- finalization after restart reloads current job/config/issue state and verifies launch token.

### Entrypoint policy tests

Scope: `once`, `dispatch`, and daemon orchestration. Prefer fake workflow/supervisor/clock.

Cover:

- `once` completes a bounded frontier,
- `dispatch` completes a bounded frontier,
- bounded commands do not adopt or wait on unrelated stale durable jobs,
- daemon polls/routes while jobs run,
- daemon reaps/refills without waiting for poll interval,
- daemon adopts only verified stale-owner durable jobs,
- unknown durable jobs remain running/operator-visible and do not auto-requeue,
- daemon shutdown cancels/finalizes owned jobs,
- heartbeat deletion happens only after clean shutdown,
- `once --no-wait` remains unsupported until detached supervision is durable.

### End-to-end smokes

Keep E2E tests few and meaningful, using real SQLite plus the wrapper backend:

- local job succeeds,
- local job fails,
- timeout kills process tree,
- daemon shutdown cancels/finalizes owned jobs,
- daemon restart finalizes a completed wrapper-backed job,
- wrapper is the operational default after cutover,
- `once --no-wait` remains unsupported unless durable detached semantics are explicitly implemented.
