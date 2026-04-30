# issueq v1 Implementation Plan

This plan implements [`v1-spec.md`](v1-spec.md). It is intentionally phased so each phase has concrete tests and requirement gates.

## 1. Delivery principles

- Keep v1 boring: Go, SQLite, YAML, GitHub REST, subprocesses.
- Prefer small vertical slices over large untested infrastructure.
- Every phase must leave the CLI runnable.
- Use fake GitHub clients and temporary SQLite databases heavily.
- Do not contact real GitHub in automated tests.
- Do not require a real coding agent in automated tests; use fixture scripts.
- Plans live here; product/technical behavior lives in the spec.

## 2. Test strategy overview

### 2.1 Test layers

1. **Unit tests**
   - config validation
   - predicate matching
   - action merging
   - dedupe key generation
   - attempt/transition decisions

2. **Store integration tests**
   - real SQLite temp DB
   - migrations
   - upserts
   - job enqueue/claim/finalize
   - lease expiry
   - attempt counters

3. **Component tests with fakes**
   - fake GitHub client
   - fake clock where useful
   - fake/fixture subprocess commands
   - poll-route-dispatch without network

4. **CLI smoke tests**
   - build binary
   - run commands against temp config/db
   - assert JSON/table output includes expected records

5. **Manual end-to-end test**
   - optional real GitHub repo/token
   - dry test issue and harmless scripts
   - verify labels/comments transition as expected

### 2.2 Required quality checks

Before each phase is accepted:

```bash
go test ./...
go vet ./...
gofmt -w .
git diff --check
```

Once a Go module exists, CI can run these. Until then, run locally.

### 2.3 Test fixtures

Create fixture task scripts under `testdata/tasks/` or generated temp dirs:

- `success.sh`: writes valid result JSON and exits 0
- `failure.sh`: writes optional result JSON and exits 1
- `sleep.sh`: sleeps longer than timeout
- `bad-result.sh`: writes malformed JSON and exits 0
- `env-dump.sh`: writes selected env/context data to result JSON

## 3. Phase 0 — Project skeleton

### Scope

- Create Go module.
- Add minimal CLI entrypoint.
- Add package layout from spec.
- Add initial README usage notes.
- Add Makefile or just documented commands.

### Requirements addressed

- Foundation for all requirements.

### Tests

- `go test ./...` passes.
- `go vet ./...` passes.
- `go run ./cmd/issueq --help` works.

### Gate

- CLI builds and prints help.
- Repository has no unformatted Go files.
- No runtime config/GitHub/SQLite behavior required yet.

## 4. Phase 1 — Config loading and validation

### Scope

- Implement YAML config structs.
- Parse durations.
- Apply defaults:
  - config path `./issueq.yaml`
  - polling interval `3m`
  - queue backend `sqlite`
  - lease duration `30m`
  - workdir `./.issueq`
  - runner env inheritance disabled unless explicitly enabled
  - minimal subprocess env pass-through such as `PATH` and `HOME`
- Implement validation rules from spec §8.1.
- Add `issueq config-check --config issueq.yaml` if useful, or make `doctor` later.

### Requirements addressed

- R1 Config
- R12 Safety/Auth, partially: argv command shape, timeout validation, and env pass-through validation

### Tests

Unit tests:

- valid sample config loads.
- missing owner/repo fails.
- duplicate route names fail.
- empty command fails.
- non-positive timeout/concurrency/max attempts fail.
- action add/remove conflict fails.
- command string is not accepted as shell string; command must be YAML list.
- invalid env var names in pass-through config fail.
- default config does not inherit the full parent environment.
- including `github.token_env` in subprocess pass-through is rejected or requires the documented explicit acknowledgement if that guard is implemented.

CLI smoke:

- valid config exits 0.
- invalid config exits nonzero with useful message.

### Gate

- Config errors are deterministic and user-readable.
- Sample config from spec can be represented in testdata and loads successfully.

## 5. Phase 2 — SQLite store and migrations

### Scope

- Choose SQLite driver.
- Implement migrations for required v1 tables.
- Implement `QueueStore` basics:
  - `UpsertIssue`
  - `ListRoutableIssues`
  - `EnqueueJob`
  - job list queries
  - issue list queries
  - event insert helper
- Generate portable IDs for jobs/events.

### Requirements addressed

- R2 SQLite
- R5 Inspect, partially
- R14 Future compatibility, partially

### Tests

Store integration tests with temp DB:

- migrations create all tables.
- migrations are idempotent.
- issue upsert inserts then updates labels/title/body.
- enqueue inserts first job and dedupes second by `dedupe_key`.
- jobs sort by priority desc then created asc.
- events can be written and read for debugging.

CLI smoke:

- `issueq issues` on empty DB works.
- `issueq jobs` on empty DB works.

### Gate

- No GitHub dependency required.
- Empty DB is automatically initialized.
- Store methods are safe to call repeatedly.

## 6. Phase 3 — Routing engine

### Scope

- Implement label predicate matcher.
- Implement stable label hash and dedupe key generation.
- Implement route-to-job creation.
- Add `issueq route` command.
- Respect runner capabilities when routing or dispatching. Prefer dispatch enforcement if simpler; tests should cover final behavior before v1 complete.

### Requirements addressed

- R4 Route
- R14 Future compatibility, partially

### Tests

Unit tests:

- include labels all required.
- exclude labels block route.
- closed issue does not match.
- terminal labels block if configured in route exclude list.
- label hash stable regardless of label order.
- dedupe key changes when labels or GitHub updated timestamp changes.

Store/component tests:

- one matching issue creates one pending job.
- repeated route command creates no duplicate jobs.
- two matching routes create two jobs.
- nonmatching issue creates no jobs.

CLI smoke:

- seed issue in test DB, run `issueq route`, inspect job appears.

### Gate

- Router is idempotent.
- Route decisions are deterministic and covered by tests.

## 7. Phase 4 — GitHub polling with fakeable client

### Scope

- Define `GitHubClient` interface.
- Read token from `github.token_env` for GitHub-contacting commands.
- Implement REST-backed client for list/get issue and basic label/comment methods as stubs or full methods depending on sequencing.
- Use the token only in the GitHub client authorization header.
- Redact token values from errors/logs.
- Implement poller that lists open issues for configured repo and upserts snapshots.
- Add `issueq poll` command.
- Keep automated tests on fake client/HTTP test server; no real network tests.

### Requirements addressed

- R3 Poll
- R12 Safety/Auth, partially: GitHub token handling

### Tests

Unit/component tests:

- fake GitHub issues are upserted.
- labels/body/title/state/timestamps are preserved.
- issue key format is correct.
- poll handles empty issue list.
- poll reports GitHub/API errors clearly.
- missing token env var fails for GitHub-contacting commands.
- token value is not present in logged/error strings.

Optional HTTP tests:

- REST client parses GitHub-like JSON from `httptest.Server`.
- HTTP client sends `Authorization` header to test server.

CLI smoke:

- poll command can run against fake transport if supported, or component coverage is accepted until full integration harness exists.

### Gate

- Polling logic is isolated behind `GitHubClient`.
- GitHub client auth is usable without leaking credentials into local state or logs.
- No automated test requires real GitHub credentials.

## 8. Phase 5 — Basic dispatcher and subprocess runner

### Scope

- Implement job claiming with leases.
- Implement running-count capacity checks.
- Implement subprocess invocation as argv, not shell.
- Write context JSON and expected env vars.
- Implement subprocess env construction from explicit allowlist plus job metadata.
- Capture stdout/stderr to files.
- Enforce timeout.
- Mark jobs succeeded/failed.
- Add `issueq dispatch` command.

At this phase, GitHub actions may be no-op or fake-only. Full action application lands in Phase 6.

### Requirements addressed

- R6 Dispatch
- R7 Context
- R8 Results, partially
- R12 Safety/Auth
- R13 Observability, partially

### Tests

Store integration:

- claim pending job sets `running`, `locked_by`, `lease_until`, `started_at`.
- expired lease can be released.
- non-expired running job is not claimed twice.

Runner/component tests:

- success script exits 0 -> job `succeeded`.
- failure script exits 1 -> job `failed`.
- timeout script is killed -> job `failed` with timeout error.
- context file contains issue/job/runner data.
- env vars are present.
- subprocess receives allowlisted env vars.
- subprocess does not receive `GITHUB_TOKEN`/the `github.token_env` value by default.
- stdout/stderr files are written.
- global concurrency limit is respected.
- per-route concurrency limit is respected.
- issue content is not shell-interpolated; malicious title/body cannot alter command argv.

CLI smoke:

- seed DB with one job and fixture command, run `issueq dispatch`, inspect succeeded job and log files.

### Gate

- Dispatcher can run local fixture jobs end-to-end without GitHub.
- Timeout behavior is reliable.
- No shell-string execution path exists.
- Parent environment secrets are not leaked to subprocesses unless explicitly allowlisted.

## 9. Phase 6 — GitHub actions, staleness, and result JSON

### Scope

- Implement action application:
  - remove labels
  - add labels
  - create comment
  - update local issue snapshot
  - record event
- Re-fetch latest issue before dispatch and skip stale jobs.
- Apply `on_start` before subprocess spawn.
- Apply `on_success`/`on_failure` after subprocess completion.
- Parse optional result JSON and merge per spec §13.4.
- Treat malformed result JSON as failure.

### Requirements addressed

- R8 Results, complete
- R9 GitHub actions
- R10 Staleness
- R13 Observability, partially

### Tests

Unit tests:

- action merge concatenates comments.
- result-file label ops win conflicts.
- malformed result JSON returns controlled error.

Component tests with fake GitHub:

- stale issue no longer matching route -> job skipped, no subprocess spawned.
- on_start removes `agent-ready`, adds `agent-running`.
- success applies success labels/comment plus result labels/comment.
- failure applies failure labels/comment.
- absent label removal is treated as success.
- action application order is remove then add then comment.
- local issue snapshot updates after actions.

Fixture subprocess tests:

- result JSON with PR comment is appended to configured success comment.
- bad-result script causes failure path.

### Gate

- A fake issue can move `agent-ready -> agent-running -> agent-review` through one dispatch.
- Stale jobs are skipped safely.
- Action behavior is deterministic and tested.

## 10. Phase 7 — Attempts and loop prevention

### Scope

- Implement `issue_state` and `route_attempts` behavior.
- Increment attempts before `on_start`.
- Enforce `max_attempts`.
- Increment transitions after label-changing actions.
- Enforce `max_transitions_per_issue` with configured terminal action.
- Mark jobs `dead` where appropriate.

### Requirements addressed

- R11 Loop prevention
- R9 GitHub actions, terminalization paths

### Tests

Store integration:

- attempts are scoped by issue/generation/route.
- attempts increment atomically.
- transition count increments.

Component tests:

- attempt 1 within max spawns subprocess.
- attempt max+1 does not spawn subprocess.
- attempts-exceeded applies configured labels/comment and marks job dead.
- transition limit exceeded applies workflow terminal action.
- review loop simulation stops after configured attempts.
- generation value scopes attempts, even if reset command is not implemented yet.

### Gate

- Infinite route loops are bounded by tests.
- Attempts count crashes/on_start failures because increment happens before `on_start`.

## 11. Phase 8 — Daemon loop and operator UX

### Scope

- Implement full `daemon` loop.
- Implement `once` semantics:
  - waits by default
  - `--no-wait` returns after spawning
- Improve `jobs`/`issues` output.
- Add `--json` to inspect commands.
- Add structured logging.
- Add graceful shutdown: stop polling, wait/terminate children, persist status.
- Add sample config and sample task scripts.

### Requirements addressed

- R5 Inspect, complete
- R13 Observability, complete
- All v1 requirements integrated

### Tests

Component tests:

- daemon loop with fake clock/client processes poll-route-dispatch once or multiple times.
- graceful shutdown does not corrupt running job state.
- `once` waits by default.
- `once --no-wait` starts and returns.

CLI smoke:

- `issueq jobs --json` emits parseable JSON.
- `issueq issues --json` emits parseable JSON.
- sample config validates.
- sample success task can run through `once` using fake/local harness where possible.

### Gate

- Operator can understand local state with CLI commands.
- Daemon can be stopped cleanly.
- Sample files are usable as a starting point.

## 12. Phase 9 — Manual end-to-end validation and v1 hardening

### Scope

- Run against a disposable real GitHub repository or test issue.
- Verify labels/comments with a harmless local script.
- Fix edge cases found during manual use.
- Add packaging/build notes.
- Optional systemd unit example.

### Requirements addressed

- Final v1 acceptance across R1-R14.

### Manual test script

1. Create test repo or choose a disposable issue.
2. Create labels: `agent-ready`, `agent-running`, `agent-review`, `agent-done`, `agent-failed`, `agent-needs-human`.
3. Configure `issueq.yaml` for the repo.
4. Use a task script that writes result JSON and exits 0.
5. Add `agent-ready` to issue.
6. Run `issueq once --config issueq.yaml`.
7. Verify:
   - job created
   - `agent-ready` removed
   - `agent-running` added during execution
   - success comment posted
   - `agent-review` or configured next label added
   - stdout/stderr/context/result files exist
8. Repeat with failing script.
9. Repeat with stale label removal before dispatch.
10. Repeat attempt-exceeded path.

### Gate

- All automated tests pass.
- Manual GitHub smoke test passes.
- README documents minimal setup and warnings.
- Known limitations are documented.

## 13. Cross-phase requirement matrix

| Requirement | Main phase | Notes |
| --- | --- | --- |
| R1 Config | Phase 1 | Defaults and validation |
| R2 SQLite | Phase 2 | Migrations and store |
| R3 Poll | Phase 4 | Fakeable GitHub client |
| R4 Route | Phase 3 | Label predicates and dedupe |
| R5 Inspect | Phases 2, 8 | Basic then polished output |
| R6 Dispatch | Phase 5 | Leases/capacity/spawn |
| R7 Context | Phase 5 | JSON/env contract |
| R8 Results | Phases 5, 6 | Exit/timeout then result JSON |
| R9 GitHub actions | Phase 6 | Start/success/failure labels/comments |
| R10 Staleness | Phase 6 | Re-fetch before spawn |
| R11 Loop prevention | Phase 7 | Attempts/transitions |
| R12 Safety/Auth | Phases 1, 4, 5 | Argv arrays, timeouts, token handling, env allowlist, no shell interpolation |
| R13 Observability | Phases 5, 8 | Logs/events/CLI output |
| R14 Future compatibility | Phases 2, 3, 5 | Interfaces, IDs, leases |

## 14. Deferral list

Do not implement in v1 unless required by real use:

- Postgres backend.
- Webhooks.
- Multiple repos per config.
- Rich predicates.
- Direct queue append support from result JSON.
- Structured GitHub state comment.
- Exit-code outcome mapping beyond success/failure.
- Web dashboard.
