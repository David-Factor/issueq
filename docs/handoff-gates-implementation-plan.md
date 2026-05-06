# First-Class Handoffs and Route Gates Implementation Plan

This plan implements [First-Class Handoffs and Route Gates Spec](handoff-gates-spec.md).

## 1. Delivery principles

- Preserve existing configs and route behavior unless `gate` or
  `attempt_scope` is configured.
- Keep issueq workflow-agnostic. Route names and decision strings are config
  data, not hardcoded domain concepts.
- Make handoffs visible and queryable in SQLite, while preserving full opaque
  payload JSON.
- Count only real work attempts against route attempt budgets.
- Keep global transition limits as the loop circuit breaker.
- Prefer fake GitHub/component tests before live smoke.

Required checks for every phase:

```bash
go test ./...
go vet ./...
gofmt -w ./cmd ./internal
git diff --check
```

## Phase 1 — Spec, config shape, and no-op compatibility

### Scope

- Add documentation for handoffs, gates, scoped attempts, and smoke scenario.
- Extend config structs to parse optional route fields without changing behavior:
  - `gate.handoff.required`
  - `gate.handoff.from`
  - `gate.handoff.decisions`
  - `gate.handoff.next_route`
  - `gate.handoff.freshness`
  - `gate.on_block`
  - `job.attempt_scope`
- Validate the new config fields generically.
- Keep runtime behavior unchanged until later phases wire evaluation.

### Requirement gates

- Existing example configs load unchanged.
- New example config with a handoff gate loads and validates.
- Invalid gate config fails deterministically:
  - unknown freshness policy;
  - `required: true` with empty `from` when a producer route is required;
  - unknown `attempt_scope` if using enumerated scopes.

### Tests

- Config unit tests for backward compatibility.
- Config unit tests for valid/invalid gate blocks.
- CLI `config-check` smoke against a gated fixture config.

## Phase 2 — Handoff parsing and SQLite storage

### Scope

- Add migration for `handoffs` table and indexes.
- Implement handoff envelope parser for canonical `issueq-handoff` fenced JSON
  blocks with schema `issueq-handoff/v1` in issue comments or result comments.
- Store handoffs idempotently.
- Preserve full payload in `payload_json`.
- Expose store methods:
  - `UpsertHandoff`
  - `ListHandoffsForIssue`
  - `LatestMatchingHandoff`

### Requirement gates

- Polling or routing can ingest existing issue comments containing handoffs.
- Re-polling the same issue does not duplicate handoffs.
- issueq does not interpret decision strings beyond storing/indexing them.
- Missing/malformed handoff payloads are ignored with local diagnostics, not
  route-breaking panics.

### Tests

- Store migration test for `handoffs` schema.
- Parser tests:
  - extracts a valid handoff from a comment;
  - ignores non-handoff comments;
  - rejects malformed JSON safely;
  - preserves unknown fields in `payload_json`.
- Store integration tests for idempotent upsert and latest matching lookup.

## Phase 3 — Gate evaluation engine

### Scope

- Add route gate evaluator before job enqueue.
- Implement handoff gate predicates:
  - required handoff from configured route names;
  - optional decision allowlist;
  - optional next-route match;
  - source freshness policy for GitHub issue source fingerprints.
- Add gate block result type with reason codes:
  - `missing_handoff`
  - `decision_not_allowed`
  - `next_route_mismatch`
  - `source_stale`
  - `target_stale` reserved for later PR/CI targets.
- Add optional `gate_blocks` table or equivalent dedupe mechanism.
- Apply `gate.on_block` labels/comments without enqueueing a work job.

### Requirement gates

- A matching route with a failing gate does not enqueue a job.
- A gate block does not increment route attempts.
- Duplicate gate blocks do not spam identical comments.
- A matching fresh handoff allows normal job enqueue.
- Routes without gates behave exactly as before.

### Tests

- Router component tests with fake issue snapshots and fake GitHub actions:
  - missing handoff blocks and applies configured block action;
  - missing handoff creates no job;
  - accepted handoff creates a job;
  - stale source blocks;
  - decision allowlist blocks unknown decisions;
  - duplicate gate block is deduped.
- Store tests for gate block dedupe if a table is added.

## Phase 4 — Scoped work attempts

### Scope

- Migrate `route_attempts` to include `scope_hash`.
- Add attempt scope derivation:
  - `legacy` default;
  - `handoff` uses accepted handoff ID or source fingerprint;
  - `issue` uses issue source fingerprint;
  - reserve `pr_head` and `ci_head` for future target-aware routes if not yet
    available in current data model.
- Ensure attempts increment only for allowed work jobs.
- Ensure max attempts is checked for the scoped route key.

### Requirement gates

- Existing route attempts migrate to `scope_hash='legacy'`.
- Routes without `attempt_scope` retain legacy behavior.
- Gate-blocked routes do not create/increment scoped attempts.
- A pre-triage blocked bug-fix route does not poison a later post-triage
  bug-fix route scoped to the accepted handoff.
- Re-running the same route for the same handoff/scope respects `max_attempts`.

### Tests

- Store migration test for old route_attempts data.
- Router tests:
  - missing handoff leaves attempts unchanged;
  - accepted handoff increments scoped attempt once;
  - same handoff second run hits max attempts;
  - different handoff produces a different scope hash when configured.

## Phase 5 — Result fallback: `work_started` accounting

### Scope

- Extend result parsing to accept optional `work_started` boolean.
- Treat `work_started: false` as a non-counting gate-like outcome when no
  irreversible work was started.
- Keep default behavior as `work_started: true` for backward compatibility after
  command execution.
- Document wrapper/agent conventions.

### Requirement gates

- Existing result JSON remains valid.
- `work_started: false` can avoid consuming an attempt for late-discovered gate
  conditions such as missing handoff or stale input.
- `work_started: true` always counts as a work attempt.
- Failures after edits, tests, branch creation, or PR publishing still count.

### Tests

- Job finalization tests for `work_started` omitted/true/false.
- Safety test that `work_started:false` cannot bypass transition cap or terminal
  labels if configured block/failure actions apply.

## Phase 6 — Controlled local smoke test

### Scope

Create a deterministic local smoke fixture that does not require real GitHub.
Use fake/local issue data and fixture agents to simulate the gated bug-fix after
triage scenario from the spec.

Scenario:

1. Seed an issue with bug-fix labels and no triage handoff.
2. Run route/dispatch once.
3. Assert gate block, no work job, no route attempt.
4. Add a triage handoff envelope.
5. Run route/dispatch once.
6. Assert bug-fix work job runs exactly once and route attempts equal 1.
7. Re-run same scoped bug-fix route.
8. Assert max attempts stops the second real work attempt and applies configured
   exceeded actions.

### Requirement gates

- Smoke is runnable by `go test ./...` or a documented `go test` target.
- Smoke uses fixture commands only.
- Smoke asserts both sides of the safety contract:
  - preflight blocks are non-counting;
  - repeated real work is capped.

### Tests

- Add the smoke as a component/e2e test with temp SQLite and fake GitHub.
- Include assertions over DB rows (`handoffs`, `route_attempts`, `gate_blocks`,
  `jobs`) and fake GitHub label/comment operations.

## Phase 7 — Live smoke and gleg/glerg production rollout runbook

### Scope

Roll the feature into the long-running gleg/glerg issueq instance only after the
binary supports the new `gate` and `attempt_scope` config fields. Treat this as a
separate deployment step from the core semantics implementation so config changes
can be reviewed, backed out, and verified independently.

Planned rollout steps:

1. Build and verify a new issueq binary from the implementation branch.
2. Back up the live instance SQLite database and current config.
3. Update the live instance config, e.g.
   `/srv/issueq/instances/jakelawllm-gleg/issueq.yaml`, so `bug-fix-pr` requires
   an accepted fresh `bug-triage` handoff before work starts.
4. Keep `bug-fix-pr` `max_attempts: 1` for real fix work unless the operator
   intentionally changes policy.
5. Set `bug-fix-pr` `attempt_scope: handoff` so a premature pre-triage block
   cannot poison the later triage-approved fix attempt.
6. Install the rebuilt binary at the instance's configured binary path, e.g.
   `/srv/issueq/bin/issueq`.
7. Restart the daemon through its service manager, for example:

   ```bash
   sudo systemctl restart <issueq-service>
   ```

   If the instance is not systemd-managed, restart the observed daemon process
   using the instance's normal deployment path.
8. Reconcile any existing poisoned live state from earlier semantics. For gleg
   issue #191 this may include removing stale terminal labels such as
   `agent-failed` / `agent-needs-human` when intentionally re-arming the issue,
   and either clearing the bad `bug-fix-pr` attempt row or relying on the new
   scoped-attempt key to make it irrelevant.
9. Tail logs and verify the gated smoke path against the live or scratch issue.

### Requirement gates

- New config is not applied until the deployed binary can parse and enforce it.
- Live SQLite and config are backed up before migration or state reconciliation.
- `issueq config-check` or equivalent validation passes against the updated live
  config before daemon restart.
- The daemon restarts cleanly and reports no config/migration errors.
- The live smoke confirms:
  - pre-triage `bug-fix-pr` blocks without incrementing work attempts;
  - `bug-triage` emits/stores the accepted handoff;
  - post-triage `bug-fix-pr` runs once;
  - repeating the same scoped real work hits `max_attempts`.

### Rollback

- Stop or restart the daemon with the previous binary.
- Restore the previous `issueq.yaml`.
- Restore the SQLite backup if migrations or state reconciliation need to be
  reverted.
- Remove any temporary scratch labels/comments used for the smoke test.

## Phase 8 — Operator docs and config examples

### Scope

- Update `examples/issueq.yaml` with a minimal gated route example.
- Update operator docs with migration/rollout guidance.
- Provide instructions for repairing poisoned counters safely after upgrade.

### Requirement gates

- Existing gleg-style route config can express:
  - report-only triage;
  - write route gated by triage handoff and write approval;
  - PR review capped to 2-3 scoped attempts;
  - CI fix capped to 1-2 scoped attempts;
  - workflow transition cap preserved.

### Testing strategy

- `config-check` against example and gleg-style fixture config.
- Manual review of route labels to ensure terminal labels still block routes.
- Optional dry-run route evaluation command if available by then.

## Open questions

1. Should gate blocks increment `transition_count` once, or never? Initial
   recommendation: do not count duplicate gate blocks; count only if block
   actions materially change labels.
2. Should `attempt_scope` accept arbitrary templates later? Initial
   recommendation: start with enumerated scopes for simplicity.
3. Should handoff ingestion happen during poll, route, or both? Initial
   recommendation: poll parses comments into the handoff index; route can also
   parse current snapshot defensively.
4. How should old poisoned counters be repaired? Initial recommendation: add an
   operator command later; until then document safe SQLite backup and targeted
   repair.
