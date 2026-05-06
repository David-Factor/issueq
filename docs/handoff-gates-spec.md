# First-Class Handoffs and Route Gates Spec

## 1. Problem

issueq currently treats a matched route and a real work attempt as the same
thing. If an issue has labels for a write-capable route but the route is missing
a prerequisite, issueq can still consume the route attempt budget before any real
work starts.

Observed example:

1. An issue was labeled for `bug-fix-pr` before a `bug-triage` handoff existed.
2. The bug-fix route ran only far enough to report "missing prior triage
   handoff".
3. That non-work gate failure consumed a `bug-fix-pr` attempt.
4. After report-only triage succeeded and recommended `bug-fix-pr`, the actual
   writable route reused the same route counter and was stopped as max attempts
   exceeded before it could produce a draft PR.

This is not just a one-off job issue. It is a counter semantics issue: attempts
are currently keyed by issue/generation/route, while the system has no explicit
concept of a non-counting preflight/gate outcome or of the handoff that makes a
later write route materially different from an earlier premature route match.

## 2. User goals

The desired workflow must support both safety and forward progress:

1. Prevent infinite automation loops.
   - Avoid cycles such as fix -> CI fail -> CI fix -> CI pass -> review -> PR
     fix -> CI fail -> repeat forever.
2. Limit repeated reviews.
   - If review continues to produce findings after two or three passes, stop and
     ask for a human. Repeated findings usually indicate an incomplete review,
     fix-induced regressions, or a deeper design problem.
3. Do not poison real work attempts with non-work precondition failures.
   - Missing handoff, missing approval, stale target, or route mismatch should
     not consume the attempt budget for a route that never actually started
     work.
4. Keep issueq small and workflow-agnostic.
   - issueq should not know what bug triage, CI diagnosis, PR review, or bug fix
     mean semantically beyond route names, decisions, source/target metadata, and
     configured predicates.
5. Keep GitHub issues/comments human-visible as the durable workflow surface,
   while SQLite remains the local queue, index, counter store, and audit trail.

## 3. Design summary

Add two first-class concepts:

1. **Handoff envelope**: An opaque, indexed record produced by one route and
   consumed by a later route gate.
2. **Route gate phase**: A pre-work phase that validates configured prerequisites
   before a work job consumes route attempts.

Route execution becomes:

```text
poll issue snapshots
  -> route labels match
  -> evaluate route gate
       -> blocked: apply configured block actions, record gate block, no work attempt
       -> allowed: enqueue/run work job, consume work attempt
  -> observe/finalize job
  -> apply success/failure/attempt-exceeded actions
```

This is deliberately not a full workflow engine. Labels still select routes.
Routes still run configured commands. Handoffs and gates only make prerequisites,
freshness, and attempt accounting explicit.

## 4. Definitions

- **Handoff**: A structured envelope emitted by a route result that describes the
  route outcome, source freshness fingerprint, optional next route, and target.
- **Gate**: Declarative route precondition evaluated before work starts.
- **Gate block**: A non-work route outcome where the gate failed. It may add
  labels/comments, but it must not consume the route's work attempt budget.
- **Work attempt**: A route execution that actually starts the configured command
  or otherwise begins agent work. Work attempts are capped.
- **Transition**: A meaningful automation state change or successful route handoff
  between stages. Transitions are capped globally to prevent loops.
- **Scope hash**: Stable identifier for the semantic input being attempted, such
  as latest handoff fingerprint, PR head SHA, CI run/head SHA, or issue source
  fingerprint.

## 5. Handoff storage model

Store handoffs in SQLite as opaque, indexed envelopes. issueq should index only
generic fields needed for gates and debugging. It should preserve the full JSON
payload unchanged.

Suggested table:

```sql
CREATE TABLE handoffs (
  id TEXT PRIMARY KEY,
  issue_key TEXT NOT NULL,
  route_name TEXT NOT NULL,
  decision TEXT NOT NULL,
  next_route TEXT,
  source_kind TEXT,
  source_key TEXT,
  source_fingerprint TEXT,
  target_kind TEXT,
  target_key TEXT,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX handoffs_issue_route_idx
  ON handoffs(issue_key, route_name, created_at);

CREATE INDEX handoffs_issue_next_route_idx
  ON handoffs(issue_key, next_route, created_at);

CREATE INDEX handoffs_target_idx
  ON handoffs(target_kind, target_key, created_at);
```

### 5.1 Handoff envelope format

Existing `issueq-handoff/v1` comment payloads are close to the desired envelope.
The durable handoff parser should accept envelopes with at least:

```json
{
  "schema": "issueq-handoff/v1",
  "schema_version": "1",
  "route": "agent-route-bug-triage",
  "decision": "bug_fix_candidate",
  "next_route": "agent-route-bug-fix-pr",
  "source": {
    "kind": "github_issue",
    "issue_number": 191,
    "updated_at": "2026-05-06T04:51:53Z",
    "body_sha256": "..."
  },
  "target": {
    "kind": "bug_issue",
    "issue_number": 191
  }
}
```

`payload_json` must preserve the full payload so route-specific agents or future
versions can inspect richer data without issueq schema changes.

### 5.2 Opinion boundary

issueq may understand these generic fields:

- route name;
- decision string;
- next route string;
- source kind/key/fingerprint;
- target kind/key;
- payload JSON;
- creation time.

issueq must not hardcode meanings such as `bug_fix_candidate`, `diagnosed`, or
`draft_pr_opened`. Config may match those strings.

## 6. Route gate configuration

Add an optional `gate` block to routes. Minimal shape:

```yaml
routes:
  - name: bug-fix-pr
    when:
      labels_include: [agent-ready, agent-route-bug-fix-pr, agent-write-approved]
      labels_exclude: [agent-running, agent-done, agent-failed, agent-needs-human, manual-only]

    gate:
      handoff:
        required: true
        from: [bug-triage]
        decisions: [bug_fix_candidate, reproducible, straightforward]
        freshness: source_unchanged
      on_block:
        labels_remove: [agent-ready, agent-running]
        labels_add: [agent-needs-human]
        comment: "issueq route blocked: {{ gate.reason }}"

    job:
      kind: bug-fix-pr
      command: ["./agents/agent.sh", "bug-fix-pr"]
      timeout: 40m
      max_attempts: 1
      attempt_scope: handoff
```

`gate` should be optional. Existing routes without gates keep current behavior.

### 6.1 Handoff gate fields

- `required`: if true, block when no matching handoff exists.
- `from`: route names accepted as producers. These are config route names, not
  label names.
- `decisions`: optional allowlist. If empty, any decision is accepted.
- `next_route`: optional boolean or value check. If enabled, the handoff must
  recommend the current route.
- `freshness`: optional freshness policy:
  - `none`: do not check source freshness.
  - `source_unchanged`: source fingerprint still matches current issue/PR source.
  - `target_head_unchanged`: PR/CI target head still matches handoff target.

### 6.2 Block behavior

Gate blocks should be visible but not noisy.

- A gate block may apply labels/comments using an `on_block` action block.
- Gate block actions should be deduped by issue, generation, route, reason, and
  scope hash to avoid repeated identical comments.
- Gate blocks do not increment route work attempts.
- Gate blocks should not count as successful route transitions. They may count
  toward a separate block counter for diagnostics.

Suggested table:

```sql
CREATE TABLE gate_blocks (
  issue_key TEXT NOT NULL,
  generation INTEGER NOT NULL,
  route_name TEXT NOT NULL,
  reason TEXT NOT NULL,
  scope_hash TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (issue_key, generation, route_name, reason, scope_hash)
);
```

## 7. Attempt accounting

Split counters into three concepts.

### 7.1 Work attempts

Work attempts count only after a gate passes and the work job is actually queued
or started.

Update route attempts to include semantic scope:

```sql
CREATE TABLE route_attempts (
  issue_key TEXT NOT NULL,
  generation INTEGER NOT NULL,
  route_name TEXT NOT NULL,
  scope_hash TEXT NOT NULL DEFAULT 'legacy',
  attempts INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (issue_key, generation, route_name, scope_hash)
);
```

`scope_hash` should be derived generically. Recommended initial values:

- `handoff`: latest accepted handoff ID or source fingerprint.
- `issue`: issue key plus current issue source fingerprint.
- `pr_head`: PR key plus head SHA.
- `ci_head`: CI run ID/head SHA when available.
- `legacy`: default for routes without an explicit scope.

### 7.2 Workflow transitions

Keep a workflow-level transition budget to stop broad loops. This can continue
using `issue_state.transition_count`, but the spec should clarify what counts:

Count:

- work job starts;
- successful route output applies a next route/handoff;
- bridge-created route issues that advance an automation workflow;
- automatic relabeling from one real route stage to another.

Do not count by default:

- repeated no-op polls;
- duplicate deduped gate blocks;
- stale preflight blocks that do not start work.

When exceeded, apply existing configured terminal actions.

### 7.3 Gate block counters

Gate block counters are for diagnostics and duplicate suppression. They do not
replace route attempts or transition counts.

## 8. Result/postflight conventions

Longer term, agents should be able to explicitly report whether work started:

```json
{
  "decision": "needs_human",
  "work_started": false,
  "reason": "missing_handoff"
}
```

or:

```json
{
  "decision": "draft_pr_opened",
  "work_started": true
}
```

The gate phase should prevent common non-work outcomes before job launch. The
`work_started` field is still useful as a fallback for wrappers that discover a
non-counting gate condition after launch.

Policy:

- `work_started: false` may be treated as a gate block if no irreversible work
  occurred.
- `work_started: true` always counts as a work attempt.
- Missing `work_started` defaults to true for backward compatibility after the
  command has run.

## 9. Loop-control examples

### 9.1 Bug fix route after triage

```text
bug-fix-pr labels present before triage handoff
  -> gate blocks missing_handoff
  -> no bug-fix-pr work attempt consumed

bug-triage runs and emits fresh handoff
  -> bug-fix-pr labels present
  -> gate allows
  -> bug-fix-pr work attempt consumed
```

### 9.2 CI/review loop

```text
bug-fix-pr -> CI fail -> ci-diagnose -> ci-fix -> CI pass -> pr-review
  -> pr-fix -> CI pass -> pr-review -> findings again -> pr-fix
  -> CI fail -> ci-fix cap exceeded or transition cap exceeded -> needs human
```

The route caps and transition cap stop infinite loops while allowing one or two
reasonable correction cycles.

## 10. Live smoke scenario requirement

Add a controlled live smoke scenario that runs against a scratch GitHub issue in
a test repository or a controlled issueq instance. The scenario must demonstrate
that gate blocks do not poison later work attempts, while real repeated work is
still capped.

Scenario: **gated bug-fix after triage**

1. Create or select a scratch issue with labels:
   - `agent-ready`
   - `agent-route-bug-fix-pr`
   - `agent-write-approved`
2. Ensure no `bug-triage` handoff exists.
3. Run `issueq once`.
4. Assert:
   - issue receives a gate block comment or configured block labels;
   - no `bug-fix-pr` work attempt is consumed;
   - no work job ran.
5. Add or trigger a `bug-triage` handoff for the same issue.
6. Restore labels for `bug-fix-pr` if needed.
7. Run `issueq once`.
8. Assert:
   - `bug-fix-pr` work job runs exactly once;
   - route attempt count for `bug-fix-pr` is 1, not 2;
   - result applies configured success labels/comment.
9. Trigger the same `bug-fix-pr` route again for the same handoff/scope.
10. Assert max attempts stops it and applies human/failure state.

This smoke should be documented as a manual/live test first, then automated with
fake GitHub if practical.

## 11. Compatibility and migration

- Existing configs remain valid because `gate` and `attempt_scope` are optional.
- Existing `route_attempts` rows migrate to `scope_hash = 'legacy'`.
- Existing issue comments containing `issueq-handoff/v1` should be parsed into
  the `handoffs` table on poll or route. Re-parsing must be idempotent.
- If the handoff table is empty, routes without handoff gates behave as they do
  today.

## 12. Acceptance criteria

The implementation is acceptable when:

1. Existing v1 route behavior still passes tests without gate config.
2. Handoff envelopes are parsed and stored idempotently in SQLite.
3. A route gate can require a fresh handoff from configured producer routes.
4. Gate blocks apply configured labels/comments without consuming work attempts.
5. Work attempts are scoped and capped using `scope_hash`.
6. The existing workflow transition cap still stops broad automation loops.
7. The controlled smoke scenario demonstrates pre-triage bug-fix route blocking
   without poisoning post-triage bug-fix execution.
