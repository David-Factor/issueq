# Event bridge routing plan

This note captures a future routing model for using issueq with GitHub events that are not native GitHub issues, such as pull request review requests and CI failures.

The intended implementation point is after the Phase 2 agent workflow is stable:

1. isolated per-job worktrees
2. skill-driven agent routes
3. draft PR creation
4. optional live app verification

## Core model

Keep issueq issue-driven. Use GitHub Actions as the event bridge:

```text
GitHub event
  -> GitHub Action creates or updates a routing issue
  -> Action applies issueq labels
  -> issueq polls/issues route as usual
  -> local agent workflow runs
  -> issueq comments/labels the routing issue
```

This keeps issueq's core simple: labels on issues are the routing API. GitHub Actions owns event-specific translation for PRs, workflow runs, check suites, and similar events.

## CI failure bridge

When CI fails on a pull request, a GitHub Action should create or update a routing issue.

Suggested title:

```text
CI failure: PR #123
```

Suggested labels:

```text
agent-ready
agent-ci-fix
```

Suggested body:

```markdown
<!-- issueq-bridge:ci-failure:pr-123:workflow-ci -->

PR: https://github.com/OWNER/REPO/pull/123
Head branch: feature/foo
Head SHA: abc123
Workflow: CI
Run: https://github.com/OWNER/REPO/actions/runs/...
Backing issue: #42
Attempt: 1

Failure summary:
- bun test failed in packages/foo/bar.test.ts
- exit code 1

Agent instructions:
Please inspect the failing CI run and make or propose a minimal fix.
```

The routed agent can then inspect logs, prepare a worktree for the PR branch, make a minimal fix, and either push to the PR branch or open a follow-up draft PR depending on route policy.

## PR review bridge

When a pull request is opened, synchronized, or reaches passing CI, a GitHub Action can create or update a review routing issue.

Suggested title:

```text
Agent review: PR #123
```

Suggested labels:

```text
agent-ready
agent-pr-review
```

Suggested body:

```markdown
<!-- issueq-bridge:pr-review:pr-123 -->

PR: https://github.com/OWNER/REPO/pull/123
Base: main @ abc123
Head: feature/foo @ def456
Backing issue: #42
CI status: passed
Attempt: 1

Agent instructions:
Please review this PR for correctness, maintainability, tests, and project conventions.
```

The review route can run report-only, post review findings, or open follow-up issues depending on repository policy.

## Linking backing issues

The bridge Action should try to infer the original issue from:

- PR body keywords such as `Fixes #42`, `Closes #42`, or `Related to #42`
- branch names such as `issue-42` or `issueq/issue-42/...`
- PR title/body metadata
- future project-specific metadata if available

Record the backing issue in the routing issue body. Avoid creating a label per backing issue; labels should stay low-cardinality and route-oriented.

## Idempotency

Bridge Actions must be idempotent. They should create at most one open routing issue per logical event.

Use hidden markers in issue bodies, for example:

```markdown
<!-- issueq-bridge:ci-failure:pr-123:workflow-ci -->
<!-- issueq-bridge:pr-review:pr-123 -->
```

Before creating a routing issue, search existing open issues for the marker. If one exists, update its body/comments/labels instead of creating a duplicate.

## Loop control

Agent workflows can create feedback loops:

```text
CI fails
  -> bridge creates CI-fix issue
  -> agent opens/pushes fix PR
  -> CI fails again
  -> bridge creates/updates CI-fix issue again
```

A small number of automated iterations is useful. Beyond that, repeated failure usually means a human should intervene.

Recommended policy:

- Track an `Attempt` count in the routing issue body, or in a hidden marker/comment block.
- Allow at most two automated attempts for the same logical event by default.
- On attempt 3, do not add `agent-ready`; instead add human-review labels.

Suggested labels when the loop budget is exhausted:

```text
agent-needs-human
manual-only
```

Suggested comment:

```text
issueq bridge stopped auto-routing this event after 2 attempts. Human review needed.
```

Apply the same loop-budget idea to PR review findings. For example, if an agent review creates changes, and a follow-up review still finds issues after two cycles, stop routing and request human review.

## Avoiding self-trigger loops

Bridge workflows should identify agent-generated branches and PRs. Depending on policy, they can skip or limit automatic review/fix routing for those branches.

Examples of branch prefixes to handle carefully:

```text
issueq/
agent/
codex/
```

Possible strategies:

- skip PR review bridge for generated PRs until CI passes
- allow exactly one agent review for generated PRs
- require a human-applied label before routing generated PRs again
- label generated PRs with `agent-generated`

## Suggested route taxonomy

Report-only routes:

```text
agent-triage
agent-pr-review
agent-test-audit
```

Local-change routes:

```text
agent-code-local
agent-docs-local
```

Draft-PR routes:

```text
agent-docs-pr
agent-small-fix-pr
agent-ci-fix
```

Live verification routes:

```text
agent-live-smoke
agent-live-fix-pr
```

## Phase 2C instance validation

The `jakelawllm-gleg` validation instance proved a skill-driven draft-PR workflow after the local worktree smoke.

The route used:

- labels: `agent-ready` + `issueq-pr-smoke`
- script: `/srv/issueq/instances/jakelawllm-gleg/agents/agent-pr-smoke.sh`
- skill: `/srv/issueq/instances/jakelawllm-gleg/agents/skills/pr-smoke.md`
- agent: `codex exec --sandbox workspace-write`

The final design kept the agent focused on editing and cheap verification. The wrapper prepared the worktree, committed the final diff, pushed the branch, opened the draft PR, and wrote issueq result JSON.

The first attempt failed safely because the sandboxed agent could edit the linked worktree but could not push to the exe.dev Git integration host. The successful retry used wrapper-owned publish steps.

Successful validation:

- Issue: https://github.com/jakelawllm/gleg/issues/222
- Job: `job_01KQRMJ465V0CN4YFNHV5HKTM5`
- Worktree: `/srv/issueq/instances/jakelawllm-gleg/repos/worktrees/issue-222-job-01KQRMJ465V0`
- Branch: `issueq/issue-222/job-01KQRMJ465V0`
- Commit: `430971c7b50de80a6bef55067e8227a28bc68be3`
- Draft PR: https://github.com/jakelawllm/gleg/pull/223

The human checkout stayed isolated from automation. Future PR-producing examples can use either boundary depending on trust level: Codex `workspace-write` with wrapper-owned publishing for stricter containment, or Codex `danger-full-access` with agent-owned commit/push/PR on controlled private repos and secretless VMs.

Follow-up full-access validation:

- Issue: https://github.com/jakelawllm/gleg/issues/224
- Job: `job_01KQRPARTGZD6WG1EB01WKF92Q`
- Worktree: `/srv/issueq/instances/jakelawllm-gleg/repos/worktrees/issue-224-job-01KQRPARTGZD`
- Branch: `issueq/issue-224/job-01KQRPARTGZD`
- Commit: `b609a64013ff32ae4c3a98fb824633eb8d33f89d`
- Draft PR: https://github.com/jakelawllm/gleg/pull/225

The full-access route matched the desired agent-owned flow: the agent edited, verified, committed, pushed, and opened the draft PR; the wrapper verified/reporting only.


When implementing this, start with one bridge workflow at a time:

1. CI failure bridge creates/updates issues but does not route automatically.
2. Add `agent-ready` only after dry-run issue creation looks correct.
3. Enable one low-risk route such as report-only CI diagnosis.
4. Then enable code-changing CI fixes with draft PRs.

Keep GitHub event complexity in Actions and keep issueq focused on issue-label routing.
