# Roadmap

issueq is currently public-preview software. This roadmap is directional, not a compatibility promise.

## Current focus

issueq's current implementation focuses on controlled GitHub Issues automation:

- poll one GitHub repository for issues;
- route issues by labels and simple predicates;
- record issue and job state in SQLite;
- run local subprocess jobs through durable wrapper-only supervision;
- capture context, result JSON, stdout, stderr, and wrapper metadata;
- update GitHub issue labels/comments on start, success, failure, and retry exhaustion;
- provide operator docs for systemd, backups, inspection, and recovery.

## Near term

Near-term work should make the existing issue-label runner easier and safer to operate:

- release tags and binary build artifacts;
- more complete production examples;
- better CLI inspection and recovery commands for jobs, events, and artifacts;
- clearer retry/dead-letter/manual-resolution workflows;
- metrics and health reporting;
- stronger smoke tests around upgrades, daemon restarts, and GitHub API edge cases;
- GitHub Actions bridge examples for PR/CI events that create or update issueq-routing issues; see `docs/event-bridge-routing-plan.md`.

## Broader GitHub event support

The intended direction is to evolve beyond issue polling into a broader GitHub event router while preserving the local-supervised execution model.

Candidate event sources:

- `issues` changes such as opened, labeled, assigned, closed, and reopened;
- `issue_comment`, including slash-command style triggers;
- `pull_request` events such as opened, synchronize, reopened, ready-for-review, and labeled;
- `pull_request_review` and review comments;
- `push` events;
- release/tag events;
- manual dispatch triggers;
- scheduled triggers.

PR and fork events need an explicit trust model before they can safely run jobs. Public fork code should not receive write tokens or host secrets unless an operator adds an external sandbox and approval gate.

## Workflow files

A future issueq workflow format may be inspired by GitHub Actions YAML without being drop-in compatible with GitHub Actions.

Possible shape:

```yaml
name: PR Review

on:
  pull_request:
    types: [opened, synchronize, reopened]
  issue_comment:
    commands: ["/issueq review"]

permissions:
  contents: read
  issues: write
  pull_requests: write

jobs:
  review:
    if: github.event.pull_request.draft == false
    steps:
      - run: ./agents/review-pr.sh
        timeout: 30m
```

Useful concepts to borrow:

- `on` event declarations;
- event type and label/comment filters;
- `jobs` and `steps`;
- `run`, `env`, `permissions`, and `timeout`;
- concurrency/cancellation semantics;
- manual dispatch.

Concepts to avoid or defer initially:

- full GitHub Actions expression compatibility;
- marketplace `uses:` compatibility;
- matrix builds;
- service containers;
- secret inheritance behavior;
- pretending local issueq jobs have the same isolation properties as GitHub-hosted runners.

A likely split is:

```text
issueq.yaml              # daemon/runtime instance config
.issueq/workflows/*.yml  # repository automation behavior
```

## Longer term

Possible longer-term directions:

- multi-repository or organization-level instances;
- pluggable event sources beyond GitHub;
- pluggable supervisors or sandbox backends;
- richer policy controls for trusted vs untrusted work;
- dashboards for queue state, latency, failures, and artifact links;
- GitHub Checks/status reporting for PR workflows;
- packaged deployment examples for common Linux distributions.

## Non-goals for now

- replacing GitHub Actions generally;
- executing arbitrary untrusted workflows safely without additional sandboxing;
- implementing every GitHub webhook event immediately;
- hiding the fact that jobs run on operator-controlled infrastructure;
- supporting production use without backups, monitoring, and a least-privilege token model.
