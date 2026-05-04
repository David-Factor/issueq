# issueq issue event bridge action

Reusable GitHub Action for translating non-issue GitHub events into normal
GitHub issues that issueq can route by label.

The first supported event is `ci-failure` from a `workflow_run` trigger. The
action creates or updates one open routing issue per pull request/workflow pair,
using a hidden marker in the issue body for idempotency.

## Example

```yaml
name: issueq CI bridge

on:
  workflow_run:
    workflows: ["CI"]
    types: [completed]

permissions:
  actions: read
  contents: read
  issues: write
  pull-requests: read

jobs:
  bridge:
    if: ${{ github.event.workflow_run.conclusion == 'failure' }}
    runs-on: ubuntu-latest
    steps:
      - uses: David-Factor/issueq/actions/issue-event-bridge@main
        with:
          event-kind: ci-failure
          routing-labels: agent-ci-diagnose
          ready-label: agent-ready
          dry-run: "true"
```

`dry-run: "true"` creates or updates the bridge issue but does not apply the
ready label. Flip it to `false` only after the created issue body, marker, and
labels are confirmed for the target repository.

## Idempotency

The action writes a marker like:

```markdown
<!-- issueq-bridge:ci-failure:pr-123:workflow-ci -->
```

Before creating a new issue, it searches open issues for the marker and updates
the existing issue if found.

## Loop control

The issue body includes an `Attempt` field. When `dry-run: "false"`, the ready
label is applied only while the attempt count is at or below `max-attempts` and
the PR branch does not match a generated branch prefix. After the attempt budget
is exhausted, human-review labels are applied instead.
