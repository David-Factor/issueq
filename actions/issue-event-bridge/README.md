# issueq issue event bridge action

Reusable GitHub Action for translating GitHub event context into normal GitHub
issues that issueq can route by label.

The Action is intentionally template-driven. It does not own a growing enum of
project-specific event types. Callers provide the idempotency marker, issue
title, body, labels, and optional JSON context. If no `context-json` is supplied,
the Action provides a small convenience context for `workflow_run` events.

## Example: CI failure bridge

```yaml
name: issueq CI bridge

on:
  workflow_run:
    workflows: ["CI", "Integration CI"]
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
          routing-labels: agent-route-ci-diagnose
          apply-ready: "false"
          generated-branch: "{{ pr.head_branch }}"
          marker: "<!-- issueq-bridge:ci-failure:pr-{{ pr.number|slug }}:workflow-{{ workflow.name|slug }} -->"
          title: "CI failure: PR #{{ pr.number }}"
          body: |
            <!-- issueq-bridge:ci-failure:pr-{{ pr.number|slug }}:workflow-{{ workflow.name|slug }} -->

            PR: {{ pr.url }}
            Base branch: {{ pr.base_branch }}
            Head branch: {{ pr.head_branch }}
            Head SHA: {{ pr.head_sha }}
            Workflow: {{ workflow.name }}
            Run: {{ workflow.run_url }}
            Backing issue: {{ pr.backing_issue }}
            Bridge event: ci-failure

            Failure summary:
            - Workflow run concluded with {{ workflow.conclusion }}.
            - Inspect the linked run for failing jobs and logs.

            Agent instructions:
            Please inspect the failing CI run and produce a concise diagnosis.
```

`apply-ready: "false"` creates or updates the bridge issue but does not apply the
ready label. Flip it to `true` only after the created issue body, marker, and
labels are confirmed for the target repository.

## Templates

Inputs `marker`, `title`, `body`, and `generated-branch` support placeholders:

```text
{{ pr.number }}
{{ workflow.name|slug }}
```

The only transform is `slug`, intended for stable marker fragments.

With `workflow_run` payloads and no explicit `context-json`, the Action exposes:

```text
repository
workflow.name
workflow.conclusion
workflow.run_url
workflow.run_id
pr.number
pr.url
pr.title
pr.base_branch
pr.head_branch
pr.head_sha
pr.backing_issue
```

For other events, build and pass a `context-json` object from a previous workflow
step, then reference its fields in the templates.

## Idempotency

The rendered `marker` is searched in open issue bodies before create. If found,
the existing issue is patched instead of creating a duplicate.

## Loop control

The issue body includes an `Attempt` field. When `apply-ready: "true"`, the ready
label is applied only while the attempt count is at or below `max-attempts` and
`generated-branch` does not match a generated branch prefix. After the attempt
budget is exhausted, human-review labels are applied instead.
