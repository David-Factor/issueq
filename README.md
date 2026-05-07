# issueq

`issueq` is a local durable runner for `issueq-event/v1` automation events.
This branch is the event hard-cutover build: synthetic bridge issues,
`agent-route-*` label scheduling, legacy handoff comments, and the old
`poll`/`route`/`dispatch` job pipeline are removed from callable surfaces.

GitHub issues, PR comments, and labels are projections for people only. They do
not schedule work.

## Production scheduler

Run the event daemon:

```sh
issueq --config ./issueq.yaml daemon
```

Run one claim/execute/finalize cycle:

```sh
issueq --config ./issueq.yaml once
```

There is no `--mode legacy` switch in this build.

## Event ingestion and operations

Create or upsert deterministic events:

```sh
issueq --config ./issueq.yaml event upsert --json event.json
cat event.json | issueq --config ./issueq.yaml event create --json -
```

Inspect and operate events:

```sh
issueq --config ./issueq.yaml events list
issueq --config ./issueq.yaml events show <event-key>
issueq --config ./issueq.yaml events retry <event-key>
issueq --config ./issueq.yaml events cancel <event-key>
issueq --config ./issueq.yaml events approve <event-key> --decision <decision> --next-kind <kind>
```

Approval stores a trusted event handoff and creates only a follow-up allowed by
route policy.

## Projection

Project event state to GitHub managed comments and optional UI-only labels:

```sh
issueq --config ./issueq.yaml project <event-key>
```

Projection failure must be handled separately; it does not rerun route work or
satisfy dependencies.

## Configuration check

```sh
issueq --config ./issueq.yaml config-check
```

Routes must have `event_kind`. Legacy label predicates/actions such as
`when.labels_include`, `when.labels_exclude`, `labels_add`, and `labels_remove`
are rejected by schema/validation.
