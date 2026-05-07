# IssueQ event operator runbook

## Validate configuration

```sh
issueq --config /srv/issueq/instances/<instance>/issueq.yaml config-check
```

## Start event daemon

```sh
systemctl start issueq@<instance>.service
systemctl status issueq@<instance>.service
```

The service runs `issueq ... daemon`. Event mode is the only daemon mode in this
hard-cutover build.

## Ingest events locally

Use the trusted local reconciler/timer to emit `issueq-event/v1` JSON and upsert
via the local CLI:

```sh
reconciler-command | while IFS= read -r event; do
  [ -n "$event" ] && printf '%s\n' "$event" | issueq --config issueq.yaml event upsert --json -
done
```

## Inspect events

```sh
issueq --config issueq.yaml events list
issueq --config issueq.yaml events show <event-key>
```

Statuses are `ready`, `running`, `blocked`, `succeeded`, `failed`, `stale`,
`needs_human`, and `cancelled`.

## Manual actions

```sh
issueq --config issueq.yaml events retry <event-key>
issueq --config issueq.yaml events cancel <event-key>
issueq --config issueq.yaml events approve <event-key> --decision <decision> --next-kind <kind>
```

Approval is policy limited. It is not represented by labels or comments.

## Projection

```sh
issueq --config issueq.yaml project <event-key>
```

Projection writes managed comments and optional UI-only status labels. These are
not scheduler inputs.
