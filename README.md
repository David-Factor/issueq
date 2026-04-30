# issueq

A small Go + SQLite GitHub issue queue runner.

`issueq` polls GitHub issues, routes matching labels/predicates into a local queue, and dispatches bounded subprocess jobs such as triage, coding agents, review agents, and cleanup tasks.

See [`docs/v1-spec.md`](docs/v1-spec.md) for the current design and [`docs/v1-implementation-plan.md`](docs/v1-implementation-plan.md) for the phased build plan.

## Current status

Phase 0 skeleton is in place. The CLI builds and exposes the planned v1 commands, but runtime config, GitHub, SQLite, routing, and dispatch behavior are implemented in later phases.

## Development commands

```bash
go run ./cmd/issueq --help
go test ./...
go vet ./...
gofmt -w ./cmd ./internal
git diff --check
```

Or use Make:

```bash
make check
```

Default config path for all commands is `./issueq.yaml`; override with `--config <path>`.
