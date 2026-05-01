# issueq

A small Go + SQLite GitHub issue queue runner.

`issueq` polls GitHub issues, routes matching labels/predicates into a local queue, and dispatches bounded subprocess jobs such as triage, coding agents, review agents, and cleanup tasks.

See [`docs/v1-spec.md`](docs/v1-spec.md) for the current design and [`docs/v1-implementation-plan.md`](docs/v1-implementation-plan.md) for the phased build plan.

## Current status

The v1 local runner is implemented through the H5/H6 wrapper-only execution-supervisor cleanup. `daemon`, `once`, and `dispatch` route jobs through SQLite durable running state and the direct `issueq job-wrapper` supervisor; the pre-H5 attached/direct runner is not a supported runtime fallback.

`issueq once --no-wait` remains intentionally unsupported until durable detached CLI semantics are explicitly designed. `dispatch --local-no-github` is only for local fixtures; normal dispatch is GitHub-aware.

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
