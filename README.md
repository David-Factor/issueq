# issueq

A small Go + SQLite GitHub issue queue runner.

`issueq` polls GitHub issues, routes matching labels/predicates into a local queue, and dispatches bounded subprocess jobs such as triage, coding agents, review agents, and cleanup tasks.

See [`docs/v1-spec.md`](docs/v1-spec.md) for the current design and [`docs/v1-implementation-plan.md`](docs/v1-implementation-plan.md) for the phased build plan.
