# issueq

A small Go + SQLite GitHub issue queue runner.

`issueq` polls GitHub issues, routes matching labels/predicates into a local queue, and dispatches bounded subprocess jobs such as triage, coding agents, review agents, and cleanup tasks.

[![CI](https://github.com/David-Factor/IssueQ/actions/workflows/ci.yml/badge.svg)](https://github.com/David-Factor/IssueQ/actions/workflows/ci.yml)

## Current status

IssueQ is early public-preview software. The current runtime is suitable for controlled, trusted automation where you operate the host, token, labels, and configured commands. It is not a sandbox for arbitrary untrusted code.

The v1 local runner is implemented through the H5/H6 wrapper-only execution-supervisor cleanup. `daemon`, `once`, and `dispatch` route jobs through SQLite durable running state and the direct `issueq job-wrapper` supervisor; the pre-H5 attached/direct runner is not a supported runtime fallback.

`issueq once --no-wait` remains intentionally unsupported until durable detached CLI semantics are explicitly designed. `dispatch --local-no-github` is only for local fixtures; normal dispatch is GitHub-aware.

## Documentation

- [v1 spec](docs/v1-spec.md)
- [v1 implementation plan](docs/v1-implementation-plan.md)
- [Operator runbook](docs/operator-runbook.md)
- [Deployment checklist](docs/deployment-checklist.md)
- [systemd template](deploy/systemd/issueq@.service)

## Trust and security notes

IssueQ executes configured local commands on the daemon host. If those commands check out or run repository content, that content executes with the permissions of the service user and with whatever secrets you pass through the job environment.

Recommended baseline:

- use a dedicated `issueq` service user;
- use least-privilege, repository-scoped GitHub tokens;
- keep tokens in instance env files or a secret manager, never in Git;
- keep SQLite DBs, workdirs, logs, and env files out of the repository;
- do not run public fork/PR code unless you add an external sandbox and remove write tokens/secrets;
- back up SQLite before manual repair or upgrades.

See [SECURITY.md](SECURITY.md) for vulnerability reporting and [docs/operator-runbook.md](docs/operator-runbook.md) for operational recovery guidance.

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
When config is loaded from a file, relative `queue.sqlite.path`, `workdir.path`, and explicit relative job executables (`job.command[0]` beginning with `./` or `../`) are resolved relative to the config file's directory. Bare commands such as `bash` or `python3` are still resolved via `PATH`.
