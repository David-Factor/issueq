# issueq

[![CI](https://github.com/David-Factor/IssueQ/actions/workflows/ci.yml/badge.svg)](https://github.com/David-Factor/IssueQ/actions/workflows/ci.yml)

IssueQ is a small Go + SQLite runner for local GitHub issue automation.

It polls GitHub issues, matches labels and predicates, records queue state in SQLite, runs configured local subprocess jobs, and reports results back to GitHub with labels/comments. Typical jobs include triage agents, coding agents, review agents, and cleanup tasks.

## Status: public preview

IssueQ is early public-preview software.

Current scope:

- GitHub Issues polling and label-based routing.
- Local SQLite queue/state storage.
- Durable wrapper-only subprocess supervision via `issueq job-wrapper`.
- GitHub issue label/comment updates after job completion.

Not current scope:

- not a GitHub Actions replacement;
- not a sandbox for untrusted code;
- not yet a general GitHub event router for PRs, pushes, releases, etc.;
- no supported fallback to the old attached/direct runner.

`issueq once --no-wait` remains intentionally unsupported until durable detached CLI semantics are explicitly designed. `dispatch --local-no-github` is only for local fixtures; normal dispatch is GitHub-aware.

## How it works

```text
GitHub issues
  -> issueq poll
  -> local SQLite issue snapshots
  -> route matching labels/predicates into jobs
  -> dispatch claims jobs within concurrency limits
  -> job-wrapper runs configured command
  -> result JSON / stdout / stderr are captured
  -> GitHub labels/comments are updated
```

Config paths are resolved relative to the config file when loaded from disk. That means an instance directory can contain `issueq.yaml`, `issueq.db`, `work/`, and `agents/` without depending on the daemon's current working directory.

## Quick start

Prerequisites:

- Go installed for local development/builds.
- A GitHub token with least-privilege access to the target repository's issues, labels, and comments.
- A repository with the labels used by your routes, for example `agent-ready`, `agent-running`, `agent-review`, `agent-failed`, and `manual-only`.

Try the example config from a scratch instance directory:

```bash
INSTANCE_DIR=/tmp/issueq-example
rm -rf "$INSTANCE_DIR"
mkdir -p "$INSTANCE_DIR"
cp -R examples/. "$INSTANCE_DIR"/

export GITHUB_TOKEN=replace_me
$EDITOR "$INSTANCE_DIR/issueq.yaml"

go run ./cmd/issueq --config "$INSTANCE_DIR/issueq.yaml" config-check
go run ./cmd/issueq --config "$INSTANCE_DIR/issueq.yaml" once
```

The example uses config-relative paths, so `./tasks/success.sh`, `./issueq.db`, and `./.issueq` are resolved inside `$INSTANCE_DIR`.

Run as a daemon during development:

```bash
go run ./cmd/issueq --config "$INSTANCE_DIR/issueq.yaml" daemon
```

Build a binary:

```bash
go build -o issueq ./cmd/issueq
./issueq --help
```

For production, prefer the documented `/srv/issueq` instance layout and systemd template.

## Configuration notes

Default config path for all commands is `./issueq.yaml`; override with `--config <path>`.

When config is loaded from a file:

- relative `queue.sqlite.path` is resolved relative to the config file directory;
- relative `workdir.path` is resolved relative to the config file directory;
- explicit relative job executables such as `./agents/code.sh` or `../tasks/run.sh` are resolved relative to the config file directory;
- bare commands such as `bash`, `python3`, or `code-agent` are left unchanged and resolved through `PATH`;
- job command arguments are not rewritten;
- SQLite `:memory:` is preserved.

Keep tokens in environment variables or secret managers, not in `issueq.yaml`.

## Operations and deployment

Recommended production layout:

```text
/srv/issueq/bin/issueq
/srv/issueq/instances/<owner-repo>/
  issueq.yaml
  env
  issueq.db
  work/
  agents/
  logs/
  runbook.md
```

Useful docs:

- [Operator runbook](docs/operator-runbook.md)
- [Deployment checklist](docs/deployment-checklist.md)
- [systemd template](deploy/systemd/issueq@.service)
- [example instance env file](deploy/systemd/instance.env.example)

Useful commands:

```bash
issueq --config ./issueq.yaml config-check
issueq --config ./issueq.yaml once
issueq --config ./issueq.yaml daemon
issueq --config ./issueq.yaml jobs --json
issueq --config ./issueq.yaml issues --json
```

## Security notes

IssueQ executes configured local commands on the daemon host. If those commands check out or run repository content, that content executes with the permissions of the service user and with whatever secrets you pass through the job environment.

Recommended baseline:

- use a dedicated `issueq` service user;
- use least-privilege, repository-scoped GitHub tokens;
- keep tokens in instance env files or a secret manager, never in Git;
- keep SQLite DBs, workdirs, logs, and env files out of the repository;
- do not run public fork/PR code unless you add an external sandbox and remove write tokens/secrets;
- back up SQLite before manual repair or upgrades.

See [SECURITY.md](SECURITY.md) for vulnerability reporting and [docs/operator-runbook.md](docs/operator-runbook.md) for recovery guidance.

## Roadmap

IssueQ currently focuses on GitHub Issues as the first durable event source. Future work may expand to PRs, comments, pushes, scheduled/manual triggers, and GitHub-Actions-inspired workflow files for local supervised automation.

See [docs/roadmap.md](docs/roadmap.md).

## Development

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

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
