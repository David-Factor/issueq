# issueq

[![CI](https://github.com/David-Factor/issueq/actions/workflows/ci.yml/badge.svg)](https://github.com/David-Factor/issueq/actions/workflows/ci.yml)

issueq is a minimal local queue for running agent workflows from GitHub issues.

It is meant as a practical starting point for experimenting with a personal "dark factory": a background automation loop where tasks are described, queued, executed by agents, reviewed, and advanced through workflow states.

## Why issueq?

As coding agents improve, more routine software work becomes a candidate for automation: triage, dependency bumps, investigations, migrations, test fixes, documentation updates, cleanup tasks, and small feature slices.

issueq lets you try that in an environment you control: your laptop, a VM, remote workstation, or controlled hosted dev environment. You keep the files, credentials, tools, prompts, and execution environment close enough to inspect and change.

GitHub issues are the interface because they already live near the code, labels, discussions, PRs, and project context. issueq treats issues as the visible source of intent while its local SQLite queue tracks execution state.

## What issueq does

You define:

- the labels/states in your workflow;
- which issues are eligible for automation;
- which command or agent runs for each route;
- what labels/comments should be applied on start, success, failure, or retry exhaustion;
- concurrency, timeouts, attempts, and environment passing.

issueq then runs this loop:

```text
GitHub issue -> local queue -> configured agent command -> result -> next issue state
```

issueq is intentionally workflow-agnostic. It does not know what "triage", "code", "review", or "done" mean for your project; it just moves issues through the states and commands you configure.

## Status and scope

issueq is public-preview software, currently focused on single-operator GitHub Issues automation.

Implemented:

- GitHub Issues polling and label-based routing;
- local SQLite queue/state storage;
- durable wrapper-only subprocess supervision via `issueq job-wrapper`;
- result JSON, stdout, stderr, and wrapper metadata capture;
- GitHub issue label/comment updates after job completion;
- systemd-oriented deployment docs for running one instance per repository.

Not implemented yet:

- broad GitHub event support for PRs, pushes, releases, etc.;
- multi-user/shared-queue collaboration;
- a sandbox for untrusted code;
- GitHub-Actions-compatible workflow execution;
- no supported fallback to the deprecated attached/direct runner.

Single-person/local-first operation is intentional. It is the smallest useful step: automate your own recurring workflows safely before introducing shared stores, hosted runners, or team coordination. Future collaboration would likely come through a shared remote queue/store rather than treating one local SQLite instance as a team coordination system.

## Quick start

Prerequisites:

- Go installed for local development/builds.
- A GitHub token with least-privilege access to the target repository: metadata read plus issues/labels/comments read-write; repository contents only if your configured agent needs them.
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

`issueq once --no-wait` remains intentionally unsupported until durable detached CLI semantics are designed. `dispatch --local-no-github` is only for local fixtures; normal dispatch is GitHub-aware.

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

Docs:

- [Operator runbook](docs/operator-runbook.md)
- [Deployment checklist](docs/deployment-checklist.md)
- [systemd template](deploy/systemd/issueq@.service)
- [example instance env file](deploy/systemd/instance.env.example)
- [Roadmap](docs/roadmap.md)

Useful commands:

```bash
issueq --config ./issueq.yaml config-check
issueq --config ./issueq.yaml once
issueq --config ./issueq.yaml daemon
issueq --config ./issueq.yaml jobs --json
issueq --config ./issueq.yaml issues --json
```

## Security notes

issueq executes configured local commands on the daemon host. If those commands check out or run repository content, that content executes with the permissions of the service user and with whatever secrets you pass through the job environment.

Recommended baseline:

- use a dedicated `issueq` service user;
- use least-privilege, repository-scoped GitHub tokens;
- keep tokens in instance env files or a secret manager, never in Git;
- keep SQLite DBs, workdirs, logs, and env files out of the repository;
- do not run public fork/PR code unless you add an external sandbox and remove write tokens/secrets;
- back up SQLite before manual repair or upgrades.

See [SECURITY.md](SECURITY.md) for vulnerability reporting and [docs/operator-runbook.md](docs/operator-runbook.md) for recovery guidance.

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
