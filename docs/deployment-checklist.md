# issueq deployment checklist

Use this checklist for a first production install or wrapper-only upgrade.

## Before publishing or deploying

- [ ] Current tree has no committed secrets, tokens, production env files, SQLite DBs, logs, or workdirs.
- [ ] `.gitignore` excludes local config/secrets, DB files, workdirs, logs, temp files, and built binaries.
- [ ] README clearly states current status and trust boundary.
- [ ] CI passes on the public default branch.
- [ ] A license is present if external use is intended.

## Instance planning

- [ ] Choose a systemd-safe instance name, e.g. `owner-repo` rather than `owner/repo`.
- [ ] Create/use a dedicated service user, e.g. `issueq`.
- [ ] Use the standard layout:

  ```text
  /srv/issueq/bin/issueq
  /srv/issueq/instances/<owner-repo>/issueq.yaml
  /srv/issueq/instances/<owner-repo>/env
  /srv/issueq/instances/<owner-repo>/issueq.db
  /srv/issueq/instances/<owner-repo>/work/
  /srv/issueq/instances/<owner-repo>/agents/
  /srv/issueq/instances/<owner-repo>/logs/
  ```

- [ ] Decide which repository this instance may control.
- [ ] Decide the queue labels and terminal labels.
- [ ] Decide whether job commands execute only trusted code. If not, add an external sandbox before enabling public/untrusted triggers.

## GitHub token

- [ ] Create a least-privilege GitHub token/App installation for one target repository.
- [ ] Token can read issues and labels.
- [ ] Token can write issue labels and issue comments.
- [ ] Token is stored only in `/srv/issueq/instances/<owner-repo>/env` or another secret manager.
- [ ] Env file ownership and permissions are locked down:

  ```sh
  sudo chown issueq:issueq /srv/issueq/instances/<owner-repo>/env
  sudo chmod 0600 /srv/issueq/instances/<owner-repo>/env
  ```

## Install or upgrade binary

- [ ] Build the binary:

  ```sh
  go test ./...
  go vet ./...
  go build -o issueq ./cmd/issueq
  ```

- [ ] Install the binary:

  ```sh
  sudo install -d -o root -g root -m 0755 /srv/issueq/bin
  sudo install -o root -g root -m 0755 ./issueq /srv/issueq/bin/issueq
  ```

- [ ] Confirm the installed version starts:

  ```sh
  /srv/issueq/bin/issueq --help
  ```

## Configure instance

- [ ] Create instance directories:

  ```sh
  INSTANCE=<owner-repo>
  sudo install -d -o issueq -g issueq -m 0750 /srv/issueq/instances/$INSTANCE
  sudo install -d -o issueq -g issueq -m 0750 /srv/issueq/instances/$INSTANCE/work
  sudo install -d -o issueq -g issueq -m 0750 /srv/issueq/instances/$INSTANCE/agents
  sudo install -d -o issueq -g issueq -m 0750 /srv/issueq/instances/$INSTANCE/logs
  ```

- [ ] Write `/srv/issueq/instances/<owner-repo>/issueq.yaml`.
- [ ] Prefer config-relative paths:

  ```yaml
  queue:
    sqlite:
      path: ./issueq.db
  workdir:
    path: ./work
  routes:
    - name: code
      job:
        command: ["./agents/code.sh"]
  ```

- [ ] Ensure `github.owner`, `github.repo`, and `github.token_env` are correct.
- [ ] Ensure `terminal_labels` include labels that should prevent future routing.
- [ ] Ensure `max_global_concurrency`, per-route `concurrency`, `timeout`, and `max_attempts` are conservative.
- [ ] Ensure all job executables exist and are executable by `issueq`.
- [ ] Ensure DB/work/agents/logs paths are writable by `issueq`.
- [ ] Validate config as the service user:

  ```sh
  sudo -u issueq /srv/issueq/bin/issueq --config /srv/issueq/instances/<owner-repo>/issueq.yaml config-check
  ```

## Install systemd unit

- [ ] Copy the template:

  ```sh
  sudo cp deploy/systemd/issueq@.service /etc/systemd/system/issueq@.service
  sudo systemctl daemon-reload
  ```

- [ ] Review unit hardening. If job commands need access outside `/srv/issueq/instances/<owner-repo>`, adjust `ReadWritePaths`, `ReadOnlyPaths`, or `ProtectSystem` intentionally.
- [ ] Enable but do not start until ready:

  ```sh
  sudo systemctl enable issueq@<owner-repo>
  ```

## Cutover from an older daemon/runtime

- [ ] Pause intake: remove ready labels from pending issues or otherwise prevent new matching issues.
- [ ] Stop the old daemon:

  ```sh
  sudo systemctl stop issueq@<owner-repo>
  ```

- [ ] Wait for, cancel, or manually resolve currently running jobs before deploying if the old runtime cannot be adopted safely.
- [ ] Back up SQLite:

  ```sh
  sudo -u issueq sqlite3 /srv/issueq/instances/<owner-repo>/issueq.db \
    ".backup '/srv/issueq/instances/<owner-repo>/issueq.db.before-cutover-$(date -u +%Y%m%dT%H%M%SZ)'"
  ```

- [ ] Verify there are no old attached-era running rows without durable launch metadata:

  ```sql
  SELECT id, issue_key, status, supervisor_kind, launch_state, launch_token
  FROM jobs
  WHERE status = 'running'
    AND (supervisor_kind IS NULL OR supervisor_kind = ''
         OR launch_token IS NULL OR launch_token = ''
         OR launch_state IS NULL OR launch_state = '');
  ```

- [ ] Deploy the wrapper-only binary.
- [ ] Run `config-check`.
- [ ] Start the systemd daemon.

## First start

- [ ] Start the daemon:

  ```sh
  sudo systemctl start issueq@<owner-repo>
  ```

- [ ] Watch logs:

  ```sh
  journalctl -u issueq@<owner-repo> -f
  ```

- [ ] Confirm service stays active:

  ```sh
  systemctl status issueq@<owner-repo>
  ```

- [ ] Run a safe test issue with a scenario-specific ready label.
- [ ] Confirm the job is claimed, wrapper starts, and GitHub labels transition as expected.
- [ ] Inspect `jobs --json` and artifact files for the first job.
- [ ] Confirm no unexpected `running`, `failed`, or `launch_state='unknown'` rows remain.

## Handoff-gates rollout

- [ ] Confirm the deployed binary supports `gate.handoff` and `job.attempt_scope`.
- [ ] Back up the live SQLite DB and current `issueq.yaml`.
- [ ] Add the handoff gate only after the new binary is installed.
- [ ] Keep the gated write route's real-work cap conservative, for example `max_attempts: 1`.
- [ ] Set the gated write route to `attempt_scope: handoff`.
- [ ] Run `config-check` as the service user.
- [ ] Run the deterministic local smoke:

  ```sh
  GOCACHE=/tmp/issueq-go-build go test -count=1 ./internal/daemon -run 'TestLocal(HandoffGatesSmoke|WorkStartedFallbackSmoke)'
  ```

- [ ] Run offline preflight against the live config:

  ```sh
  ./scripts/handoff-gates-live-preflight.sh \
    --bin /srv/issueq/bin/issueq \
    --config /srv/issueq/instances/<owner-repo>/issueq.yaml \
    --db /srv/issueq/instances/<owner-repo>/issueq.db \
    --issue OWNER/REPO#123
  ```

- [ ] Before applying smoke labels or handoff comments, pause intake if applicable, stop the production `issueq` service, and confirm no daemon is polling the smoke config/DB.
- [ ] Run the live smoke from `docs/operator-runbook.md` on a scratch or explicitly approved issue.
- [ ] Verify missing handoff blocks did not create jobs or consume route attempts.
- [ ] Verify a fresh accepted handoff allows exactly one scoped work attempt.
- [ ] Verify re-arming the same handoff/scope hits max attempts without launching work again.
- [ ] Clean up smoke labels/comments before restarting the service unless the issue is intentionally retained as evidence.
- [ ] Restart the service after the controlled smoke window and tail logs for config, migration, polling, routing, and dispatch errors.
- [ ] Document rollback and cleanup notes in the instance-local `runbook.md`.

## Ongoing operations

- [ ] Schedule SQLite backups.
- [ ] Configure log retention for journald and any instance logs.
- [ ] Keep previous release binaries for rollback.
- [ ] Keep an instance-local `runbook.md` with repository-specific labels, agents, and manual repair notes.
- [ ] Periodically check token expiry and permissions.
- [ ] Re-run a smoke issue after changing config, agents, labels, token permissions, or the issueq binary.
