#!/usr/bin/env sh
set -eu

usage() {
  cat <<'USAGE'
Usage: handoff-gates-live-preflight.sh --config PATH [--db PATH] [--issue OWNER/REPO#N] [--bin PATH]

Checks local readiness for the handoff-gates live smoke. This script is
intentionally offline: it does not contact GitHub, mutate labels/comments, start
the daemon, or run issueq once.
USAGE
}

config=
db=
issue=
bin=${ISSUEQ_BIN:-issueq}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)
      config=${2:-}
      shift 2
      ;;
    --db)
      db=${2:-}
      shift 2
      ;;
    --issue)
      issue=${2:-}
      shift 2
      ;;
    --bin)
      bin=${2:-}
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -z "$config" ]; then
  usage >&2
  exit 2
fi

if [ ! -f "$config" ]; then
  echo "config not found: $config" >&2
  exit 1
fi

if command -v "$bin" >/dev/null 2>&1; then
  issueq_cmd=$bin
elif [ -x "$bin" ]; then
  issueq_cmd=$bin
else
  echo "issueq binary not found or not executable: $bin" >&2
  echo "set --bin PATH or ISSUEQ_BIN=PATH" >&2
  exit 1
fi

echo "== config validation =="
"$issueq_cmd" --config "$config" config-check

echo
echo "== handoff gate config hints =="
if grep -Eq '^[[:space:]]+gate:[[:space:]]*$' "$config"; then
  echo "found route gate block"
else
  echo "missing visible route gate block in config; confirm the live smoke route is intentionally gated" >&2
fi
if grep -Eq 'attempt_scope:[[:space:]]*handoff([[:space:]]|$)' "$config"; then
  echo "found attempt_scope: handoff"
else
  echo "missing attempt_scope: handoff; the smoke may not prove scoped attempt accounting" >&2
fi

if [ -n "$db" ]; then
  echo
  echo "== sqlite schema/readiness =="
  if [ ! -f "$db" ]; then
    echo "db not found yet: $db"
    echo "issueq will create/migrate it on first open; back it up before live repair once it exists"
  elif command -v sqlite3 >/dev/null 2>&1; then
    for table in handoffs gate_blocks route_attempts; do
      table_count=$(sqlite3 "$db" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='$table';")
      if [ "$table_count" != "1" ]; then
        echo "required table missing: $table" >&2
        exit 1
      fi
      echo "required table present: $table"
    done
    if sqlite3 "$db" "PRAGMA table_info(route_attempts);" | grep -q '|scope_hash|'; then
      echo "route_attempts.scope_hash present"
    else
      echo "route_attempts.scope_hash missing" >&2
      exit 1
    fi
  else
    echo "sqlite3 not found; cannot inspect existing DB schema" >&2
    exit 1
  fi
fi

echo
echo "== deterministic local smoke =="
echo "Run before live smoke:"
echo "  GOCACHE=/tmp/issueq-go-build go test -count=1 ./internal/daemon -run 'TestLocal(HandoffGatesSmoke|WorkStartedFallbackSmoke)'"

echo
echo "== live smoke commands to run manually in a stopped-service window =="
if [ -n "$issue" ]; then
  echo "Scratch issue: $issue"
else
  echo "Create or choose a scratch issue and record it as OWNER/REPO#N."
fi
echo "1. Pause intake if applicable, then stop the production issueq service and confirm it is inactive."
echo "2. Apply labels: agent-ready, agent-route-bug-fix-pr, agent-write-approved."
echo "3. Confirm no accepted bug-triage canonical issueq-handoff fenced comment exists on the issue."
echo "4. Run: $issueq_cmd --config $config once"
echo "5. Verify a missing_handoff block comment/label appeared and no bug-fix-pr attempt row was consumed."
echo "6. Add a fresh bug-triage handoff comment, restore route labels if the block action removed them, then run once again."
echo "7. Verify bug-fix-pr ran exactly once and route_attempts for that handoff scope is 1."
echo "8. Re-arm the same handoff/scope once more and verify max_attempts stops it without another work launch."
echo "9. Remove smoke labels/comments as needed, restart the issueq service, and tail logs."

echo
echo "Preflight complete. No live GitHub operations were executed."
