#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
ISSUEQ_BIN=${ISSUEQ_BIN:-}
issueq() {
  if [ -n "$ISSUEQ_BIN" ]; then
    "$ISSUEQ_BIN" "$@"
  else
    (cd "$ROOT" && go run ./cmd/issueq "$@")
  fi
}
WORK=${WORK:-$(mktemp -d)}
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK"
cp "$ROOT/fixtures/events/pr-review.json" "$WORK/event.json"
cat > "$WORK/fake-agent.sh" <<'AGENT'
#!/usr/bin/env bash
set -euo pipefail
ctx=${1:?context path required}
res=${2:?result path required}
kind=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["event"]["kind"])' "$ctx")
key=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["event"]["key"])' "$ctx")
route=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["event"]["route"])' "$ctx")
case "$kind" in
  pr-review)
    python3 - "$res" "$key" "$route" <<'PY'
import json, sys, datetime
res, key, route = sys.argv[1:]
obj = {
  "schema": "issueq-agent-result/v1",
  "event_key": key,
  "route": route,
  "status": "succeeded",
  "decision": "findings_straightforward",
  "summary_markdown": "fake review found straightforward blocking findings",
  "work_started": True,
  "handoff": {
    "schema": "issueq-handoff/v1",
    "producer": {"event_key": key, "route": route, "decision": "findings_straightforward"},
    "target": {"kind": "pull_request", "key": "pr-241", "fingerprint": "head-0123456789abcdef0123456789abcdef01234567", "subscope": ""},
    "next_event": {"kind": "pr-fix", "route": "pr-fix"},
    "payload": {"finding_summary": "controlled fixture"},
    "created_at": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')
  },
  "next_event": {"kind": "pr-fix", "payload_patch": {"review_event_key": key}}
}
json.dump(obj, open(res, 'w'), indent=2)
PY
    ;;
  pr-fix)
    python3 - "$res" "$key" "$route" <<'PY'
import json, sys
res, key, route = sys.argv[1:]
json.dump({"schema":"issueq-agent-result/v1","event_key":key,"route":route,"status":"succeeded","decision":"fix_applied","summary_markdown":"fake fix applied","work_started":True}, open(res, 'w'), indent=2)
PY
    ;;
  *) echo "unexpected event kind $kind" >&2; exit 1 ;;
esac
AGENT
chmod +x "$WORK/fake-agent.sh"
cat > "$WORK/issueq.yaml" <<YAML
runner:
  name: fake-event-runner
queue:
  sqlite:
    path: ./issueq.db
  lease_duration: 30s
workdir:
  path: ./work
github:
  owner: example-org
  repo: example-repo
routes:
- name: pr-review
  event_kind: pr-review
  job:
    kind: event
    command: ["./fake-agent.sh"]
    timeout: 30s
    concurrency: 1
    max_attempts: 1
    follow_ups:
    - decision: findings_straightforward
      kind: pr-fix
      route: pr-fix
- name: pr-fix
  event_kind: pr-fix
  requires:
    handoff:
      from: pr-review
      decisions: [findings_straightforward]
      same_target: true
      expected_next: true
  job:
    kind: event
    command: ["./fake-agent.sh"]
    timeout: 30s
    concurrency: 1
    max_attempts: 1
YAML
(
  cd "$WORK"
  issueq --config "$WORK/issueq.yaml" config-check >/dev/null
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/event.json"
  issueq --config "$WORK/issueq.yaml" once
  issueq --config "$WORK/issueq.yaml" once
  issueq --config "$WORK/issueq.yaml" events list --json > events.json
  python3 - <<'PY'
import json
from pathlib import Path
items = json.load(open('events.json'))
status = {item['event_key']: item['status'] for item in items}
assert len(items) == 2, status
review = next(item for item in items if item['kind'] == 'pr-review')
fix = next(item for item in items if item['kind'] == 'pr-fix')
assert review['status'] == 'succeeded', review
assert fix['status'] == 'succeeded', fix
assert review['route_name'] == 'pr-review' and fix['route_name'] == 'pr-fix', items
assert review['attempt_count'] == 1 and fix['attempt_count'] == 1, items
assert 'review_event_key' in fix['payload_json'], fix['payload_json']
review_contexts = list(Path('work/events').glob('pr-review_*/context.json'))
fix_contexts = list(Path('work/events').glob('pr-fix_*/context.json'))
assert review_contexts and fix_contexts, 'context files missing'
fix_ctx = json.load(open(fix_contexts[0]))
assert fix_ctx['handoff']['producer_route'] == 'pr-review', fix_ctx['handoff']
assert fix_ctx['event']['route'] == 'pr-fix', fix_ctx['event']
print('event cutover scenario OK')
PY
)
