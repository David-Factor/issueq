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
if [ -z "${KEEP_WORK:-}" ]; then
  trap 'rm -rf "$WORK"' EXIT
else
  echo "keeping scenario workdir: $WORK" >&2
fi
mkdir -p "$WORK"
cat > "$WORK/fake-agent.sh" <<'AGENT'
#!/usr/bin/env bash
set -euo pipefail
ctx=${1:?context path required}
res=${2:?result path required}
python3 - "$ctx" "$res" <<'PY'
import datetime, json, sys
ctx_path, res_path = sys.argv[1:]
ctx = json.load(open(ctx_path))
event = ctx["event"]
payload = ctx.get("payload") or {}
kind = event["kind"]
key = event["key"]
route = event["route"]
scenario = payload.get("scenario", "")
now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')

def result(status, decision, summary, *, next_kind=None, handoff=True, patch=None):
    obj = {
        "schema": "issueq-agent-result/v1",
        "event_key": key,
        "route": route,
        "status": status,
        "decision": decision,
        "summary_markdown": summary,
        "work_started": status != "stale",
    }
    if handoff:
        obj["handoff"] = {
            "schema": "issueq-handoff/v1",
            "producer": {"event_key": key, "route": route, "decision": decision},
            "target": {"kind": ctx["target"]["kind"], "key": ctx["target"]["key"], "fingerprint": ctx["target"]["fingerprint"], "subscope": event.get("subscope", "")},
            "next_event": {"kind": next_kind or "", "route": next_kind or ""},
            "payload": {"scenario": scenario or kind},
            "created_at": now,
        }
    if next_kind:
        obj["next_event"] = {"kind": next_kind, "payload_patch": patch or {"producer_event_key": key, "scenario": f"{next_kind}-from-{kind}"}}
    json.dump(obj, open(res_path, "w"), indent=2)

if kind == "pr-review" and scenario == "wrong-handoff":
    result("succeeded", "merge_ready", "wrong decision handoff must not satisfy pr-fix gate", next_kind=None, handoff=True)
elif kind == "pr-review" and scenario == "stale-review":
    result("stale", "stale_noop", "stale target; no write follow-up", handoff=False)
elif kind == "pr-review":
    result("succeeded", "findings_straightforward", "review findings", next_kind="pr-fix")
elif kind == "pr-fix":
    result("succeeded", "fix_applied", "fix applied", handoff=False)
elif kind == "ci-diagnose":
    result("succeeded", "fix_candidate", "diagnosed straightforward CI failure", next_kind="ci-fix")
elif kind == "ci-fix":
    result("succeeded", "fix_applied", "ci fix applied", handoff=False)
else:
    raise SystemExit(f"unexpected kind {kind}")
PY
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
- name: ci-diagnose
  event_kind: ci-diagnose
  job:
    kind: event
    command: ["./fake-agent.sh"]
    timeout: 30s
    concurrency: 1
    max_attempts: 1
    follow_ups:
    - decision: fix_candidate
      kind: ci-fix
      route: ci-fix
- name: ci-fix
  event_kind: ci-fix
  requires:
    handoff:
      from: ci-diagnose
      decisions: [fix_candidate]
      same_target: true
      expected_next: true
  job:
    kind: event
    command: ["./fake-agent.sh"]
    timeout: 30s
    concurrency: 1
    max_attempts: 1
YAML
make_event() {
  local path=$1 kind=$2 pr=$3 sha=$4 scenario=${5:-} subscope=${6:-}
  python3 - "$path" "$kind" "$pr" "$sha" "$scenario" "$subscope" <<'PY'
import json, sys
path, kind, pr, sha, scenario, subscope = sys.argv[1:]
fingerprint = f"head-{sha}"
event = {
  "schema": "issueq-event/v1",
  "kind": kind,
  "event_key": f"{kind}:gleg.int.exe.xyz/example-org/example-repo:pr-{pr}:{fingerprint}" + (f":{subscope}" if subscope else ""),
  "repo": {"host": "gleg.int.exe.xyz", "owner": "example-org", "name": "example-repo"},
  "source": {"kind": "local_scenario", "key": f"{kind}-{pr}", "url": "https://example.invalid/run"},
  "target": {"kind": "pull_request", "key": f"pr-{pr}", "fingerprint": fingerprint},
  "payload": {"pr_number": int(pr), "head_sha": sha, "scenario": scenario},
}
if subscope:
    event["subscope"] = subscope
json.dump(event, open(path, "w"), indent=2)
PY
}
(
  cd "$WORK"
  issueq --config "$WORK/issueq.yaml" config-check >/dev/null

  # Duplicate ingestion before processing must remain a single event.
  make_event "$WORK/dup.json" pr-review 300 0000000000000000000000000000000000000300 duplicate
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/dup.json" >/dev/null
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/dup.json" >/dev/null
  issueq --config "$WORK/issueq.yaml" events list --json > events.json
  python3 - <<'PY'
import json
items=json.load(open('events.json'))
assert len(items)==1, items
assert items[0]['attempt_count']==0 and items[0]['status']=='ready', items
PY
  issueq --config "$WORK/issueq.yaml" once >/dev/null
  issueq --config "$WORK/issueq.yaml" once >/dev/null
  issueq --config "$WORK/issueq.yaml" events list --json > "$WORK/after-dup-run.json"
  # Terminal duplicate upsert must not reset status/attempt/result or enqueue another run.
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/dup.json" >/dev/null
  issueq --config "$WORK/issueq.yaml" once >/dev/null
  issueq --config "$WORK/issueq.yaml" events list --json > "$WORK/after-dup-reupsert.json"
  python3 - <<'PY'
import json
before = {item['event_key']: item for item in json.load(open('after-dup-run.json'))}
after = {item['event_key']: item for item in json.load(open('after-dup-reupsert.json'))}
assert set(before) == set(after), (before, after)
for key, item in before.items():
    assert after[key]['status'] == item['status'], (item, after[key])
    assert after[key]['attempt_count'] == item['attempt_count'], (item, after[key])
    assert after[key]['result_json'] == item['result_json'], (item, after[key])
PY

  # Missing handoff: direct write event is terminal needs_human and does not run command.
  make_event "$WORK/missing.json" pr-fix 301 0000000000000000000000000000000000000301 direct-write
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/missing.json" >/dev/null
  issueq --config "$WORK/issueq.yaml" once >/dev/null

  # Wrong handoff: producer decision is not allowed by pr-fix gate.
  make_event "$WORK/wrong-review.json" pr-review 302 0000000000000000000000000000000000000302 wrong-handoff
  make_event "$WORK/wrong-fix.json" pr-fix 302 0000000000000000000000000000000000000302 wrong-handoff
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/wrong-review.json" >/dev/null
  issueq --config "$WORK/issueq.yaml" once >/dev/null
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/wrong-fix.json" >/dev/null
  issueq --config "$WORK/issueq.yaml" once >/dev/null

  # Stale/no-op report: terminal stale and no follow-up event.
  make_event "$WORK/stale.json" pr-review 303 0000000000000000000000000000000000000303 stale-review
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/stale.json" >/dev/null
  issueq --config "$WORK/issueq.yaml" once >/dev/null

  # CI diagnose -> CI fix gated follow-up.
  make_event "$WORK/ci.json" ci-diagnose 304 0000000000000000000000000000000000000304 ci-failure workflow-ci
  issueq --config "$WORK/issueq.yaml" event upsert --json "$WORK/ci.json" >/dev/null
  issueq --config "$WORK/issueq.yaml" once >/dev/null
  issueq --config "$WORK/issueq.yaml" once >/dev/null

  issueq --config "$WORK/issueq.yaml" events list --json > final-events.json
  python3 - <<'PY'
import json
from pathlib import Path
items=json.load(open('final-events.json'))
by_key={i['event_key']:i for i in items}

def key(kind, pr, sha, subscope=''):
    suffix=f':{subscope}' if subscope else ''
    return f'{kind}:gleg.int.exe.xyz/example-org/example-repo:pr-{pr}:head-{sha}{suffix}'
sha=lambda n: '0'*37+str(n)
# Duplicate/terminal non-reset path: exactly one review and one policy fix; review was not re-run.
dup=by_key[key('pr-review',300,sha(300))]
dup_fix=by_key[key('pr-fix',300,sha(300))]
assert dup['status']=='succeeded' and dup['attempt_count']==1, dup
assert dup_fix['status']=='succeeded' and dup_fix['attempt_count']==1, dup_fix
# Missing/wrong handoffs are blocked before command execution.
missing=by_key[key('pr-fix',301,sha(301))]
wrong_review=by_key[key('pr-review',302,sha(302))]
wrong_fix=by_key[key('pr-fix',302,sha(302))]
assert missing['status']=='needs_human' and missing['attempt_count']==1, missing
assert wrong_review['status']=='succeeded' and wrong_review['attempt_count']==1, wrong_review
assert wrong_fix['status']=='needs_human' and wrong_fix['attempt_count']==1, wrong_fix
# Stale report is terminal and no pr-fix follow-up exists for that target.
stale=by_key[key('pr-review',303,sha(303))]
assert stale['status']=='stale' and stale['attempt_count']==1, stale
assert key('pr-fix',303,sha(303)) not in by_key, by_key.get(key('pr-fix',303,sha(303)))
# CI diagnose creates exactly the gated ci-fix follow-up, preserving workflow subscope.
ci=by_key[key('ci-diagnose',304,sha(304),'workflow-ci')]
ci_fix=by_key[key('ci-fix',304,sha(304),'workflow-ci')]
assert ci['status']=='succeeded' and ci['attempt_count']==1, ci
assert ci_fix['status']=='succeeded' and ci_fix['attempt_count']==1, ci_fix
fix_contexts=list(Path('work/events').glob('ci-fix_*/context.json'))
assert fix_contexts, 'ci-fix context missing'
ctx=json.load(open(fix_contexts[0]))
assert ctx['handoff']['producer_route']=='ci-diagnose', ctx['handoff']
assert ctx['event']['subscope']=='workflow-ci', ctx['event']
print('event cutover safety scenario OK')
PY
)
