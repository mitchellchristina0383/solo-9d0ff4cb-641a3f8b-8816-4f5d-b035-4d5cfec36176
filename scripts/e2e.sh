#!/usr/bin/env bash
# End-to-end run against a real uvicorn process:
#   1. start server, submit a racing job, confirm the retry wins
#   2. submit a slow job, SIGKILL the server mid-flight, restart, check state
#   3. confirm the published ZIP and winner record survive
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${PORT:-8765}"
DATA="$(mktemp -d /tmp/ci-e2e-XXXXXX)"
export PYTHONPATH="$ROOT/.pylibs:$ROOT"
export CI_PACKAGER_DATA_DIR="$DATA"
BASE="http://127.0.0.1:$PORT"
PID=""

cleanup() {
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

start_server() {
  python3 -m uvicorn app.main:app --host 127.0.0.1 --port "$PORT" --no-access-log \
    >"$DATA/server.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do
    if curl -fsS "$BASE/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "server failed to start"; cat "$DATA/server.log"; exit 1
}

submit() { # files... via form
  curl -fsS -X POST "$BASE/jobs" "$@"
}

echo "== data dir: $DATA"
start_server
echo "server pid=$PID"

printf 'alpha\n' >"$DATA/a.txt"
mkdir -p "$DATA/in/src"
printf 'print(1)\n' >"$DATA/in/src/m.py"

echo "-- 1) race: slow round 1 vs fast retry"
JOB=$(submit \
  -F "files=@$DATA/a.txt;filename=a.txt" \
  -F "files=@$DATA/in/src/m.py;filename=src/m.py" \
  -F "first_delay_seconds=3" \
  -F "retry_delay_seconds=0.1" \
  -F "first_wait_seconds=0.5" | jq -r .job_id)
echo "job=$JOB"
sleep 1.2
curl -fsS "$BASE/jobs/$JOB" | jq -c '{state,winner_round,attempts:[.attempts[]|{round,state}]}'
sleep 3.5
MID=$(curl -fsS "$BASE/jobs/$JOB")
echo "$MID" | jq -c '{state,winner_round,attempts:[.attempts[]|{round,state}],sha:.artifact.sha256}'
SHA=$(echo "$MID" | jq -r .artifact.sha256)
curl -fsS "$BASE/jobs/$JOB/artifact" -o "$DATA/winner.zip"
unzip -l "$DATA/winner.zip" | sed -n '1,10p'
python3 - "$DATA/winner.zip" "$SHA" <<'PY'
import hashlib, sys, zipfile
path, expected = sys.argv[1], sys.argv[2]
digest = hashlib.sha256(open(path,'rb').read()).hexdigest()
assert digest == expected, (digest, expected)
assert zipfile.ZipFile(path).testzip() is None
print("zip verified, sha ok")
PY

echo "-- 2) restart durability: submit slow job, SIGKILL, restart"
SLOW=$(submit -F "files=@$DATA/a.txt;filename=a.txt" \
  -F "first_delay_seconds=30" -F "retry_delay_seconds=30" \
  -F "first_wait_seconds=5" -F "max_runtime_seconds=60" | jq -r .job_id)
echo "slow job=$SLOW"
sleep 0.7
kill -9 "$PID"; wait "$PID" 2>/dev/null || true
echo "killed server"
start_server
echo "server restarted pid=$PID"
for _ in $(seq 1 50); do
  ST=$(curl -fsS "$BASE/jobs/$SLOW" | jq -r .state)
  [ "$ST" != "running" ] && break
  sleep 0.1
done
curl -fsS "$BASE/jobs/$SLOW" | jq -c '{state,detail,attempts:[.attempts[]|{round,state}]}'
[ "$ST" = "interrupted" ] || { echo "expected interrupted, got $ST"; exit 1; }

echo "-- 3) published artifact + winner record survive restart"
curl -fsS "$BASE/jobs/$JOB" | jq -c '{state,winner_round,sha:.artifact.sha256}'
curl -fsS "$BASE/jobs/$JOB/artifact" -o "$DATA/after-restart.zip"
cmp "$DATA/winner.zip" "$DATA/after-restart.zip" && echo "zip identical after restart"
curl -fsS "$BASE/jobs/$JOB/winner" | jq -c .
curl -fsS "$BASE/winners" | jq -c '[.[]|{job_id,round,sha256}]'
[ "$(curl -fsS "$BASE/jobs/$JOB" | jq -r .artifact.sha256)" = "$SHA" ] || exit 1

echo "-- 4) docs + openapi reachable"
curl -fsS -o /dev/null -w 'docs=%{http_code}\n' "$BASE/docs"
curl -fsS -o /dev/null -w 'openapi=%{http_code}\n' "$BASE/openapi.json"

echo "E2E OK ($DATA)"
