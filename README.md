# CI Artifact Packager

Small FastAPI service for CI: submit a handful of files, a Python executor
builds a verified ZIP, and the first valid result wins even when a retry is
racing a slow first round. Published ZIPs and winner records survive restarts.

## Run with Docker Compose

```bash
docker compose up --build -d
open http://localhost:8000/docs
```

The service listens on `http://localhost:8000` (override with `PACKAGER_PORT`).
State lives in the named volume `packager-data`, so `docker compose restart`
keeps published artifacts. Use `docker compose down -v` to reset.

Without Docker (dev):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
CI_PACKAGER_DATA_DIR=./data uvicorn app.main:app --port 8000
```

## API

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/jobs` | multipart upload (`files` repeated) + options; returns the job (202) |
| GET | `/jobs` | list jobs, newest first (`limit`, `offset`) |
| GET | `/jobs/{id}` | job state, both attempt states, winner, artifact metadata |
| GET | `/jobs/{id}/artifact` | download the published ZIP (`X-Artifact-SHA256`, `X-Artifact-Round`) |
| GET | `/jobs/{id}/winner` | winner record for one job |
| GET | `/winners` | winner records for every published job |
| GET | `/healthz` | liveness + counters |

### Options (multipart form fields on `POST /jobs`)

| Field | Default | Meaning |
| --- | --- | --- |
| `first_delay_seconds` | `0` | executor runtime of round 1 |
| `retry_delay_seconds` | `0` | executor runtime of round 2 |
| `first_fail` | `false` | make round 1 fail |
| `retry_fail` | `false` | make round 2 fail |
| `first_wait_seconds` | `10` | how long round 1 may run before a retry is started |
| `max_runtime_seconds` | `900` | absolute job cap; must exceed `first_wait_seconds` |

Limits (env): `CI_PACKAGER_MAX_FILES` (20), `CI_PACKAGER_MAX_FILE_BYTES`
(2 MiB), `CI_PACKAGER_MAX_TOTAL_BYTES` (5 MiB).

## Behaviour

* Round 1 starts immediately.
* Exactly one retry is started when round 1 **fails**, or when round 1 is still
  running after `first_wait_seconds` (the first round keeps running).
* The first ZIP that validates and commits is published; late valid results are
  recorded as `superseded` and never replace it.
* Both rounds failing (or the runtime cap) makes the job `failed`.
* A restart marks in-flight jobs `interrupted`; published artifacts and winner
  records stay downloadable.

### Race demo through `/docs`

1. `POST /jobs` with one file and
   `first_delay_seconds=5`, `retry_delay_seconds=0`, `first_wait_seconds=1`.
2. After ~1 s, `GET /jobs/{id}` shows round 1 `running`, round 2 `succeeded`,
   `winner_round=2`.
3. After ~5 s, round 1 turns `superseded`; the artifact hash is unchanged.
4. For a late win, use `first_delay_seconds=4`, `retry_delay_seconds=0.2`,
   `first_wait_seconds=1`, `retry_fail=true`: round 2 fails, round 1 publishes.
5. For total failure, `first_fail=true&retry_fail=true`.

## Layout

```
app/executor.py      Python ZIP executor (separate process per round)
app/orchestrator.py  submission, first-wait deadline, retry race, publication
app/storage.py       SQLite (WAL, FULL sync) jobs/attempts/artifacts
app/main.py          FastAPI routes and lifespan/restart reconciliation
tests/               API, race, restart and executor tests
scripts/probe.py     optional manual E2E probe
```

## Tests

```bash
pip install -r requirements.txt pytest httpx
python -m pytest
```
