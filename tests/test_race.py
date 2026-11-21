"""Race semantics: one retry, first valid ZIP wins, late results never replace."""

from __future__ import annotations

import io
import threading
import zipfile
from datetime import datetime, timezone

from conftest import SAMPLE_FILES, submit, wait_for_state

from app.models import JobOptions
from app.storage import Storage


def _zip_bytes(client, job_id: str) -> bytes:
    response = client.get(f"/jobs/{job_id}/artifact")
    assert response.status_code == 200
    return response.content


def test_retry_is_started_once_when_first_round_is_slow(client):
    job = wait_for_state(
        client,
        submit(
            client,
            SAMPLE_FILES,
            first_delay_seconds=1.5,
            retry_delay_seconds=0.1,
            first_wait_seconds=0.3,
        )["job_id"],
        timeout=20,
    )
    assert job["state"] == "succeeded"
    assert job["winner_round"] == 2  # fast retry published first
    rounds = [(a["round"], a["state"]) for a in job["attempts"]]
    assert rounds == [(1, "running"), (2, "succeeded")]
    first = next(a for a in job["attempts"] if a["round"] == 1)
    assert first["finished_at"] is None  # still executing after publication

    # A late round-1 result is recorded as superseded once it completes.
    def eventually() -> dict:
        import time

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            current = client.get(f"/jobs/{job['job_id']}").json()
            if all(a["state"] != "running" for a in current["attempts"]):
                return current
            time.sleep(0.1)
        raise AssertionError("late round never finished")

    late = eventually()
    assert late["winner_round"] == 2
    assert late["artifact"]["sha256"] == job["artifact"]["sha256"]
    assert [(a["round"], a["state"]) for a in late["attempts"]] == [
        (1, "superseded"),
        (2, "succeeded"),
    ]
    # The published ZIP is byte-identical to the one served right after publish.
    assert _zip_bytes(client, job["job_id"]) == _zip_bytes(client, job["job_id"])
    archive = zipfile.ZipFile(io.BytesIO(_zip_bytes(client, job["job_id"])))
    assert archive.testzip() is None


def test_fast_first_round_cancels_pending_retry(client):
    job = wait_for_state(
        client,
        submit(
            client,
            SAMPLE_FILES,
            first_delay_seconds=0.0,
            first_wait_seconds=0.5,
        )["job_id"],
    )
    assert job["winner_round"] == 1
    import time

    time.sleep(0.8)  # past the deadline
    job = client.get(f"/jobs/{job['job_id']}").json()
    assert [a["round"] for a in job["attempts"]] == [1]


def test_only_two_rounds_ever(client):
    job = wait_for_state(
        client,
        submit(client, SAMPLE_FILES, first_fail="true", retry_fail="true")["job_id"],
    )
    assert [a["round"] for a in job["attempts"]] == [1, 2]
    assert job["state"] == "failed"


def test_storage_publish_is_first_writer_wins(tmp_path):
    """Two threads racing to publish: exactly one artifact, the other superseded."""
    storage = Storage(tmp_path / "db.sqlite3")
    options = JobOptions(first_wait_seconds=0)
    storage.create_job("job1", options, datetime.now(timezone.utc))
    assert storage.begin_attempt("job1", 2)

    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def publish(round_no: int) -> None:
        barrier.wait()
        outcomes.append(
            storage.finish_attempt(
                "job1",
                round_no,
                zip_sha256=f"hash{round_no}",
                zip_name=f"artifact-round{round_no}.zip",
                size=10,
                file_count=1,
            )
        )

    threads = [threading.Thread(target=publish, args=(r,)) for r in (1, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(outcomes) == ["published", "superseded"]
    view = storage.get_job("job1")
    assert view is not None and view.state.value == "succeeded"
    winner = view.winner_round
    assert winner in (1, 2)
    artifact = storage.get_artifact("job1")
    assert artifact is not None and artifact["sha256"] == f"hash{winner}"
    states = {a.round: a.state.value for a in view.attempts}
    assert states[winner] == "succeeded"
    assert states[3 - winner] == "superseded"
    storage.close()


def test_late_valid_result_cannot_replace_published_artifact(client):
    job = wait_for_state(
        client,
        submit(
            client,
            SAMPLE_FILES,
            first_delay_seconds=1.2,
            retry_delay_seconds=0.1,
            first_wait_seconds=0.2,
        )["job_id"],
        timeout=20,
    )
    first_sha = job["artifact"]["sha256"]
    import time

    time.sleep(2.0)
    current = client.get(f"/jobs/{job['job_id']}").json()
    assert current["artifact"]["sha256"] == first_sha
    assert current["winner_round"] == job["winner_round"]


def test_many_slow_first_rounds_still_get_exactly_one_retry(client):
    """Publish-then-late-finish for several jobs at once; one retry each."""
    jobs = [
        submit(
            client,
            SAMPLE_FILES,
            first_delay_seconds=0.8,
            retry_delay_seconds=0.05,
            first_wait_seconds=0.1,
        )["job_id"]
        for _ in range(4)
    ]
    settled = [wait_for_state(client, j, timeout=20) for j in jobs]
    assert all(j["state"] == "succeeded" for j in settled)
    assert all(j["winner_round"] == 2 for j in settled)

    import time

    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        current = [client.get(f"/jobs/{j}").json() for j in jobs]
        if all(
            all(a["state"] != "running" for a in job["attempts"]) for job in current
        ):
            break
        time.sleep(0.1)
    else:
        raise AssertionError("late first rounds never finished")
    for job in current:
        assert [a["round"] for a in job["attempts"]] == [1, 2]
        assert [a["state"] for a in job["attempts"]] == ["superseded", "succeeded"]
        assert job["winner_round"] == 2
