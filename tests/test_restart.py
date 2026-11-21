"""Restart semantics: published artifacts survive, in-flight jobs interrupted."""

from __future__ import annotations

import os
from pathlib import Path

from conftest import SAMPLE_FILES, submit, wait_for_state


def _client_for(data_dir: Path):
    from fastapi.testclient import TestClient

    from app.config import Settings
    from app.main import create_app

    os.environ["CI_PACKAGER_DATA_DIR"] = str(data_dir)
    try:
        return TestClient(create_app(Settings.from_env()))
    finally:
        os.environ.pop("CI_PACKAGER_DATA_DIR", None)


def test_published_artifact_and_winner_record_survive_restart(tmp_path):
    data_dir = tmp_path / "data"
    with _client_for(data_dir) as client:
        job = wait_for_state(client, submit(client, SAMPLE_FILES)["job_id"])
        assert job["state"] == "succeeded"
        job_id = job["job_id"]
        sha = job["artifact"]["sha256"]
        zip_before = client.get(f"/jobs/{job_id}/artifact").content

    # New process: new Storage/app on the same data dir.
    with _client_for(data_dir) as client:
        again = client.get(f"/jobs/{job_id}").json()
        assert again["state"] == "succeeded"
        assert again["winner_round"] == 1
        assert again["artifact"]["sha256"] == sha
        assert client.get(f"/jobs/{job_id}/artifact").content == zip_before
        winners = client.get("/winners").json()
        assert [w["job_id"] for w in winners] == [job_id]
        record = client.get(f"/jobs/{job_id}/winner").json()
        assert record["sha256"] == sha and record["round"] == 1


def test_inflight_jobs_are_marked_interrupted_on_restart(tmp_path):
    data_dir = tmp_path / "data"
    with _client_for(data_dir) as client:
        job = submit(
            client,
            SAMPLE_FILES,
            first_delay_seconds=30,
            retry_delay_seconds=30,
            first_wait_seconds=5,
            max_runtime_seconds=60,
        )
        job_id = job["job_id"]
        assert job["state"] == "running"

    # Restart: startup must reconcile the dangling in-flight job.
    with _client_for(data_dir) as client:
        again = client.get(f"/jobs/{job_id}").json()
        assert again["state"] == "interrupted"
        assert again["detail"]
        assert all(a["state"] == "interrupted" for a in again["attempts"])
        assert client.get(f"/jobs/{job_id}/artifact").status_code == 404
        # The interrupted job can't be "resurrected" by a late executor result:
        # the retry row is still absent and nothing publishes.
        assert len(again["attempts"]) == 1


def test_interrupted_flag_persists_across_another_restart(tmp_path):
    data_dir = tmp_path / "data"
    with _client_for(data_dir) as client:
        job_id = submit(client, SAMPLE_FILES, first_delay_seconds=30)["job_id"]
    with _client_for(data_dir):
        pass
    with _client_for(data_dir) as client:
        assert client.get(f"/jobs/{job_id}").json()["state"] == "interrupted"


def test_orphan_round_zip_removed_on_startup(tmp_path):
    data_dir = tmp_path / "data"
    # A job that never published leaves a round ZIP behind; startup must prune it.
    (data_dir / "artifacts" / "orphan").mkdir(parents=True)
    orphan = data_dir / "artifacts" / "orphan" / "artifact-round1.zip"
    orphan.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
    with _client_for(data_dir) as client:
        assert client.get("/healthz").status_code == 200
    assert not orphan.exists()
