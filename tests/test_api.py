"""API-level behaviour: validation, success path, failure path."""

from __future__ import annotations

import io
import os
import zipfile

from conftest import SAMPLE_FILES, submit, wait_for_state
from app import __version__


def test_health(client):
    body = client.get("/healthz").json()
    assert body["status"] == "ok"
    assert body["version"] == __version__


def test_openapi_and_docs(client):
    assert client.get("/docs").status_code == 200
    spec = client.get("/openapi.json").json()
    assert "/jobs" in spec["paths"]
    assert "/jobs/{job_id}/artifact" in spec["paths"]


def test_success_publishes_verified_zip(client):
    job = wait_for_state(client, submit(client, SAMPLE_FILES)["job_id"])
    assert job["state"] == "succeeded"
    assert job["winner_round"] == 1
    assert [a["state"] for a in job["attempts"]] == ["succeeded"]

    response = client.get(f"/jobs/{job['job_id']}/artifact")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert response.headers["x-artifact-sha256"] == job["artifact"]["sha256"]

    archive = zipfile.ZipFile(io.BytesIO(response.content))
    assert archive.testzip() is None
    assert set(archive.namelist()) == {n for n, _ in SAMPLE_FILES} | {"manifest.json"}


def test_first_round_failure_triggers_retry(client):
    job = wait_for_state(client, submit(client, SAMPLE_FILES, first_fail="true")["job_id"])
    assert job["state"] == "succeeded"
    assert job["winner_round"] == 2
    assert [(a["round"], a["state"]) for a in job["attempts"]] == [
        (1, "failed"),
        (2, "succeeded"),
    ]


def test_both_rounds_failing_fails_job(client):
    job = wait_for_state(
        client,
        submit(client, SAMPLE_FILES, first_fail="true", retry_fail="true")["job_id"],
    )
    assert job["state"] == "failed"
    assert job["artifact"] is None
    assert "round 1" in job["detail"] and "round 2" in job["detail"]
    assert client.get(f"/jobs/{job['job_id']}/artifact").status_code == 404


def test_retry_failing_while_first_round_still_running_waits_then_wins(client):
    job = wait_for_state(
        client,
        submit(
            client,
            SAMPLE_FILES,
            first_delay_seconds=2.0,
            retry_delay_seconds=0.1,
            first_wait_seconds=0.5,
            retry_fail="true",
        )["job_id"],
        timeout=20,
    )
    assert job["state"] == "succeeded"
    assert job["winner_round"] == 1
    assert [(a["round"], a["state"]) for a in job["attempts"]] == [
        (1, "succeeded"),
        (2, "failed"),
    ]


def test_unknown_job_is_404(client):
    assert client.get("/jobs/nope").status_code == 404
    assert client.get("/jobs/nope/artifact").status_code == 404
    assert client.get("/jobs/nope/winner").status_code == 404


def test_reject_path_traversal(client):
    response = client.post(
        "/jobs", files=[("files", ("../evil.txt", b"x", "text/plain"))]
    )
    assert response.status_code == 400


def test_reject_duplicate_names(client):
    response = client.post(
        "/jobs",
        files=[
            ("files", ("a.txt", b"1", "text/plain")),
            ("files", ("a.txt", b"2", "text/plain")),
        ],
    )
    assert response.status_code == 400


def test_reject_file_directory_conflict(client):
    response = client.post(
        "/jobs",
        files=[
            ("files", ("a", b"1", "text/plain")),
            ("files", ("a/b.txt", b"2", "text/plain")),
        ],
    )
    assert response.status_code == 400
    assert "conflicts" in response.json()["detail"]


def test_dotfiles_are_allowed(client):
    job = wait_for_state(
        client, submit(client, [(".gitignore", b"data/\n")])["job_id"]
    )
    assert job["state"] == "succeeded"
    response = client.get(f"/jobs/{job['job_id']}/artifact")
    archive = zipfile.ZipFile(io.BytesIO(response.content))
    assert archive.read(".gitignore") == b"data/\n"


def test_reject_empty_and_reserved(client):
    assert client.post("/jobs", files=[]).status_code == 422
    response = client.post(
        "/jobs", files=[("files", ("manifest.json", b"{}", "application/json"))]
    )
    assert response.status_code == 400
    response = client.post("/jobs", files=[("files", ("empty.txt", b"", "text/plain"))])
    assert response.status_code == 400


def test_reject_oversized_file(tmp_path, monkeypatch):
    os.environ["CI_PACKAGER_DATA_DIR"] = str(tmp_path / "data")
    monkeypatch.setenv("CI_PACKAGER_MAX_FILE_BYTES", "4")
    from fastapi.testclient import TestClient

    from app.config import Settings
    from app.main import create_app

    with TestClient(create_app(Settings.from_env())) as small_client:
        response = small_client.post(
            "/jobs", files=[("files", ("big.bin", b"x" * 8, "application/octet-stream"))]
        )
        assert response.status_code == 413


def test_worker_cannot_read_its_own_name_as_python_module(client):
    """Submissions are never on sys.path; an imported module name can't shadow."""
    job = wait_for_state(
        client,
        submit(client, [("json.py", b"raise SystemExit('hijacked')\n")])["job_id"],
    )
    assert job["state"] == "succeeded"
