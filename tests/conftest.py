"""Shared pytest fixtures for the packager test-suite."""

from __future__ import annotations

import io
import time
from pathlib import Path

import pytest


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CI_PACKAGER_DATA_DIR", str(tmp_path / "data"))
    from fastapi.testclient import TestClient

    from app.config import Settings
    from app.main import create_app

    app = create_app(Settings.from_env())
    with TestClient(app) as test_client:
        yield test_client


def submit(client, files, **options) -> dict:
    payload = [
        ("files", (name, io.BytesIO(data), "application/octet-stream"))
        for name, data in files
    ]
    form = {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in options.items()}
    response = client.post("/jobs", files=payload, data=form)
    assert response.status_code == 202, response.text
    return response.json()


def wait_for_state(client, job_id: str, timeout: float = 30.0, poll: float = 0.05) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if job["state"] != "running":
            return job
        time.sleep(poll)
    raise AssertionError(f"job {job_id} did not settle within {timeout}s")


SAMPLE_FILES = [
    ("hello.txt", b"hello world\n"),
    ("src/main.py", b"print('hi')\n"),
    ("docs/readme.md", b"# sample\n"),
]
