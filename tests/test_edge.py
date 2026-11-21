"""Extra edge cases: runtime cap, concurrent submissions, artifacts listing."""

from __future__ import annotations

import concurrent.futures
import io
import zipfile

from conftest import SAMPLE_FILES, submit, wait_for_state


def test_runtime_cap_fails_undecided_job(client):
    job = wait_for_state(
        client,
        submit(
            client,
            SAMPLE_FILES,
            first_delay_seconds=30,
            retry_delay_seconds=30,
            first_wait_seconds=0.2,
            max_runtime_seconds=1.5,
        )["job_id"],
        timeout=20,
    )
    assert job["state"] == "failed"
    assert "runtime limit" in job["detail"]
    assert all(a["state"] == "failed" for a in job["attempts"])
    assert client.get(f"/jobs/{job['job_id']}/artifact").status_code == 404


def test_concurrent_jobs_publish_independently(client):
    def one(index: int) -> dict:
        job = wait_for_state(
            client,
            submit(
                client,
                [("f.txt", f"job-{index}\n".encode())],
                first_delay_seconds=0.05 * index,
            )["job_id"],
        )
        return job

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        jobs = list(pool.map(one, range(5)))

    assert {j["state"] for j in jobs} == {"succeeded"}
    shas = {j["artifact"]["sha256"] for j in jobs}
    assert len(shas) == 5

    listing = client.get("/jobs", params={"limit": 100}).json()
    assert listing["total"] >= 5
    winners = client.get("/winners").json()
    assert len(winners) >= 5
    for winner in winners:
        assert client.get(winner["download_url"]).status_code == 200


def test_zip_contains_submitted_content(client):
    files = [("data/values.csv", b"a,b\n1,2\n"), ("notes.txt", b"hello\n")]
    job = wait_for_state(client, submit(client, files)["job_id"])
    response = client.get(f"/jobs/{job['job_id']}/artifact")
    archive = zipfile.ZipFile(io.BytesIO(response.content))
    for name, data in files:
        assert archive.read(name) == data
    assert job["artifact"]["file_count"] == len(files)


def test_options_roundtrip(client):
    job = wait_for_state(
        client,
        submit(
            client,
            SAMPLE_FILES,
            first_delay_seconds="0.1",
            retry_delay_seconds="0.2",
            first_wait_seconds="3",
        )["job_id"],
    )
    options = job["options"]
    assert options["first_delay_seconds"] == 0.1
    assert options["retry_delay_seconds"] == 0.2
    assert options["first_wait_seconds"] == 3


def test_invalid_options_rejected(client):
    response = client.post(
        "/jobs",
        files=[("files", ("a.txt", b"x", "text/plain"))],
        data={"max_runtime_seconds": "1", "first_wait_seconds": "10"},
    )
    assert response.status_code == 422
