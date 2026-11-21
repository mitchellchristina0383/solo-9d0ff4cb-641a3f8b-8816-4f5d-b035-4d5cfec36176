"""Executor/build-level unit tests (no HTTP, no FastAPI)."""

from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest

from app.executor import ExecutorError, build_zip, run, verify_zip


def _submission(tmp_path: Path, files: dict[str, bytes]) -> Path:
    root = tmp_path / "sub"
    root.mkdir()
    for name, data in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    return root


def test_build_zip_is_deterministic(tmp_path):
    sub = _submission(tmp_path, {"b.txt": b"two", "a.txt": b"one"})
    first = build_zip(sub, tmp_path / "one.zip", "job", 1)
    second = build_zip(sub, tmp_path / "two.zip", "job", 1)
    assert first["sha256"] == second["sha256"]
    assert (tmp_path / "one.zip").read_bytes() == (tmp_path / "two.zip").read_bytes()


def test_manifest_lists_hashes(tmp_path):
    sub = _submission(tmp_path, {"dir/x.txt": b"payload"})
    build_zip(sub, tmp_path / "out.zip", "job", 2)
    with zipfile.ZipFile(tmp_path / "out.zip") as zf:
        manifest = json.loads(zf.read("manifest.json"))
        payload = zf.read("dir/x.txt")
    assert manifest["round"] == 2
    assert manifest["files"][0]["name"] == "dir/x.txt"
    assert manifest["files"][0]["size"] == len(payload)
    assert manifest["files"][0]["sha256"] == hashlib.sha256(payload).hexdigest()


def test_corrupt_zip_detected(tmp_path):
    good = tmp_path / "good.zip"
    sub = _submission(tmp_path, {"a.txt": b"hello"})
    build_zip(sub, good, "job", 1)
    data = bytearray(good.read_bytes())
    data[30] ^= 0xFF  # flip a byte inside the first local file header
    bad = tmp_path / "bad.zip"
    bad.write_bytes(bytes(data))
    with pytest.raises(Exception):
        verify_zip(bad)


def test_reserved_manifest_and_empty_submission(tmp_path):
    sub = _submission(tmp_path, {"manifest.json": b"{}"})
    with pytest.raises(ExecutorError):
        build_zip(sub, tmp_path / "x.zip", "job", 1)
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ExecutorError):
        build_zip(empty, tmp_path / "y.zip", "job", 1)


def test_run_writes_result_and_honours_fail(tmp_path):
    sub = _submission(tmp_path, {"a.txt": b"hello"})
    request = tmp_path / "req.json"
    request.write_text(
        json.dumps(
            {
                "job_id": "j",
                "round": 1,
                "submission_dir": str(sub),
                "zip_path": str(tmp_path / "z.zip"),
                "result_path": str(tmp_path / "r.json"),
                "delay_seconds": 0,
                "fail": True,
            }
        )
    )
    assert run(request) == 1
    result = json.loads((tmp_path / "r.json").read_text())
    assert result["ok"] is False and "simulated" in result["error"]
    assert not (tmp_path / "z.zip").exists()
