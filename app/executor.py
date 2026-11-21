"""Python ZIP executor.

Runs as a *separate process* (``python -m app.executor <request.json>``) so a
crash or a simulated failure cannot take the API down.  It reads a JSON
request, optionally sleeps (to emulate a slow CI round), builds a deterministic
ZIP containing the submitted files plus a ``manifest.json``, verifies the ZIP
by reopening it, and writes a JSON result atomically next to the artifact.

The executor never decides whether a result is published; it only produces and
self-checks the archive.  The orchestrator commits the result through storage.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

MANIFEST_NAME = "manifest.json"


class ExecutorError(RuntimeError):
    pass


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _dir_fsync(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _collect_files(submission_dir: Path) -> list[Path]:
    files = sorted(p for p in submission_dir.rglob("*") if p.is_file())
    if not files:
        raise ExecutorError("submission contains no files")
    return files


def build_zip(submission_dir: Path, out_zip: Path, job_id: str, round_no: int) -> dict[str, Any]:
    files = _collect_files(submission_dir)
    manifest = {
        "job_id": job_id,
        "round": round_no,
        "files": [],
    }
    entries: list[tuple[str, bytes, int]] = []
    for path in files:
        rel = path.relative_to(submission_dir).as_posix()
        if rel == MANIFEST_NAME:
            raise ExecutorError(f"reserved file name in submission: {rel}")
        data = path.read_bytes()
        entries.append((rel, data, 0o644))
        manifest["files"].append(
            {
                "name": rel,
                "size": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")

    tmp_zip = out_zip.with_name(out_zip.name + ".partial")
    try:
        with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for rel, data, mode in entries:
                info = zipfile.ZipInfo(rel, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = (0o100000 | mode) << 16
                zf.writestr(info, data)
            info = zipfile.ZipInfo(MANIFEST_NAME, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o100000 | 0o644) << 16
            zf.writestr(info, manifest_bytes)
        verify_zip(tmp_zip)
        with tmp_zip.open("rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp_zip, out_zip)
        _dir_fsync(out_zip.parent)
    finally:
        tmp_zip.unlink(missing_ok=True)

    digest = hashlib.sha256(out_zip.read_bytes()).hexdigest()
    return {
        "ok": True,
        "zip_path": str(out_zip),
        "sha256": digest,
        "size": out_zip.stat().st_size,
        "file_count": len(entries),
    }


def verify_zip(path: Path) -> None:
    """Reopen and CRC-check every member; also check the manifest."""
    with zipfile.ZipFile(path) as zf:
        bad = zf.testzip()
        if bad is not None:
            raise ExecutorError(f"corrupt zip member: {bad}")
        names = zf.namelist()
        if len(names) != len(set(names)):
            raise ExecutorError("duplicate zip member names")
        if MANIFEST_NAME not in names:
            raise ExecutorError("manifest.json missing from zip")
        manifest = json.loads(zf.read(MANIFEST_NAME))
        listed = {entry["name"] for entry in manifest["files"]}
        actual = set(names) - {MANIFEST_NAME}
        if listed != actual:
            raise ExecutorError("manifest does not match zip contents")
        for entry in manifest["files"]:
            if hashlib.sha256(zf.read(entry["name"])).hexdigest() != entry["sha256"]:
                raise ExecutorError(f"manifest hash mismatch for {entry['name']}")
        if path.stat().st_size == 0:
            raise ExecutorError("empty zip")


def run(request_path: Path) -> int:
    request = json.loads(request_path.read_text(encoding="utf-8"))
    result_path = Path(request["result_path"])
    out_zip = Path(request["zip_path"])
    payload: dict[str, Any] = {"ok": False, "error": None}
    try:
        delay = float(request.get("delay_seconds", 0))
        if delay > 0:
            time.sleep(delay)
        if request.get("fail"):
            raise ExecutorError(
                request.get("fail_message") or "simulated executor failure"
            )
        payload = build_zip(
            Path(request["submission_dir"]),
            out_zip,
            str(request["job_id"]),
            int(request["round"]),
        )
    except ExecutorError as exc:
        payload = {"ok": False, "error": str(exc)}
    except Exception as exc:  # pragma: no cover - defensive
        payload = {"ok": False, "error": f"executor crashed: {exc}"}
    _write_json_atomic(result_path, payload)
    return 0 if payload.get("ok") else 1


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python -m app.executor <request.json>", file=sys.stderr)
        return 2
    return run(Path(argv[1]))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
