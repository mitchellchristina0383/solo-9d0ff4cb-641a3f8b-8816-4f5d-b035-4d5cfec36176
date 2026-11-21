"""Storage-level unit tests for terminal-state rules."""

from __future__ import annotations

from datetime import datetime, timezone

from app.models import JobOptions
from app.storage import Storage


def _storage(tmp_path) -> Storage:
    storage = Storage(tmp_path / "db.sqlite3")
    storage.create_job("job", JobOptions(first_wait_seconds=0.1, max_runtime_seconds=5), datetime.now(timezone.utc))
    return storage


def test_stop_running_fails_undecided_job(tmp_path):
    storage = _storage(tmp_path)
    storage.stop_running("job", "runtime limit")
    view = storage.get_job("job")
    assert view is not None
    assert view.state.value == "failed"
    assert view.attempts[0].state.value == "failed"
    # A late executor result for the stopped attempt is ignored.
    assert storage.finish_attempt("job", 1, zip_sha256="h", zip_name="z.zip") == "ignored"
    assert storage.get_artifact("job") is None
    storage.close()


def test_stop_running_keeps_published_artifact(tmp_path):
    storage = _storage(tmp_path)
    assert storage.finish_attempt("job", 1, zip_sha256="h", zip_name="z.zip") == "published"
    storage.stop_running("job", "runtime limit")
    view = storage.get_job("job")
    assert view is not None
    assert view.state.value == "succeeded"
    assert view.attempts[0].state.value == "succeeded"
    storage.close()


def test_retry_row_is_created_at_most_once(tmp_path):
    storage = _storage(tmp_path)
    assert storage.begin_attempt("job", 2) is True
    assert storage.begin_attempt("job", 2) is False
    assert len(storage.get_job("job").attempts) == 2
    storage.close()


def test_no_retry_after_publication(tmp_path):
    storage = _storage(tmp_path)
    assert storage.finish_attempt("job", 1, zip_sha256="h", zip_name="z.zip") == "published"
    assert storage.begin_attempt("job", 2) is False
    storage.close()


def test_job_fails_only_after_both_rounds(tmp_path):
    storage = _storage(tmp_path)
    assert storage.finish_attempt("job", 1, error="boom") == "failed"
    assert storage.get_job("job").state.value == "running"
    assert storage.begin_attempt("job", 2) is True
    assert storage.finish_attempt("job", 2, error="boom") == "failed"
    view = storage.get_job("job")
    assert view.state.value == "failed"
    assert "round 1: boom" in (view.detail or "")
    storage.close()


def test_mark_interrupted_is_idempotent(tmp_path):
    storage = _storage(tmp_path)
    assert storage.mark_interrupted() == 1
    assert storage.mark_interrupted() == 0
    assert storage.get_job("job").state.value == "interrupted"
    storage.close()
