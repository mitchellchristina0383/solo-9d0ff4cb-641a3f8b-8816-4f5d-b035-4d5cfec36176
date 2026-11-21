"""SQLite-backed job/attempt/artifact registry.

A single connection guarded by a re-entrant lock is used because every
operation is short.  Writers use ``BEGIN IMMEDIATE`` so that the "publish the
first valid ZIP" decision is serialised across executor threads, rounds and
API requests.  SQLite in WAL mode with ``synchronous=FULL`` plus committed
transactions gives durability across restarts.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from .models import (
    ArtifactView,
    AttemptInfo,
    AttemptState,
    JobOptions,
    JobState,
    JobView,
)
from .utils import iso, parse_iso, utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id        TEXT PRIMARY KEY,
    state         TEXT NOT NULL,
    detail        TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    options_json  TEXT NOT NULL,
    winner_round  INTEGER,
    winner_sha256 TEXT
);

CREATE TABLE IF NOT EXISTS attempts (
    job_id      TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    round       INTEGER NOT NULL CHECK (round BETWEEN 1 AND 2),
    state       TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    error       TEXT,
    zip_sha256  TEXT,
    PRIMARY KEY (job_id, round)
);

CREATE TABLE IF NOT EXISTS artifacts (
    job_id       TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
    round        INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    size         INTEGER NOT NULL,
    file_count   INTEGER NOT NULL,
    zip_name     TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    published_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS jobs_created_idx ON jobs(created_at DESC);
"""


class Storage:
    def __init__(self, db_path: Path) -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None, timeout=30
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- primitives ----------------------------------------------------
    def _rows(self, sql: str, *args: object) -> list[sqlite3.Row]:
        return list(self._conn.execute(sql, args))

    def _row(self, sql: str, *args: object) -> Optional[sqlite3.Row]:
        return self._conn.execute(sql, args).fetchone()

    def _begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    # -- writes --------------------------------------------------------
    def create_job(
        self, job_id: str, options: JobOptions, first_started_at: datetime
    ) -> None:
        now = iso(utcnow())
        with self._lock:
            self._begin()
            try:
                self._conn.execute(
                    "INSERT INTO jobs (job_id, state, detail, created_at, updated_at,"
                    " options_json) VALUES (?, ?, NULL, ?, ?, ?)",
                    (
                        job_id,
                        JobState.RUNNING.value,
                        now,
                        now,
                        options.model_dump_json(),
                    ),
                )
                self._conn.execute(
                    "INSERT INTO attempts (job_id, round, state, started_at)"
                    " VALUES (?, 1, ?, ?)",
                    (job_id, AttemptState.RUNNING.value, iso(first_started_at)),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def begin_attempt(self, job_id: str, round_no: int) -> bool:
        """Create the retry attempt row if the job is still undecided."""
        now = iso(utcnow())
        with self._lock:
            self._begin()
            try:
                job = self._row("SELECT state FROM jobs WHERE job_id = ?", job_id)
                if job is None or job["state"] != JobState.RUNNING.value:
                    self._conn.execute("ROLLBACK")
                    return False
                if self._row(
                    "SELECT 1 FROM attempts WHERE job_id = ? AND round = ?",
                    job_id,
                    round_no,
                ):
                    self._conn.execute("ROLLBACK")
                    return False
                self._conn.execute(
                    "INSERT INTO attempts (job_id, round, state, started_at)"
                    " VALUES (?, ?, ?, ?)",
                    (job_id, round_no, AttemptState.RUNNING.value, now),
                )
                self._conn.execute(
                    "UPDATE jobs SET updated_at = ? WHERE job_id = ?", (now, job_id)
                )
                self._conn.execute("COMMIT")
                return True
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def finish_attempt(
        self,
        job_id: str,
        round_no: int,
        *,
        error: Optional[str] = None,
        zip_sha256: Optional[str] = None,
        zip_name: Optional[str] = None,
        size: int = 0,
        file_count: int = 0,
        zip_created_at: Optional[datetime] = None,
    ) -> str:
        """Record an attempt result atomically.

        Returns ``published``, ``superseded``, ``failed`` or ``ignored``.
        The first accepted ZIP becomes the job's artifact; every later valid
        result is marked ``superseded`` and can never replace it.
        """
        now_dt = utcnow()
        now = iso(now_dt)
        with self._lock:
            self._begin()
            try:
                job = self._row("SELECT * FROM jobs WHERE job_id = ?", job_id)
                if job is None:
                    self._conn.execute("ROLLBACK")
                    return "ignored"
                attempt = self._row(
                    "SELECT state FROM attempts WHERE job_id = ? AND round = ?",
                    job_id,
                    round_no,
                )
                if attempt is None or attempt["state"] != AttemptState.RUNNING.value:
                    # Already interrupted by shutdown or stopped by a guard.
                    self._conn.execute("ROLLBACK")
                    return "ignored"

                if zip_sha256 is not None:
                    if (
                        job["state"] == JobState.RUNNING.value
                        and job["winner_sha256"] is None
                    ):
                        assert zip_name is not None
                        self._conn.execute(
                            "INSERT INTO artifacts (job_id, round, sha256, size,"
                            " file_count, zip_name, created_at, published_at)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                job_id,
                                round_no,
                                zip_sha256,
                                size,
                                file_count,
                                zip_name,
                                iso(zip_created_at or now_dt),
                                now,
                            ),
                        )
                        self._conn.execute(
                            "UPDATE jobs SET state = ?, detail = NULL,"
                            " winner_round = ?, winner_sha256 = ?, updated_at = ?"
                            " WHERE job_id = ?",
                            (
                                JobState.SUCCEEDED.value,
                                round_no,
                                zip_sha256,
                                now,
                                job_id,
                            ),
                        )
                        self._conn.execute(
                            "UPDATE attempts SET state = ?, finished_at = ?,"
                            " zip_sha256 = ?, error = NULL"
                            " WHERE job_id = ? AND round = ?",
                            (
                                AttemptState.SUCCEEDED.value,
                                now,
                                zip_sha256,
                                job_id,
                                round_no,
                            ),
                        )
                        outcome = "published"
                    else:
                        # Valid but late: the job already has a winner.
                        self._conn.execute(
                            "UPDATE attempts SET state = ?, finished_at = ?,"
                            " zip_sha256 = ?, error = NULL"
                            " WHERE job_id = ? AND round = ?",
                            (
                                AttemptState.SUPERSEDED.value,
                                now,
                                zip_sha256,
                                job_id,
                                round_no,
                            ),
                        )
                        outcome = "superseded"
                else:
                    self._conn.execute(
                        "UPDATE attempts SET state = ?, finished_at = ?, error = ?"
                        " WHERE job_id = ? AND round = ?",
                        (AttemptState.FAILED.value, now, error, job_id, round_no),
                    )
                    outcome = "failed"
                    self._maybe_fail_job(job_id, now)

                self._conn.execute("COMMIT")
                return outcome
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def _maybe_fail_job(self, job_id: str, now: str) -> None:
        """Fail the job only once both rounds are terminal and nothing won."""
        job = self._row("SELECT state FROM jobs WHERE job_id = ?", job_id)
        if job is None or job["state"] != JobState.RUNNING.value:
            return
        attempts = self._rows(
            "SELECT round, state, error FROM attempts WHERE job_id = ?", job_id
        )
        if len(attempts) < 2:
            return
        if any(a["state"] == AttemptState.RUNNING.value for a in attempts):
            return
        reasons = [
            f"round {a['round']}: {a['error'] or 'failed'}"
            for a in sorted(attempts, key=lambda a: a["round"])
        ]
        self._conn.execute(
            "UPDATE jobs SET state = ?, detail = ?, updated_at = ? WHERE job_id = ?",
            (JobState.FAILED.value, "; ".join(reasons), now, job_id),
        )

    def stop_running(self, job_id: str, error: str) -> None:
        """Stop attempts that exceeded the job runtime limit.

        A job that already published keeps its artifact; its stragglers are
        marked superseded.  An undecided job is failed outright: the hard
        runtime cap is terminal and must never leave a job ``running`` forever.
        """
        now = iso(utcnow())
        with self._lock:
            self._begin()
            try:
                job = self._row("SELECT state FROM jobs WHERE job_id = ?", job_id)
                if job is None:
                    self._conn.execute("ROLLBACK")
                    return
                if job["state"] == JobState.SUCCEEDED.value:
                    self._conn.execute(
                        "UPDATE attempts SET state = ?, finished_at = ?, error = ?"
                        " WHERE job_id = ? AND state = ?",
                        (
                            AttemptState.SUPERSEDED.value,
                            now,
                            error,
                            job_id,
                            AttemptState.RUNNING.value,
                        ),
                    )
                else:
                    self._conn.execute(
                        "UPDATE attempts SET state = ?, finished_at = ?, error = ?"
                        " WHERE job_id = ? AND state = ?",
                        (
                            AttemptState.FAILED.value,
                            now,
                            error,
                            job_id,
                            AttemptState.RUNNING.value,
                        ),
                    )
                    if job["state"] == JobState.RUNNING.value:
                        self._conn.execute(
                            "UPDATE jobs SET state = ?, detail = ?, updated_at = ?"
                            " WHERE job_id = ?",
                            (JobState.FAILED.value, error, now, job_id),
                        )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def mark_interrupted(self) -> int:
        """Reconcile in-flight work after a restart.  Idempotent.

        * jobs still ``running`` become ``interrupted``;
        * their running attempts become ``interrupted``;
        * a running attempt belonging to an already-published job becomes
          ``superseded`` (it was a straggler that the restart killed).
        """
        now = iso(utcnow())
        with self._lock:
            self._begin()
            try:
                pending = self._rows(
                    "SELECT job_id FROM jobs WHERE state = ?",
                    JobState.RUNNING.value,
                )
                self._conn.execute(
                    "UPDATE attempts SET state = ?, finished_at = ?, error = ?"
                    " WHERE state = ? AND job_id IN"
                    " (SELECT job_id FROM jobs WHERE state = ?)",
                    (
                        AttemptState.INTERRUPTED.value,
                        now,
                        "service restarted while the attempt was running",
                        AttemptState.RUNNING.value,
                        JobState.RUNNING.value,
                    ),
                )
                self._conn.execute(
                    "UPDATE attempts SET state = ?, finished_at = ?, error = ?"
                    " WHERE state = ? AND job_id IN"
                    " (SELECT job_id FROM jobs WHERE state = ?)",
                    (
                        AttemptState.SUPERSEDED.value,
                        now,
                        "service restarted after another result was published",
                        AttemptState.RUNNING.value,
                        JobState.SUCCEEDED.value,
                    ),
                )
                self._conn.execute(
                    "UPDATE jobs SET state = ?, detail = ?, updated_at = ?"
                    " WHERE state = ?",
                    (
                        JobState.INTERRUPTED.value,
                        "service restarted while the job was in flight",
                        now,
                        JobState.RUNNING.value,
                    ),
                )
                self._conn.execute("COMMIT")
                return len(pending)
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # -- reads ---------------------------------------------------------
    def count_jobs(self) -> int:
        with self._lock:
            row = self._row("SELECT COUNT(*) AS n FROM jobs")
            return int(row["n"]) if row else 0

    def count_artifacts(self) -> int:
        with self._lock:
            row = self._row("SELECT COUNT(*) AS n FROM artifacts")
            return int(row["n"]) if row else 0

    def all_artifacts(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._rows("SELECT * FROM artifacts ORDER BY published_at, job_id")

    def get_artifact(self, job_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._row("SELECT * FROM artifacts WHERE job_id = ?", job_id)

    def get_job(self, job_id: str) -> Optional[JobView]:
        with self._lock:
            return self._job_view(job_id)

    def list_jobs(self, limit: int, offset: int) -> tuple[list[JobView], int]:
        with self._lock:
            total = self.count_jobs()
            rows = self._rows(
                "SELECT job_id FROM jobs ORDER BY created_at DESC, job_id DESC"
                " LIMIT ? OFFSET ?",
                limit,
                offset,
            )
            views = [v for v in (self._job_view(r["job_id"]) for r in rows) if v]
            return views, total

    def _job_view(self, job_id: str) -> Optional[JobView]:
        job = self._row("SELECT * FROM jobs WHERE job_id = ?", job_id)
        if job is None:
            return None
        attempts = [
            AttemptInfo(
                round=a["round"],
                state=AttemptState(a["state"]),
                started_at=parse_iso(a["started_at"]) if a["started_at"] else None,
                finished_at=parse_iso(a["finished_at"]) if a["finished_at"] else None,
                error=a["error"],
                zip_sha256=a["zip_sha256"],
            )
            for a in self._rows(
                "SELECT * FROM attempts WHERE job_id = ? ORDER BY round", job_id
            )
        ]
        artifact_row = self._row("SELECT * FROM artifacts WHERE job_id = ?", job_id)
        artifact = None
        if artifact_row is not None:
            artifact = ArtifactView(
                sha256=artifact_row["sha256"],
                size=artifact_row["size"],
                file_count=artifact_row["file_count"],
                round=artifact_row["round"],
                created_at=parse_iso(artifact_row["created_at"]),
                published_at=parse_iso(artifact_row["published_at"]),
                download_url=f"/jobs/{job_id}/artifact",
            )
        return JobView(
            job_id=job["job_id"],
            state=JobState(job["state"]),
            detail=job["detail"],
            created_at=parse_iso(job["created_at"]),
            updated_at=parse_iso(job["updated_at"]),
            options=JobOptions.model_validate_json(job["options_json"]),
            attempts=attempts,
            winner_round=job["winner_round"],
            artifact=artifact,
        )
