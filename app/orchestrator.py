"""Job orchestration: submissions, the first-round/retry race, publication.

Lifecycle of one job (all times driven by ``JobOptions``):

1. Round 1 starts immediately in a worker thread / child process.
2. A deadline thread waits ``first_wait_seconds``.  If round 1 has not produced
   a result by then, one retry (round 2) is started **while round 1 keeps
   running**.  If round 1 did finish before the deadline, no retry happens --
   unless round 1 *failed*, in which case the retry is started immediately.
3. Both rounds are allowed to finish.  Whichever result reaches
   ``Storage.finish_attempt`` first and validates becomes the published ZIP;
   any later result is recorded as ``superseded`` and can never replace it.
4. The job fails only if both rounds failed (or were stopped by the runtime
   guard).  Interrupts from a restart are handled by ``Storage.mark_interrupted``
   at startup; late executor results for interrupted attempts are ignored.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from .config import Settings
from .executor import verify_zip
from .models import AttemptState, JobOptions, JobState
from .storage import Storage
from .utils import sha256_file, utcnow

log = logging.getLogger("ci_packager.orchestrator")


class SubmissionError(ValueError):
    """Client-side validation failure; maps to HTTP 400."""


class Orchestrator:
    def __init__(self, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self._stop = threading.Event()
        self._threads: set[threading.Thread] = set()
        self._threads_lock = threading.Lock()
        # job_id -> {"wait": Timer, "runtime": Timer}
        self._timers: dict[str, dict[str, threading.Timer]] = {}
        self._timers_lock = threading.Lock()
        self._procs: dict[tuple[str, int], subprocess.Popen[bytes]] = {}
        self._procs_lock = threading.Lock()

    # -- lifecycle -----------------------------------------------------
    def start(self) -> None:
        self._stop.clear()

    def shutdown(self, timeout: float = 10.0) -> None:
        """Stop workers and child executors; mark in-flight work interrupted."""
        self._stop.set()
        with self._timers_lock:
            timers = [t for group in self._timers.values() for t in group.values()]
            self._timers.clear()
        for timer in timers:
            timer.cancel()
        with self._procs_lock:
            procs = list(self._procs.values())
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
        deadline = time.monotonic() + timeout
        for proc in procs:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:  # pragma: no cover
                proc.kill()
        with self._threads_lock:
            threads = list(self._threads)
        for thread in threads:
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)
        self.storage.mark_interrupted()

    # -- submission ----------------------------------------------------
    def submit(
        self,
        files: list[tuple[str, bytes]],
        options: JobOptions,
    ) -> str:
        self._validate_submission(files)
        job_id = uuid.uuid4().hex
        submission_dir = self.settings.submissions_dir / job_id
        submission_dir.mkdir(parents=True, exist_ok=False)
        try:
            for name, data in files:
                target = submission_dir / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                fsync_file(target)
            fsync_dir(submission_dir)
            self.storage.create_job(job_id, options, utcnow())
        except BaseException:
            shutil.rmtree(submission_dir, ignore_errors=True)
            raise

        job_dir = self.settings.artifacts_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=True)

        self._spawn(
            self._round_worker, job_id, 1, submission_dir, job_dir, options
        )
        timer = threading.Timer(
            options.first_wait_seconds,
            self._first_round_deadline,
            args=(job_id, submission_dir, job_dir, options),
        )
        timer.daemon = True
        runtime_timer = threading.Timer(
            options.max_runtime_seconds,
            self._runtime_deadline,
            args=(job_id,),
        )
        runtime_timer.daemon = True
        with self._timers_lock:
            self._timers[job_id] = {"wait": timer, "runtime": runtime_timer}
        timer.start()
        runtime_timer.start()
        return job_id

    def _disarm_timer(self, job_id: str, which: Optional[str] = None) -> None:
        """Cancel the first-wait timer, the runtime timer, or (None) both.

        The runtime guard stays armed after publication so that a straggler can
        still be killed and recorded as superseded.
        """
        with self._timers_lock:
            group = self._timers.get(job_id)
            if group is None:
                return
            if which is None:
                timers = list(group.values())
                self._timers.pop(job_id, None)
            else:
                timer = group.pop(which, None)
                timers = [timer] if timer is not None else []
                if not group:
                    self._timers.pop(job_id, None)
        for timer in timers:
            timer.cancel()

    def _validate_submission(self, files: list[tuple[str, bytes]]) -> None:
        if not files:
            raise SubmissionError("at least one file is required")
        if len(files) > self.settings.max_files:
            raise SubmissionError(
                f"too many files ({len(files)} > {self.settings.max_files})"
            )
        total = 0
        seen: set[str] = set()
        for name, data in files:
            self._validate_name(name)
            if name in seen:
                raise SubmissionError(f"duplicate file name: {name}")
            seen.add(name)
            if len(data) > self.settings.max_file_bytes:
                raise SubmissionError(
                    f"file {name} exceeds {self.settings.max_file_bytes} bytes"
                )
            if not data:
                raise SubmissionError(f"file {name} is empty")
            total += len(data)
        for name in seen:
            # "a" as a file and "a/b" as another would collide on disk.
            parts = name.split("/")
            for i in range(1, len(parts)):
                prefix = "/".join(parts[:i])
                if prefix in seen:
                    raise SubmissionError(
                        f"file {prefix!r} conflicts with directory of {name!r}"
                    )
        if total > self.settings.max_total_bytes:
            raise SubmissionError(
                f"submission exceeds {self.settings.max_total_bytes} bytes"
            )

    @staticmethod
    def _validate_name(name: str) -> None:
        if not name or len(name) > 200:
            raise SubmissionError("file names must be 1-200 characters")
        if name.startswith("/") or "\\" in name:
            raise SubmissionError(f"absolute or backslash path not allowed: {name}")
        parts = name.split("/")
        if any(part in ("", ".", "..") for part in parts):
            raise SubmissionError(f"invalid path segment in {name!r}")
        if name == "manifest.json":
            raise SubmissionError("manifest.json is reserved")

    # -- worker plumbing ----------------------------------------------
    def _spawn(self, target, *args) -> None:
        thread = threading.Thread(target=self._guard, args=(target, args), daemon=True)
        with self._threads_lock:
            self._threads.add(thread)
        thread.start()

    def _guard(self, target, args) -> None:
        try:
            target(*args)
        except Exception:  # pragma: no cover - defensive
            log.exception("worker crashed: %s", target.__name__)
        finally:
            with self._threads_lock:
                self._threads.discard(threading.current_thread())

    def _first_round_deadline(
        self, job_id: str, submission_dir: Path, job_dir: Path, options: JobOptions
    ) -> None:
        if self._stop.is_set():
            return
        self._disarm_timer(job_id, "wait")
        job = self.storage.get_job(job_id)
        if job is None or job.state != JobState.RUNNING:
            return
        first = next((a for a in job.attempts if a.round == 1), None)
        if first is not None and first.state == AttemptState.RUNNING:
            log.info("job %s: first round exceeded wait, starting retry", job_id)
            self._start_retry(job_id, submission_dir, job_dir, options)
        # else: round 1 already reported failure -> retry was started by worker

    def _start_retry(
        self, job_id: str, submission_dir: Path, job_dir: Path, options: JobOptions
    ) -> None:
        if not self.storage.begin_attempt(job_id, 2):
            return
        self._spawn(
            self._round_worker, job_id, 2, submission_dir, job_dir, options
        )

    def _runtime_deadline(self, job_id: str) -> None:
        if self._stop.is_set():
            return
        self._disarm_timer(job_id, "runtime")
        job = self.storage.get_job(job_id)
        if job is None:
            return
        if any(a.state == AttemptState.RUNNING for a in job.attempts):
            log.warning("job %s: runtime limit reached, stopping attempts", job_id)
            self.storage.stop_running(job_id, "attempt exceeded the job runtime limit")
            self._kill_procs(job_id)

    def _kill_procs(self, job_id: str) -> None:
        with self._procs_lock:
            procs = [(k, p) for k, p in self._procs.items() if k[0] == job_id]
        for key, proc in procs:
            if proc.poll() is None:
                proc.terminate()

    def _round_worker(
        self,
        job_id: str,
        round_no: int,
        submission_dir: Path,
        job_dir: Path,
        options: JobOptions,
    ) -> None:
        if self._stop.is_set():
            return
        delay = (
            options.first_delay_seconds
            if round_no == 1
            else options.retry_delay_seconds
        )
        should_fail = options.first_fail if round_no == 1 else options.retry_fail

        request_path = job_dir / f"request-round{round_no}.json"
        result_path = job_dir / f"result-round{round_no}.json"
        zip_path = job_dir / f"artifact-round{round_no}.zip"
        request = {
            "job_id": job_id,
            "round": round_no,
            "submission_dir": str(submission_dir),
            "zip_path": str(zip_path),
            "result_path": str(result_path),
            "delay_seconds": delay,
            "fail": should_fail,
            "fail_message": (
                "simulated failure of round 1"
                if round_no == 1
                else "simulated failure of round 2"
            ),
        }
        request_path.write_text(json.dumps(request), encoding="utf-8")

        try:
            rc = self._run_executor(request_path, job_id, round_no)
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("job %s round %s: executor invocation failed", job_id, round_no)
            self._record_failure(job_id, round_no, f"executor invocation failed: {exc}", options)
            return
        finally:
            request_path.unlink(missing_ok=True)

        if self._stop.is_set():
            return  # shutdown path handles interruption

        result: Optional[dict] = None
        if result_path.exists():
            try:
                result = json.loads(result_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                result = None
        result_path.unlink(missing_ok=True)

        if result is None:
            self._record_failure(
                job_id, round_no, f"executor exited with code {rc} and no result", options
            )
            return

        if not result.get("ok"):
            self._record_failure(
                job_id, round_no, str(result.get("error") or "executor failed"), options
            )
            return

        # Belt-and-braces: re-hash and re-verify before publishing.
        try:
            verify = self._verify_candidate(zip_path, result)
        except Exception as exc:
            self._record_failure(
                job_id, round_no, f"candidate ZIP failed verification: {exc}", options
            )
            return

        outcome = self.storage.finish_attempt(
            job_id,
            round_no,
            zip_sha256=verify["sha256"],
            zip_name=zip_path.name,
            size=verify["size"],
            file_count=verify["file_count"],
            zip_created_at=verify["created_at"],
        )
        log.info("job %s round %s: %s", job_id, round_no, outcome)
        if outcome in ("superseded", "ignored"):
            # The ZIP did not win (or the attempt was already settled); never
            # keep a candidate that could be confused with the published one.
            zip_path.unlink(missing_ok=True)
        elif outcome == "published":
            # No retry can be started now; the runtime guard stays armed to
            # stop a straggler from the other round.
            self._disarm_timer(job_id, "wait")

    def _record_failure(
        self, job_id: str, round_no: int, error: str, options: JobOptions
    ) -> None:
        outcome = self.storage.finish_attempt(job_id, round_no, error=error)
        if outcome != "failed":
            log.info("job %s round %s: failure %s", job_id, round_no, outcome)
            return
        if round_no == 1:
            # Round 1 is decided; start the retry right away (requirement: a
            # failed first round is retried once) and stand the wait timer down.
            self._disarm_timer(job_id, "wait")
            job = self.storage.get_job(job_id)
            if job is not None and job.state == JobState.RUNNING:
                job_dir = self.settings.artifacts_dir / job_id
                submission_dir = self.settings.submissions_dir / job_id
                self._start_retry(job_id, submission_dir, job_dir, options)
        else:
            # Round 2 is the last chance; nothing more to schedule.
            self._disarm_timer(job_id, "wait")

    def _run_executor(self, request_path: Path, job_id: str, round_no: int) -> int:
        # ``-P`` keeps the current directory (untrusted submissions live under
        # the data dir) off ``sys.path``; only the project root is importable.
        env = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(ROOT)}
        cmd = [sys.executable, "-P", "-m", "app.executor", str(request_path)]
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with self._procs_lock:
            self._procs[(job_id, round_no)] = proc
        try:
            return proc.wait()
        finally:
            with self._procs_lock:
                self._procs.pop((job_id, round_no), None)

    def _verify_candidate(self, zip_path: Path, result: dict) -> dict:
        if not zip_path.is_file() or zip_path.stat().st_size == 0:
            raise RuntimeError("zip missing or empty")
        digest = sha256_file(zip_path)
        if digest != result.get("sha256"):
            raise RuntimeError("zip hash does not match executor result")
        verify_zip(zip_path)
        return {
            "sha256": digest,
            "size": zip_path.stat().st_size,
            "file_count": int(result.get("file_count", 0)),
            "created_at": utcnow(),
        }

    # -- published artifact -------------------------------------------
    def artifact_path(self, job_id: str) -> Optional[Path]:
        row = self.storage.get_artifact(job_id)
        if row is None:
            return None
        path = self.settings.artifacts_dir / job_id / row["zip_name"]
        return path if path.is_file() else None

    def prune_orphan_zips(self) -> int:
        """Delete round ZIPs for jobs that never published (startup hygiene)."""
        removed = 0
        for job_dir in self.settings.artifacts_dir.glob("*"):
            if not job_dir.is_dir():
                continue
            if self.storage.get_artifact(job_dir.name) is None:
                for zip_path in job_dir.glob("artifact-round*.zip"):
                    zip_path.unlink(missing_ok=True)
                    removed += 1
        return removed


ROOT = Path(__file__).resolve().parent.parent


def fsync_file(path: Path) -> None:
    with path.open("rb") as fh:
        os.fsync(fh.fileno())


def fsync_dir(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
