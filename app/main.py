"""FastAPI entry point for the CI packaging service.

Run with ``uvicorn app.main:app``.  Interactive docs are available at
``/docs`` (and ``/redoc``).
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html
from pydantic import ValidationError

from . import __version__
from .config import Settings
from .models import (
    HealthView,
    JobList,
    JobOptions,
    JobView,
    WinnerRecord,
)
from .orchestrator import Orchestrator, SubmissionError
from .storage import Storage

log = logging.getLogger("ci_packager")

STATIC_DIR = Path(__file__).resolve().parent / "static"

DESCRIPTION = """
Submit a handful of files and let a Python executor build a verified ZIP.

* Round 1 starts immediately.
* If round 1 fails **or** exceeds `first_wait_seconds`, exactly one retry is
  started while a still-running round 1 keeps executing.
* The first ZIP that passes verification and is committed is published; later
  results can never replace it.
* The job only fails when both rounds fail.
* Published ZIPs and winner records survive a restart; jobs that were still
  running are marked `interrupted`.

Use `first_delay_seconds`, `retry_delay_seconds`, `first_fail` and
`retry_fail` to stage the race in `/docs`.
"""


def _settings() -> Settings:
    return Settings.from_env()


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or _settings()
    settings.ensure_dirs()
    storage = Storage(settings.db_path)
    orchestrator = Orchestrator(settings, storage)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Any job that was in flight when the previous process stopped is
        # interrupted; published artifacts stay downloadable.
        interrupted = storage.mark_interrupted()
        if interrupted:
            log.warning("marked %s in-flight job(s) interrupted", interrupted)
        removed = orchestrator.prune_orphan_zips()
        if removed:
            log.info("removed %s orphan round ZIP(s)", removed)
        orchestrator.start()
        app.state.settings = settings
        app.state.storage = storage
        app.state.orchestrator = orchestrator
        try:
            yield
        finally:
            orchestrator.shutdown()
            storage.close()

    app = FastAPI(
        title="CI Artifact Packager",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
    )
    app.state.settings = settings
    app.state.storage = storage
    app.state.orchestrator = orchestrator

    static_dir = STATIC_DIR
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/docs", include_in_schema=False)
    def swagger_ui():
        # Assets are vendored so /docs works without outbound network access.
        return get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=f"{app.title} - Swagger UI",
            swagger_js_url="/static/swagger-ui/swagger-ui-bundle.js",
            swagger_css_url="/static/swagger-ui/swagger-ui.css",
            swagger_favicon_url="/static/swagger-ui/favicon-32x32.png",
        )

    @app.get("/redoc", include_in_schema=False)
    def redoc_ui():
        # ReDoc's bundle is not vendored, so serve the offline Swagger UI here
        # as well; /docs remains the canonical interactive console.
        return get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=f"{app.title} - API docs",
            swagger_js_url="/static/swagger-ui/swagger-ui-bundle.js",
            swagger_css_url="/static/swagger-ui/swagger-ui.css",
            swagger_favicon_url="/static/swagger-ui/favicon-32x32.png",
        )

    @app.get("/healthz", response_model=HealthView, tags=["ops"])
    def healthz() -> HealthView:
        return HealthView(
            status="ok",
            version=__version__,
            jobs=storage.count_jobs(),
            artifacts=storage.count_artifacts(),
        )

    @app.post(
        "/jobs",
        response_model=JobView,
        status_code=202,
        tags=["packaging"],
        summary="Submit files and start packaging",
    )
    async def submit_job(
        files: Annotated[list[UploadFile], File(description="Small set of files")],
        first_delay_seconds: Annotated[float, Form(ge=0, le=300)] = 0.0,
        retry_delay_seconds: Annotated[float, Form(ge=0, le=300)] = 0.0,
        first_fail: Annotated[bool, Form()] = False,
        retry_fail: Annotated[bool, Form()] = False,
        first_wait_seconds: Annotated[float, Form(ge=0, le=300)] = 10.0,
        max_runtime_seconds: Annotated[float, Form(ge=1, le=3600)] = 900.0,
    ) -> JobView:
        try:
            options = JobOptions(
                first_delay_seconds=first_delay_seconds,
                retry_delay_seconds=retry_delay_seconds,
                first_fail=first_fail,
                retry_fail=retry_fail,
                first_wait_seconds=first_wait_seconds,
                max_runtime_seconds=max_runtime_seconds,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=422,
                detail=[
                    {"loc": list(err["loc"]), "msg": err["msg"], "type": err["type"]}
                    for err in exc.errors()
                ],
            ) from exc

        payload: list[tuple[str, bytes]] = []
        total = 0
        for upload in files:
            name = _safe_upload_name(upload.filename)
            data = await _read_limited(
                upload,
                settings.max_file_bytes,
                settings.max_total_bytes - total,
                settings.max_total_bytes,
            )
            total += len(data)
            payload.append((name, data))

        try:
            job_id = orchestrator.submit(payload, options)
        except SubmissionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        job = storage.get_job(job_id)
        assert job is not None
        return job

    @app.get(
        "/jobs",
        response_model=JobList,
        tags=["packaging"],
        summary="List jobs (newest first)",
    )
    def list_jobs(
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> JobList:
        jobs, total = storage.list_jobs(limit, offset)
        return JobList(jobs=jobs, total=total, limit=limit, offset=offset)

    @app.get(
        "/jobs/{job_id}",
        response_model=JobView,
        tags=["packaging"],
        summary="Job status, attempts and published artifact",
    )
    def get_job(job_id: str) -> JobView:
        job = storage.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @app.get(
        "/jobs/{job_id}/artifact",
        tags=["packaging"],
        summary="Download the published ZIP",
        response_class=FileResponse,
    )
    def download_artifact(job_id: str):
        job = storage.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        path = orchestrator.artifact_path(job_id)
        if path is None:
            raise HTTPException(status_code=404, detail="artifact not published")
        row = storage.get_artifact(job_id)
        assert row is not None
        return FileResponse(
            path,
            media_type="application/zip",
            filename=f"{job_id}.zip",
            headers={
                "X-Artifact-SHA256": row["sha256"],
                "X-Artifact-Round": str(row["round"]),
            },
        )

    @app.get(
        "/winners",
        response_model=list[WinnerRecord],
        tags=["packaging"],
        summary="Winner records for every published job",
    )
    def list_winners() -> list[WinnerRecord]:
        return [_winner(row) for row in storage.all_artifacts()]

    @app.get(
        "/jobs/{job_id}/winner",
        response_model=WinnerRecord,
        tags=["packaging"],
        summary="Winning record for one job",
    )
    def get_winner(job_id: str) -> WinnerRecord:
        row = storage.get_artifact(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="no published artifact")
        return _winner(row)

    @app.exception_handler(SubmissionError)
    async def _submission_error(_request, exc: SubmissionError):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    return app


def _winner(row) -> WinnerRecord:
    return WinnerRecord(
        job_id=row["job_id"],
        round=row["round"],
        sha256=row["sha256"],
        size=row["size"],
        file_count=row["file_count"],
        published_at=_parse(row["published_at"]),
        download_url=f"/jobs/{row['job_id']}/artifact",
    )


def _parse(raw: str):
    from .utils import parse_iso

    return parse_iso(raw)


def _safe_upload_name(name: Optional[str]) -> str:
    """File names are taken verbatim; validation rejects unsafe paths."""
    if not name:
        raise HTTPException(status_code=400, detail="every file needs a name")
    return name


async def _read_limited(
    upload: UploadFile, max_file: int, max_remaining: int, max_total: int
) -> bytes:
    limit = min(max_file, max_remaining)
    if limit <= 0:
        raise HTTPException(status_code=413, detail="submission too large")
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = await upload.read(64 * 1024)
        if not chunk:
            break
        size += len(chunk)
        if size > limit:
            detail = (
                f"submission exceeds {max_total} bytes"
                if limit == max_remaining
                else f"file {upload.filename} exceeds the size limit"
            )
            raise HTTPException(status_code=413, detail=detail)
        chunks.append(chunk)
    await upload.close()
    return b"".join(chunks)


app = create_app()


def main() -> None:  # pragma: no cover - convenience entry point
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
    )


if __name__ == "__main__":
    main()
