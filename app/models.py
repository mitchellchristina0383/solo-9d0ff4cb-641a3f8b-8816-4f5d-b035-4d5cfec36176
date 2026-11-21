"""Pydantic models shared by the API, storage and executor layers."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, Field, model_validator


class JobState(str, Enum):
    RUNNING = "running"  # in flight: first round and/or retry
    SUCCEEDED = "succeeded"  # exactly one ZIP was published
    FAILED = "failed"  # both rounds failed
    INTERRUPTED = "interrupted"  # service restarted while the job was in flight


class AttemptState(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SUPERSEDED = "superseded"  # produced a valid ZIP, but another result won
    INTERRUPTED = "interrupted"


class AttemptInfo(BaseModel):
    round: Annotated[int, Field(ge=1, le=2)]
    state: AttemptState
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    zip_sha256: Optional[str] = None


class JobOptions(BaseModel):
    """Per-job knobs, exposed as multipart form fields.

    ``*_delay_seconds`` emulate how long the Python executor takes.  The
    ``*_fail`` flags make the corresponding round fail with a simulated
    executor error.  ``first_wait_seconds`` is the deadline after which a
    still-running first round triggers exactly one retry while the first
    round keeps executing.
    """

    first_delay_seconds: Annotated[float, Field(ge=0, le=300)] = 0.0
    retry_delay_seconds: Annotated[float, Field(ge=0, le=300)] = 0.0
    first_fail: bool = False
    retry_fail: bool = False
    first_wait_seconds: Annotated[float, Field(ge=0, le=300)] = 10.0
    max_runtime_seconds: Annotated[float, Field(ge=1, le=3600)] = 900.0

    @model_validator(mode="after")
    def _runtime_after_wait(self) -> "JobOptions":
        # The runtime guard is an absolute cap for the whole job, so it must
        # leave room for the first-round wait plus at least some execution.
        if self.max_runtime_seconds <= self.first_wait_seconds:
            raise ValueError(
                "max_runtime_seconds must be greater than first_wait_seconds"
            )
        return self


class ArtifactView(BaseModel):
    sha256: str
    size: int
    file_count: int
    round: int
    created_at: datetime
    published_at: datetime
    download_url: str


class JobView(BaseModel):
    job_id: str
    state: JobState
    detail: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    options: JobOptions
    attempts: list[AttemptInfo] = Field(default_factory=list)
    winner_round: Optional[Annotated[int, Field(ge=1, le=2)]] = None
    artifact: Optional[ArtifactView] = None


class JobList(BaseModel):
    jobs: list[JobView]
    total: int
    limit: int
    offset: int


class HealthView(BaseModel):
    status: str
    version: str
    jobs: int
    artifacts: int


class WinnerRecord(BaseModel):
    """Compact, durable record of the winning submission."""

    job_id: str
    round: int
    sha256: str
    size: int
    file_count: int
    published_at: datetime
    download_url: str
