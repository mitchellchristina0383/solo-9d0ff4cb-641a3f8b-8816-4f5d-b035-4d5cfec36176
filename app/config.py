"""Runtime configuration (environment driven)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be positive, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    max_files: int
    max_total_bytes: int
    max_file_bytes: int

    @property
    def db_path(self) -> Path:
        return self.data_dir / "packager.sqlite3"

    @property
    def submissions_dir(self) -> Path:
        return self.data_dir / "submissions"

    @property
    def artifacts_dir(self) -> Path:
        return self.data_dir / "artifacts"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.submissions_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "Settings":
        max_total = _int_env("CI_PACKAGER_MAX_TOTAL_BYTES", 5 * 1024 * 1024)
        max_file = _int_env("CI_PACKAGER_MAX_FILE_BYTES", 2 * 1024 * 1024)
        if max_file > max_total:
            max_file = max_total
        return cls(
            data_dir=Path(os.environ.get("CI_PACKAGER_DATA_DIR", "./data")).resolve(),
            max_files=_int_env("CI_PACKAGER_MAX_FILES", 20),
            max_total_bytes=max_total,
            max_file_bytes=max_file,
        )
