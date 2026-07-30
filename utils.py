"""Time helpers and atomic filesystem writes."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any


def parse_iso_string(iso_datetime_string: str) -> datetime:
    """Parse an ISO 8601 string into a timezone-aware datetime.

    Example: 2026-07-14T15:30:00+02:00
    """
    return datetime.fromisoformat(iso_datetime_string)


def datetime_to_timestamp_ms_utc(dt: datetime) -> int:
    """Convert a timezone-aware datetime to Unix epoch milliseconds (UTC)."""
    return int(dt.timestamp() * 1000)


def iso_string_to_timestamp_ms_utc(iso_datetime_string: str) -> int:
    """Parse an ISO 8601 string and return Unix epoch milliseconds (UTC)."""
    return datetime_to_timestamp_ms_utc(parse_iso_string(iso_datetime_string))


def _tmp_path(path: Path) -> Path:
    """Sibling temp path on the same directory (required for atomic os.replace)."""
    path = Path(path)
    return path.with_name(f"{path.name}.{os.getpid()}.tmp")


def atomic_write_text(path: Path | str, text: str, *, encoding: str = "utf-8") -> None:
    """Write text via temp file + os.replace so readers never see a partial file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    try:
        tmp.write_text(text, encoding=encoding)
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def atomic_write_parquet(df: Any, path: Path | str, **kwargs: Any) -> None:
    """Write a DataFrame to Parquet via temp file + os.replace."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    try:
        df.to_parquet(tmp, **kwargs)
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
