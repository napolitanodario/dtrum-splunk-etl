"""Time helpers, log rotation helpers, and atomic filesystem writes."""

from __future__ import annotations

import logging
import logging.handlers
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Daily midnight rotation; keep ~2 weeks of history (aligned with cache default).
LOG_WHEN = "midnight"
LOG_INTERVAL = 1
LOG_BACKUP_COUNT = 14
LOG_RETENTION_DAYS = 14
_LOG_PRUNE_GLOBS = ("etl*.log*", "daily_run*.log*")


def make_timed_rotating_handler(
    path: Path,
    *,
    level: int,
    formatter: logging.Formatter,
    backup_count: int = LOG_BACKUP_COUNT,
) -> logging.handlers.TimedRotatingFileHandler:
    """File handler that rotates at midnight UTC and keeps ``backup_count`` files."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.TimedRotatingFileHandler(
        path,
        when=LOG_WHEN,
        interval=LOG_INTERVAL,
        backupCount=backup_count,
        encoding="utf-8",
        utc=True,
    )
    handler.setLevel(level)
    handler.setFormatter(formatter)
    return handler


def prune_log_files(
    log_dir: Path,
    *,
    retention_days: int = LOG_RETENTION_DAYS,
) -> int:
    """Delete aged log files (active + rotated + legacy timestamped names).

    Returns the number of files removed. ``retention_days <= 0`` disables pruning.
    """
    log_dir = Path(log_dir)
    if retention_days <= 0 or not log_dir.is_dir():
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    removed = 0
    seen: set[Path] = set()
    for pattern in _LOG_PRUNE_GLOBS:
        for path in log_dir.glob(pattern):
            if not path.is_file() or path in seen:
                continue
            seen.add(path)
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                continue
            if mtime < cutoff:
                path.unlink(missing_ok=True)
                removed += 1
    return removed


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
