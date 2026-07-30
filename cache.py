"""Local Parquet cache for Dynatrace USQL DataFrames.

Two-tier layout per calendar day (FUNNEL_DAY_TZ):

    .cache/usql/{YYYY-MM-DD}/
        discovery.parquet + discovery.meta.json
        actions.parquet + actions.meta.json      # consolidated after full fetch
        _staging/                                  # per-chunk staging during fetch
            actions_0001_of_0171_{key}.parquet
        {query_fp}.watermark.json

Staging supports resume on partial runs; consolidated actions are the fast path
for funnel rebuild and Splunk export (single Parquet read).
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from config import FUNNEL_DAY_TZ
from utils import atomic_write_parquet, atomic_write_text

log = logging.getLogger("usat")

CACHE_DIR = Path(".cache/usql")
SOURCE_MARK = "dynatrace-usql"
STAGING_DIRNAME = "_staging"


def _query_fingerprint(query: str) -> str:
    return hashlib.sha256(query.encode("utf-8")).hexdigest()[:16]


def _entry_key(query: str, start_ms: int, end_ms: int) -> str:
    raw = f"{query}|{start_ms}|{end_ms}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def day_key_from_ms(start_ms: int, tz_name: str | None = None) -> str:
    """Calendar day key (YYYY-MM-DD) for a UTC epoch-ms window start."""
    tz = ZoneInfo(tz_name or FUNNEL_DAY_TZ)
    dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).astimezone(tz)
    return dt.strftime("%Y-%m-%d")


def _write_meta(
        path: Path,
        *,
        kind: str,
        query: str | None,
        start_ms: int,
        end_ms: int,
        row_count: int,
        label: str | None = None,
) -> None:
    meta = {
        "source": SOURCE_MARK,
        "kind": kind,
        "query_fingerprint": _query_fingerprint(query) if query else None,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "row_count": row_count,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "label": label,
    }
    atomic_write_text(path, json.dumps(meta, indent=2) + "\n")


def _read_meta(meta_path: Path) -> dict | None:
    if not meta_path.exists():
        return None
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if meta.get("source") != SOURCE_MARK:
        log.warning("Ignoring unmarked cache entry: %s", meta_path.name)
        return None
    return meta


def _meta_matches_window(meta: dict, start_ms: int, end_ms: int, query: str | None) -> bool:
    if meta.get("start_ms") != start_ms or meta.get("end_ms") != end_ms:
        return False
    if query is not None and meta.get("query_fingerprint") != _query_fingerprint(query):
        return False
    return True


class UsqlCache:
    """Per-day disk cache: staging chunks + consolidated discovery/actions."""

    def __init__(self, day: str, root: Path = CACHE_DIR):
        if not day:
            raise ValueError("day is required (YYYY-MM-DD)")
        self.root = Path(root)
        self.day = day
        self.day_dir = self.root / day
        self.staging_dir = self.day_dir / STAGING_DIRNAME
        self.day_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    # -- consolidated (day root) ------------------------------------------------

    def put_discovery(
            self,
            df: pd.DataFrame,
            query: str,
            start_ms: int,
            end_ms: int,
            *,
            advance_watermark: bool = True,
    ) -> Path:
        parquet_path = self.day_dir / "discovery.parquet"
        meta_path = self.day_dir / "discovery.meta.json"
        atomic_write_parquet(df, parquet_path, index=False, compression="zstd")
        _write_meta(
            meta_path,
            kind="discovery",
            query=query,
            start_ms=start_ms,
            end_ms=end_ms,
            row_count=int(len(df)),
        )
        log.info("Cached discovery %d rows -> %s", len(df), parquet_path)
        if advance_watermark:
            self.set_watermark(query, end_ms)
        return parquet_path

    def get_discovery(
            self,
            query: str,
            start_ms: int,
            end_ms: int,
    ) -> Optional[pd.DataFrame]:
        parquet_path = self.day_dir / "discovery.parquet"
        meta_path = self.day_dir / "discovery.meta.json"
        if not parquet_path.exists():
            return None
        meta = _read_meta(meta_path)
        if meta is None or not _meta_matches_window(meta, start_ms, end_ms, query):
            return None
        log.info("Discovery cache hit: %s (%s rows)", parquet_path.name, meta.get("row_count"))
        return pd.read_parquet(parquet_path)

    def put_actions(
            self,
            df: pd.DataFrame,
            start_ms: int,
            end_ms: int,
    ) -> Path:
        parquet_path = self.day_dir / "actions.parquet"
        meta_path = self.day_dir / "actions.meta.json"
        atomic_write_parquet(df, parquet_path, index=False, compression="zstd")
        _write_meta(
            meta_path,
            kind="actions",
            query=None,
            start_ms=start_ms,
            end_ms=end_ms,
            row_count=int(len(df)),
        )
        log.info("Cached consolidated actions %d rows -> %s", len(df), parquet_path)
        return parquet_path

    def get_actions(self, start_ms: int, end_ms: int) -> Optional[pd.DataFrame]:
        parquet_path = self.day_dir / "actions.parquet"
        meta_path = self.day_dir / "actions.meta.json"
        if not parquet_path.exists():
            return None
        meta = _read_meta(meta_path)
        if meta is None or not _meta_matches_window(meta, start_ms, end_ms, query=None):
            return None
        log.info("Actions cache hit: %s (%s rows)", parquet_path.name, meta.get("row_count"))
        return pd.read_parquet(parquet_path)

    def consolidate_actions(
            self,
            df: pd.DataFrame,
            start_ms: int,
            end_ms: int,
            *,
            clear_staging: bool = True,
    ) -> Path:
        """Write consolidated actions and optionally remove staging chunks."""
        path = self.put_actions(df, start_ms, end_ms)
        if clear_staging:
            self.clear_staging()
        return path

    def has_consolidated_actions(self, start_ms: int, end_ms: int) -> bool:
        return self.get_actions(start_ms, end_ms) is not None

    # -- staging (per-chunk) ----------------------------------------------------

    def put_chunk(
            self,
            df: pd.DataFrame,
            query: str,
            start_ms: int,
            end_ms: int,
            label: str,
    ) -> Path:
        parquet_path, meta_path = self._chunk_paths(query, start_ms, end_ms, label)
        atomic_write_parquet(df, parquet_path, index=False, compression="zstd")
        _write_meta(
            meta_path,
            kind="chunk",
            query=query,
            start_ms=start_ms,
            end_ms=end_ms,
            row_count=int(len(df)),
            label=label,
        )
        log.info("Cached staging chunk %d rows -> %s", len(df), parquet_path.name)
        return parquet_path

    def get_chunk(
            self,
            query: str,
            start_ms: int,
            end_ms: int,
            label: str,
    ) -> Optional[pd.DataFrame]:
        parquet_path, meta_path = self._chunk_paths(query, start_ms, end_ms, label)
        if not parquet_path.exists():
            return None
        meta = _read_meta(meta_path)
        if meta is None or not _meta_matches_window(meta, start_ms, end_ms, query):
            return None
        log.info("Staging cache hit: %s (%s rows)", parquet_path.name, meta.get("row_count"))
        return pd.read_parquet(parquet_path)

    def list_staging_chunks(self) -> list[Path]:
        return sorted(self.staging_dir.glob("actions_*.parquet"))

    def load_staging_actions(self) -> pd.DataFrame:
        files = self.list_staging_chunks()
        if not files:
            return pd.DataFrame()
        parts = [pd.read_parquet(p) for p in files]
        return pd.concat(parts, ignore_index=True).drop_duplicates()

    def clear_staging(self) -> int:
        """Remove all files under _staging/. Returns number of files removed."""
        if not self.staging_dir.exists():
            return 0
        removed = 0
        for path in self.staging_dir.iterdir():
            path.unlink()
            removed += 1
        log.info("Cleared %d staging file(s) under %s", removed, self.staging_dir)
        return removed

    def invalidate(self) -> None:
        """Drop consolidated discovery/actions, staging, and day watermarks."""
        for name in ("discovery.parquet", "discovery.meta.json", "actions.parquet", "actions.meta.json"):
            path = self.day_dir / name
            if path.exists():
                path.unlink()
        self.clear_staging()
        for path in self.day_dir.glob("*.watermark.json"):
            path.unlink()
        log.info("Invalidated cache for day %s", self.day)

    # -- watermark ----------------------------------------------------------------

    def get_watermark(self, query: str) -> Optional[int]:
        path = self._watermark_path(query)
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("source") != SOURCE_MARK:
            return None
        return data.get("end_ms")

    def set_watermark(self, query: str, end_ms: int) -> None:
        current = self.get_watermark(query)
        if current is not None and end_ms < current:
            return
        payload = {
            "source": SOURCE_MARK,
            "query_fingerprint": _query_fingerprint(query),
            "end_ms": end_ms,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        path = self._watermark_path(query)
        atomic_write_text(path, json.dumps(payload, indent=2) + "\n")
        log.info("Watermark day=%s query=%s end_ms=%s", self.day, _query_fingerprint(query), end_ms)

    # -- legacy flat API (delegates to day-scoped methods) ------------------------

    def put(
            self,
            df: pd.DataFrame,
            query: str,
            start_ms: int,
            end_ms: int,
            label: str = "",
            advance_watermark: bool = True,
    ) -> Path:
        if label == "discovery":
            return self.put_discovery(
                df, query, start_ms, end_ms, advance_watermark=advance_watermark,
            )
        return self.put_chunk(df, query, start_ms, end_ms, label=label)

    def get(
            self,
            query: str,
            start_ms: int,
            end_ms: int,
            label: str = "",
    ) -> Optional[pd.DataFrame]:
        if label == "discovery":
            return self.get_discovery(query, start_ms, end_ms)
        return self.get_chunk(query, start_ms, end_ms, label=label)

    def _chunk_paths(
            self, query: str, start_ms: int, end_ms: int, label: str,
    ) -> tuple[Path, Path]:
        key = _entry_key(query, start_ms, end_ms)
        stem = f"{label}_{key}" if label else key
        return (
            self.staging_dir / f"{stem}.parquet",
            self.staging_dir / f"{stem}.meta.json",
        )

    def _watermark_path(self, query: str) -> Path:
        return self.day_dir / f"{_query_fingerprint(query)}.watermark.json"


def _dir_size_bytes(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            try:
                total += child.stat().st_size
            except OSError:
                pass
    return total


def prune_usql_days(
    root: Path,
    *,
    keep_before: date,
    protect_after: date | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Remove USQL day directories strictly older than ``keep_before``.

    Day folders are named ``YYYY-MM-DD``. Never touches Splunk state dirs.

    If ``protect_after`` is set, also refuse to delete any day strictly after
    that date (unshipped / still needed for catch-up). A day equal to
    ``protect_after`` is considered shipped and may be pruned by age.

    Returns a list of dicts: ``{day, path, bytes, deleted}``.
    """
    root = Path(root)
    if not root.is_dir():
        return []

    removed: list[dict] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        try:
            day = date.fromisoformat(child.name)
        except ValueError:
            continue

        if day >= keep_before:
            continue
        if protect_after is not None and day > protect_after:
            log.info(
                "Skip prune %s (after protect_after=%s, unshipped)",
                child.name,
                protect_after.isoformat(),
            )
            continue

        size = _dir_size_bytes(child)
        entry = {
            "day": child.name,
            "path": str(child),
            "bytes": size,
            "deleted": not dry_run,
        }
        if dry_run:
            log.info("Would prune USQL day %s (%d bytes)", child.name, size)
        else:
            shutil.rmtree(child)
            log.info("Pruned USQL day %s (%d bytes)", child.name, size)
        removed.append(entry)
    return removed


def day_cache_status(cache_root: Path, day: str) -> str:
    """Classify a USQL day folder for ingest watermark decisions.

    Returns one of:
    - ``"actions"``: consolidated ``actions.parquet`` present (may be 0 rows).
      This is the completion marker written by a successful fetch.
    - ``"empty"``: legacy confirmed-empty day (``discovery.parquet`` only, no
      leftover staging chunks). Prefer writing empty ``actions.parquet`` going forward.
    - ``"incomplete"``: no proof of a finished fetch — do **not** advance the
      Splunk watermark (missing cache, failed/skipped fetch, or mid-fetch staging).
    """
    day_dir = Path(cache_root) / day
    if (day_dir / "actions.parquet").is_file():
        return "actions"

    staging = day_dir / STAGING_DIRNAME
    has_staging = staging.is_dir() and any(staging.glob("actions_*.parquet"))
    if has_staging:
        # Partial fetch: discovery may exist, but consolidate never ran.
        return "incomplete"

    if (day_dir / "discovery.parquet").is_file():
        return "empty"
    return "incomplete"


def resolve_day_dir(cache_root: Path, day: str | None = None) -> Path:
    """Return the day directory under cache_root (day required if root is not a day dir)."""
    cache_root = Path(cache_root)
    if day:
        return cache_root / day
    if (cache_root / "actions.parquet").exists() or (cache_root / STAGING_DIRNAME).is_dir():
        return cache_root
    raise ValueError("day is required when cache root is not a day directory")


def load_cached_actions(cache_root: Path, day: str | None = None) -> pd.DataFrame:
    """Load actions: prefer consolidated, else staging chunks, else legacy flat layout."""
    cache_root = Path(cache_root)

    try:
        day_dir = resolve_day_dir(cache_root, day)
    except ValueError:
        day_dir = None

    if day_dir is not None:
        consolidated = day_dir / "actions.parquet"
        if consolidated.exists():
            log.info("Loading consolidated actions from %s", consolidated)
            return pd.read_parquet(consolidated)

        staging = sorted((day_dir / STAGING_DIRNAME).glob("actions_*.parquet"))
        if staging:
            log.info("Loading %d staging chunk(s) from %s", len(staging), day_dir / STAGING_DIRNAME)
            parts = [pd.read_parquet(p) for p in staging]
            return pd.concat(parts, ignore_index=True).drop_duplicates()

    # Legacy flat layout (pre two-tier): actions_*.parquet directly under root.
    legacy = sorted(cache_root.glob("actions_*.parquet"))
    if legacy:
        log.info("Loading %d legacy flat chunk(s) from %s", len(legacy), cache_root)
        parts = [pd.read_parquet(p) for p in legacy]
        return pd.concat(parts, ignore_index=True).drop_duplicates()

    return pd.DataFrame()


def migrate_legacy_flat_cache(root: Path = CACHE_DIR, *, dry_run: bool = False) -> list[str]:
    """Move legacy flat discovery/chunk files into the newest day staging dir.

    Heuristic: group files by meta start_ms day key. Returns day keys touched.
    """
    root = Path(root)
    if not root.exists():
        return []

    touched: set[str] = set()
    for meta_path in sorted(root.glob("*.meta.json")):
        if meta_path.name.endswith(".watermark.json"):
            continue
        meta = _read_meta(meta_path)
        if meta is None:
            continue
        start_ms = meta.get("start_ms")
        if start_ms is None:
            continue
        day = day_key_from_ms(int(start_ms))
        label = meta.get("label") or meta_path.stem
        stem = meta_path.name.removesuffix(".meta.json")
        parquet_path = root / f"{stem}.parquet"
        if not parquet_path.exists():
            continue

        cache = UsqlCache(day, root=root)
        if meta.get("kind") == "discovery" or label == "discovery":
            target_p = cache.day_dir / "discovery.parquet"
            target_m = cache.day_dir / "discovery.meta.json"
        else:
            target_p = cache.staging_dir / parquet_path.name
            target_m = cache.staging_dir / meta_path.name

        if dry_run:
            log.info("Would migrate %s -> %s", parquet_path, target_p)
        else:
            target_p.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(parquet_path), str(target_p))
            shutil.move(str(meta_path), str(target_m))
            log.info("Migrated %s -> %s", parquet_path.name, target_p)
        touched.add(day)

    return sorted(touched)
