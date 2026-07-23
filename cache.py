"""Local Parquet cache for Dynatrace USQL DataFrames.

Independent of DynatraceUSQLClient: callers pass DataFrames in and out.
Each entry is marked with a sidecar so continuous ETL runs can tell which
blobs came from the Dynatrace USQL API and how far a query has progressed.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

log = logging.getLogger("usat")

# Project-local cache root. Safe to wipe; everything is rebuildable from the API.
CACHE_DIR = Path(".cache/usql")

# Provenance mark written into every sidecar and watermark file.
SOURCE_MARK = "dynatrace-usql"


def _query_fingerprint(query: str) -> str:
    """Stable short id for a USQL query string (full text is not stored)."""
    return hashlib.sha256(query.encode("utf-8")).hexdigest()[:16]


def _entry_key(query: str, start_ms: int, end_ms: int) -> str:
    """Cache key for one (query, window) pair."""
    raw = f"{query}|{start_ms}|{end_ms}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class UsqlCache:
    """Disk cache: Parquet payload + JSON provenance sidecar + per-query watermark.

    Design choices (kept deliberately small):
    - One directory, flat layout.
    - Sidecar must be present and marked SOURCE_MARK or the entry is ignored.
    - Watermark is a single end_ms per query fingerprint; it only moves forward.
    - No TTL / eviction: continuous ETL owns when to delete or force-refresh.
    """

    def __init__(self, root: Path = CACHE_DIR):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _entry_paths(
            self, query: str, start_ms: int, end_ms: int, label: str = ""
    ) -> tuple[Path, Path]:
        key = _entry_key(query, start_ms, end_ms)
        stem = f"{label}_{key}" if label else key
        return self.root / f"{stem}.parquet", self.root / f"{stem}.meta.json"

    def _watermark_path(self, query: str) -> Path:
        return self.root / f"{_query_fingerprint(query)}.watermark.json"

    def put(
            self,
            df: pd.DataFrame,
            query: str,
            start_ms: int,
            end_ms: int,
            label: str = "",
            advance_watermark: bool = True,
    ) -> Path:
        """Persist a fetch result and mark it as Dynatrace USQL data."""
        parquet_path, meta_path = self._entry_paths(query, start_ms, end_ms, label)
        df.to_parquet(parquet_path, index=False, compression="zstd")

        meta = {
            "source": SOURCE_MARK,
            "query_fingerprint": _query_fingerprint(query),
            "start_ms": start_ms,
            "end_ms": end_ms,
            "row_count": int(len(df)),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "label": label or None,
        }
        meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
        log.info("Cached %d rows -> %s", len(df), parquet_path.name)

        if advance_watermark:
            self.set_watermark(query, end_ms)

        return parquet_path

    def get(
            self,
            query: str,
            start_ms: int,
            end_ms: int,
            label: str = "",
    ) -> Optional[pd.DataFrame]:
        """Return a cached DataFrame, or None on miss / invalid mark."""
        parquet_path, meta_path = self._entry_paths(query, start_ms, end_ms, label)
        if not parquet_path.exists() or not meta_path.exists():
            return None

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("source") != SOURCE_MARK:
            log.warning("Ignoring unmarked cache entry: %s", meta_path.name)
            return None
        if (
                meta.get("start_ms") != start_ms
                or meta.get("end_ms") != end_ms
                or meta.get("query_fingerprint") != _query_fingerprint(query)
        ):
            log.warning("Cache sidecar mismatch for %s; ignoring.", meta_path.name)
            return None

        log.info("Cache hit: %s (%s rows)", parquet_path.name, meta.get("row_count"))
        return pd.read_parquet(parquet_path)

    def get_watermark(self, query: str) -> Optional[int]:
        """Farthest end_ms already covered for this query, if recorded."""
        path = self._watermark_path(query)
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("source") != SOURCE_MARK:
            return None
        return data.get("end_ms")

    def set_watermark(self, query: str, end_ms: int) -> None:
        """Mark this query as covered up to end_ms. Never moves backwards."""
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
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        log.info(
            "Watermark query=%s end_ms=%s",
            _query_fingerprint(query),
            end_ms,
        )
