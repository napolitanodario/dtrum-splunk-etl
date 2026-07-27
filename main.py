"""CLI: discover sessions and download/cache user actions for a time range."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from cache import UsqlCache
from client import PAGE_SIZE, DynatraceUSQLClient
from config import (
    ACTION_COLUMNS,
    DISCOVERY_NAME_PREFIXES,
    get_credentials,
)
from queries import discovery_query, session_actions_query
from utils import iso_string_to_timestamp_ms_utc

log = logging.getLogger("usat")

LOG_DIR = Path("logs")
OUTPUT_DIR = Path("output")
# Sessions per actions query: keeps URL size and LIMIT 5000 headroom.
DEFAULT_CHUNK_SIZE = 40
# First full-day smoke run defaults (Europe/Rome).
DEFAULT_START = "2026-07-21T09:00:00+02:00"
DEFAULT_END = "2026-07-21T18:00:00+02:00"
DEFAULT_OUTPUT = OUTPUT_DIR / "user_actions_2026-07-21_09-18.parquet"


def setup_logging(log_dir: Path) -> tuple[Path, Path]:
    """Configure console + file logging under log_dir.

    Returns (full_run_log, issues_log). The issues file stores WARNING+
    (incomplete windows, sampling, hard failures).
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_log = log_dir / f"etl_{run_id}.log"
    issues_log = log_dir / f"etl_{run_id}_issues.log"

    logger = logging.getLogger("usat")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_all = logging.FileHandler(run_log, encoding="utf-8")
    file_all.setLevel(logging.DEBUG)
    file_all.setFormatter(fmt)

    file_issues = logging.FileHandler(issues_log, encoding="utf-8")
    file_issues.setLevel(logging.WARNING)
    file_issues.setFormatter(fmt)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    logger.addHandler(file_all)
    logger.addHandler(file_issues)
    logger.addHandler(console)

    return run_log, issues_log


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Discover matching sessions and download/cache their user actions "
            "from Dynatrace USQL for a given time range."
        ),
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        help=f"Range start ISO-8601 (default: {DEFAULT_START})",
    )
    parser.add_argument(
        "--end",
        default=DEFAULT_END,
        help=f"Range end ISO-8601 (default: {DEFAULT_END})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore cache and re-fetch from Dynatrace",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Session ids per actions query (default: {DEFAULT_CHUNK_SIZE})",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=LOG_DIR,
        help=f"Directory for run and issue logs (default: {LOG_DIR})",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Override Parquet cache directory (default: .cache/usql)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output Parquet path for all fetched actions (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args(argv)


def _chunked(values: Sequence[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(values), size):
        yield list(values[i:i + size])


def _load_or_fetch(
        client: DynatraceUSQLClient,
        cache: UsqlCache,
        query: str,
        start_ms: int,
        end_ms: int,
        label: str,
        page_size: int,
        force: bool,
) -> pd.DataFrame:
    """Return cached DataFrame or fetch from Dynatrace and store it."""
    if not force:
        cached = cache.get(query, start_ms, end_ms, label=label)
        if cached is not None:
            log.info("Cache hit label=%s rows=%d", label, len(cached))
            return cached

    log.info("Fetching label=%s page_size=%d", label, page_size)
    try:
        df = client.fetch(
            query=query,
            start_ms=start_ms,
            end_ms=end_ms,
            page_size=page_size,
        )
    except RuntimeError as exc:
        log.error("Fetch failed label=%s: %s", label, exc)
        raise

    cache.put(df, query, start_ms, end_ms, label=label)
    log.info("Cached label=%s rows=%d", label, len(df))
    return df


def run(
        start_ms: int,
        end_ms: int,
        *,
        force: bool = False,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Discovery then chunked session-actions download with caching."""
    if end_ms <= start_ms:
        raise ValueError("end must be after start")
    if chunk_size < 1:
        raise ValueError("chunk-size must be >= 1")

    env_id, api_token = get_credentials()
    client = DynatraceUSQLClient(env_id, api_token)
    cache = UsqlCache(cache_dir) if cache_dir is not None else UsqlCache()

    log.info(
        "ETL start start_ms=%s end_ms=%s force=%s chunk_size=%s",
        start_ms, end_ms, force, chunk_size,
    )

    # --- Phase 1: discovery (action startTime filter; dedupe session ids) ---
    d_query = discovery_query(DISCOVERY_NAME_PREFIXES)
    sessions_df = _load_or_fetch(
        client, cache, d_query, start_ms, end_ms,
        label="discovery",
        page_size=PAGE_SIZE,
        force=force,
    )
    if sessions_df.empty or "sessionId" not in sessions_df.columns:
        log.warning("Discovery returned no sessions for this range")
        return pd.DataFrame()

    sessions_df = sessions_df.drop_duplicates(subset=["sessionId"])
    session_ids = (
        sessions_df["sessionId"].dropna().astype(str).tolist()
    )
    log.info("Discovery found %d distinct sessions", len(session_ids))

    # --- Phase 2: actions in chunks ---
    chunks = list(_chunked(session_ids, chunk_size))
    parts: list[pd.DataFrame] = []
    for index, chunk in enumerate(chunks, start=1):
        label = f"actions_{index:04d}_of_{len(chunks):04d}"
        a_query = session_actions_query(chunk, ACTION_COLUMNS)
        part = _load_or_fetch(
            client, cache, a_query, start_ms, end_ms,
            label=label,
            page_size=PAGE_SIZE,
            force=force,
        )
        if not part.empty:
            parts.append(part)
        log.info(
            "Actions chunk %d/%d sessions=%d rows=%d",
            index, len(chunks), len(chunk), len(part),
        )

    if not parts:
        log.warning("No user actions retrieved")
        return pd.DataFrame()

    actions = pd.concat(parts, ignore_index=True).drop_duplicates()
    log.info(
        "ETL done action_rows=%d sessions=%d",
        len(actions), actions["sessionId"].nunique() if "sessionId" in actions else 0,
    )
    # Advance a high-level watermark on the discovery query fingerprint.
    cache.set_watermark(d_query, end_ms)
    return actions


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_log, issues_log = setup_logging(args.log_dir)
    log.info("Logging to %s (issues: %s)", run_log, issues_log)

    try:
        start_ms = iso_string_to_timestamp_ms_utc(args.start)
        end_ms = iso_string_to_timestamp_ms_utc(args.end)
        actions = run(
            start_ms,
            end_ms,
            force=args.force,
            chunk_size=args.chunk_size,
            cache_dir=args.cache_dir,
        )
    except Exception:
        log.exception("ETL aborted due to an error")
        return 1

    if actions.empty:
        log.warning("No actions to write")
        print("action_rows=0")
        print(f"log={run_log}")
        print(f"issues_log={issues_log}")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    actions.to_parquet(args.output, index=False, compression="zstd")
    log.info("Wrote %s rows=%d", args.output, len(actions))

    print(f"action_rows={len(actions)}")
    if "sessionId" in actions.columns:
        print(f"sessions={actions['sessionId'].nunique()}")
    print(f"output={args.output}")
    print(f"log={run_log}")
    print(f"issues_log={issues_log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
