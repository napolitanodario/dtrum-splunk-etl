"""CLI for unattended daily USQL fetch + Splunk ingest + cache prune."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from splunk_ingest.config import IngestConfig

from .orchestrate import run_daily

log = logging.getLogger("daily_run")


def setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_log = log_dir / f"daily_run_{run_id}.log"

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(run_log, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    root.addHandler(fh)
    root.addHandler(ch)

    # Keep child loggers (usat, splunk_ingest) visible.
    for name in ("usat", "splunk_ingest", "daily_run"):
        logging.getLogger(name).setLevel(logging.DEBUG)

    return run_log


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="daily_run",
        description=(
            "Unattended daily pipeline: fetch settled USQL day(s) into cache, "
            "ship to Splunk HEC, prune old cache folders."
        ),
    )
    p.add_argument(
        "--config",
        default="splunk_ingest/prod.toml",
        help="HEC TOML config (default: splunk_ingest/prod.toml).",
    )
    p.add_argument("--cache-dir", help="Override USQL cache root.")
    p.add_argument(
        "--day",
        help="Process a single YYYY-MM-DD (must be settled unless --force-settlement).",
    )
    p.add_argument(
        "--force-settlement",
        action="store_true",
        help="Allow --day even if not yet settled.",
    )
    p.add_argument(
        "--force-fetch",
        action="store_true",
        help="Ignore USQL cache and re-fetch from Dynatrace.",
    )
    p.add_argument("--skip-fetch", action="store_true", help="Skip Dynatrace fetch.")
    p.add_argument("--skip-ingest", action="store_true", help="Skip Splunk ingest.")
    p.add_argument("--skip-prune", action="store_true", help="Skip USQL cache prune.")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Ingest dry-run (no HEC send); skips prune.",
    )
    p.add_argument(
        "--cache-retention-days",
        type=int,
        default=None,
        help="Override [cache] retention_days (0 = never prune). Default from TOML/14.",
    )
    p.add_argument(
        "--log-dir",
        type=Path,
        default=Path("logs"),
        help="Directory for daily_run_*.log (default: logs/).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_log = setup_logging(args.log_dir)
    log.info("Logging to %s", run_log)

    try:
        cfg_path = Path(args.config)
        if cfg_path.is_file():
            cfg = IngestConfig.from_toml(cfg_path)
        else:
            log.warning("Config file %s not found; falling back to env vars", cfg_path)
            cfg = IngestConfig.from_env()

        if args.cache_dir:
            cfg.cache_dir = Path(args.cache_dir)

        summary = run_daily(
            cfg,
            day=args.day,
            force_settlement=args.force_settlement,
            force_fetch=args.force_fetch,
            skip_fetch=args.skip_fetch,
            skip_ingest=args.skip_ingest,
            skip_prune=args.skip_prune,
            dry_run=args.dry_run,
            retention_days=args.cache_retention_days,
        )
    except Exception:
        log.exception("daily_run aborted")
        return 1

    pruned = summary.get("prune") or {}
    log.info(
        "Done range=%s..%s fetched=%s ingest_days=%d pruned=%s bytes_freed=%s",
        summary.get("start"),
        summary.get("end"),
        summary.get("fetched"),
        len(summary.get("ingest") or []),
        pruned.get("pruned_days"),
        pruned.get("bytes_freed"),
    )
    print(f"log={run_log}")
    return 0
