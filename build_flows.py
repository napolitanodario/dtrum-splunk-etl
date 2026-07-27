"""Build FlussoP1 flows from cached action chunks or a consolidated Parquet file."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from funnel.reconstruct import load_action_chunks, reconstruct_flows, write_flow_outputs

log = logging.getLogger("usat")
OUTPUT_DIR = Path("output")
DEFAULT_CACHE = Path(".cache/usql")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reconstruct FlussoP1 flows from raw action Parquet data.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Consolidated actions Parquet (mutually exclusive with --cache-dir).",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help=f"Load actions_*.parquet chunks from cache (default: {DEFAULT_CACHE})",
    )
    parser.add_argument(
        "--stem",
        required=True,
        help="Output file stem, e.g. 2026-07-21",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Directory for flow Parquets (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.input is not None:
        raw = pd.read_parquet(args.input)
    else:
        cache_dir = args.cache_dir or DEFAULT_CACHE
        raw = load_action_chunks(cache_dir)
        if raw.empty:
            log.error("No action chunks found under %s", cache_dir)
            return 1

    log.info("Loaded %d action rows", len(raw))
    result = reconstruct_flows(raw)
    paths = write_flow_outputs(result, args.output_dir, args.stem)

    print(f"flussi={len(result.flows)}")
    print(f"matched_rows={len(result.matched)}")
    if not result.flows.empty:
        print(f"completed={int(result.flows['completed'].sum())}")
    for key, path in paths.items():
        print(f"{key}={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
