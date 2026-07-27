"""Discovery test: 1h window on 2026-07-21 (action startTime filter)."""

from __future__ import annotations

import logging

import pandas as pd

from client import PAGE_SIZE, DynatraceUSQLClient
from config import DISCOVERY_LIMIT, DISCOVERY_NAME_PREFIXES, get_credentials
from queries import discovery_query
from utils import iso_string_to_timestamp_ms_utc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("usat")

START_MS = iso_string_to_timestamp_ms_utc("2026-07-21T10:00:00+02:00")
END_MS = iso_string_to_timestamp_ms_utc("2026-07-21T11:00:00+02:00")
OUT_CSV = "discovery_2026-07-21_10-11.csv"


def main() -> None:
    log.info(
        "Window %s -> %s (ms %s -> %s)",
        "2026-07-21T10:00:00+02:00",
        "2026-07-21T11:00:00+02:00",
        START_MS,
        END_MS,
    )

    env_id, api_token = get_credentials()
    client = DynatraceUSQLClient(env_id, api_token)
    query = discovery_query(DISCOVERY_NAME_PREFIXES, limit=DISCOVERY_LIMIT)

    # Full adaptive fetch so the hour is covered completely.
    df = client.fetch(
        query=query,
        start_ms=START_MS,
        end_ms=END_MS,
        page_size=PAGE_SIZE,
    )
    if "sessionId" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["sessionId"]).reset_index(drop=True)
        log.info("drop_duplicates sessionId: %d -> %d", before, len(df))

    df.to_csv(OUT_CSV, index=False)
    log.info("Wrote %s sessions=%d", OUT_CSV, len(df))
    print(df.head(20).to_string())
    print(f"sessions={len(df)} file={OUT_CSV}")


if __name__ == "__main__":
    main()
