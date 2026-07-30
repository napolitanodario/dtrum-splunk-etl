"""Persistent ingestion state: day watermark and per-day ledger."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from utils import atomic_write_parquet, atomic_write_text

log = logging.getLogger("splunk_ingest")


class State:
    """On-disk state under state_dir. Makes periodic re-runs idempotent."""

    def __init__(self, state_dir: Path):
        self.dir = Path(state_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.watermark_path = self.dir / "watermark.json"
        self.ledger_path = self.dir / "days_ledger.parquet"

    def last_settled_day(self) -> Optional[str]:
        if self.watermark_path.exists():
            return json.loads(self.watermark_path.read_text()).get("last_settled_day")
        return None

    def set_last_settled_day(self, day: str) -> None:
        atomic_write_text(self.watermark_path, json.dumps({"last_settled_day": day}))

    def shipped_days(self) -> set[str]:
        if self.ledger_path.exists():
            return set(pd.read_parquet(self.ledger_path)["day"].astype(str))
        return set()

    def record_day(self, day: str, n_flussi: int) -> None:
        row = pd.DataFrame([{
            "day": day,
            "shipped_at": pd.Timestamp.utcnow().isoformat(),
            "n_flussi": n_flussi,
        }])
        if self.ledger_path.exists():
            prev = pd.read_parquet(self.ledger_path)
            prev = prev[prev["day"].astype(str) != day]
            row = pd.concat([prev, row], ignore_index=True)
        atomic_write_parquet(row, self.ledger_path, index=False)
