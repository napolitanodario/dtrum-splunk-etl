"""Orchestration: reconstruct a calendar day from USQL cache and ship to Splunk HEC.

Shipping unit is the funnel calendar day (FUNNEL_DAY_TZ). A day is shipped once it is
settled (day end + settlement_lag_hours is in the past). Payload is lean flusso schema v2
only (sourcetype ``…:flusso``).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config import FUNNEL_DAY_TZ
from funnel.reconstruct import load_action_chunks, reconstruct_flows
from funnel.splunk_events import SCHEMA_VERSION, iter_flusso_events

from .config import IngestConfig
from .envelope import wrap_flusso_events
from .hec import HECClient
from .state import State

log = logging.getLogger("splunk_ingest")


def _hec(cfg: IngestConfig) -> HECClient:
    return HECClient(
        cfg.hec_url,
        cfg.hec_token,
        verify=cfg.verify,
        timeout=cfg.timeout,
        batch_size=cfg.batch_size,
        gzip_payload=cfg.gzip_payload,
        channel=cfg.channel,
    )


def process_day(
    cfg: IngestConfig,
    state: State,
    hec: HECClient | None,
    day: str,
    *,
    dry_run: bool = False,
) -> dict:
    """Load cached actions for ``day``, reconstruct flussi, ship HEC envelopes."""
    counts = {
        "day": day,
        "action_rows": 0,
        "flussi": 0,
        "schema_version": SCHEMA_VERSION,
    }

    raw = load_action_chunks(cfg.cache_dir, day=day)
    if raw.empty:
        log.info("Day %s: no cached actions under %s", day, cfg.cache_dir)
        if not dry_run:
            state.record_day(day, 0)
        return counts

    counts["action_rows"] = len(raw)
    result = reconstruct_flows(raw)
    bodies = list(iter_flusso_events(result))
    envelopes = list(wrap_flusso_events(bodies, cfg))
    counts["flussi"] = len(envelopes)

    if dry_run:
        log.info("DRY-RUN %s: %s", day, counts)
        return counts

    assert hec is not None
    hec.send(envelopes)
    state.record_day(day, len(envelopes))
    log.info("Shipped %s: %s", day, counts)
    return counts


def latest_settled_day(cfg: IngestConfig) -> date:
    """Last funnel calendar day whose end + lag is already in the past."""
    tz = ZoneInfo(FUNNEL_DAY_TZ)
    now_local = datetime.now(timezone.utc).astimezone(tz)
    cutoff = now_local - timedelta(hours=cfg.settlement_lag_hours)
    # Day D ends at midnight of D+1 local; settled when that instant + lag has passed
    # iff cutoff is past end of D, i.e. latest settled is cutoff.date() - 1 day.
    return cutoff.date() - timedelta(days=1)


def run_incremental(
    cfg: IngestConfig,
    *,
    since: str | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Ship every settled day from the watermark (exclusive) up to latest settled."""
    state = State(cfg.state_dir)
    hec = None if dry_run else _hec(cfg)
    latest = latest_settled_day(cfg)

    wm = state.last_settled_day()
    if wm:
        start_day = date.fromisoformat(wm) + timedelta(days=1)
    elif since:
        start_day = date.fromisoformat(since) if isinstance(since, str) else since
    else:
        start_day = latest
        log.info("No watermark: ingesting only %s (use backfill for history).", latest)

    if start_day > latest:
        log.info("Nothing to ingest (watermark=%s, latest_settled=%s).", wm, latest)
        return []

    results = []
    d = start_day
    while d <= latest:
        results.append(process_day(cfg, state, hec, d.isoformat(), dry_run=dry_run))
        if not dry_run:
            state.set_last_settled_day(d.isoformat())
        d += timedelta(days=1)
    return results


def run_backfill(
    cfg: IngestConfig,
    since: str,
    until: str,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> list[dict]:
    """Ship a historical day range without touching the incremental watermark."""
    state = State(cfg.state_dir)
    hec = None if dry_run else _hec(cfg)
    shipped = state.shipped_days()

    results = []
    d = date.fromisoformat(since)
    end = date.fromisoformat(until)
    while d <= end:
        ds = d.isoformat()
        if ds in shipped and not force:
            log.info("Skip %s (already shipped; use --force to resend).", ds)
        else:
            results.append(process_day(cfg, state, hec, ds, dry_run=dry_run))
        d += timedelta(days=1)
    return results
