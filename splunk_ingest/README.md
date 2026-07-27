# splunk_ingest

Ship reconstructed lean funnel events to a Splunk HTTP Event Collector.

- **Sourcetype:** `{prefix}:flusso` only (default `dtrum:funnel:flusso`)
- **Schema:** version **2** (same payload as `export_splunk_flussi.py` / JSONL exports)
- **Source:** USQL day cache (`.cache/usql/{YYYY-MM-DD}/`), not live Dynatrace fetch

## Events produced

| sourcetype | grain | notes |
|---|---|---|
| `dtrum:funnel:flusso` | one per reconstructed emission attempt | nested `steps[]` with actions; `blockStartTime` / `blockEndTime`; enrichment `browserType` / `country` / `city` / `bounce` |

No `:action` or `:action_dim` events are emitted.

## Setup

```bash
cp splunk_ingest/config.example.toml splunk_ingest/prod.toml   # edit url/token/index
# or export SPLUNK_HEC_URL / SPLUNK_HEC_TOKEN / SPLUNK_HEC_INDEX ...
```

Fetch the day first with the ETL, then ingest:

```bash
python main.py --start 2026-06-22T00:00:00+02:00 --end 2026-06-23T00:00:00+02:00
python -m splunk_ingest backfill --since 2026-06-22 --until 2026-06-22 --config splunk_ingest/prod.toml --dry-run
```

## Use

```bash
# Incremental (for cron): ships every settled day since the watermark.
python -m splunk_ingest run --config splunk_ingest/prod.toml

# Historical load from cache.
python -m splunk_ingest backfill --since 2026-06-15 --until 2026-06-22 --config splunk_ingest/prod.toml

# Validate volume / shape without sending.
python -m splunk_ingest backfill --since 2026-06-22 --until 2026-06-22 --config splunk_ingest/prod.toml --dry-run
```

## Idempotency

- Unit of work: funnel calendar day (`FUNNEL_DAY_TZ`, Europe/Rome).
- A day ships only after settlement (`day end + settlement_lag_hours`).
- Ledger under `state_dir` (default `.cache/splunk_state/`): `watermark.json`, `days_ledger.parquet`.
- Deterministic `eventId` (`f:{flussoId}`): on partial re-run use `| dedup eventId` Splunk-side.
