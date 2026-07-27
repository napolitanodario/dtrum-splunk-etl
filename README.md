# dtrum_splunk_etl

Python ETL helpers to pull Dynatrace RUM user-session data via USQL and prepare
it for downstream analysis (e.g. FlussoP1 funnel filtering, Splunk export).

## What it does

1. **Discovery** – find tagged user sessions (`userId` not null) whose actions
   match FlussoP1 name prefixes.
2. **Session actions** – fetch action timelines for those session ids.
3. **Complete fetch** – `DynatraceUSQLClient.fetch` walks a time range with an
   adaptive window so results are not truncated or sampled.
4. **Funnel reconstruction** – rebuild FlussoP1 emission attempts per tagged
   `userId` from the raw action stream (`funnel/` package).
5. **Local cache** – two-tier Parquet cache per calendar day: staging chunks during
   fetch, consolidated `actions.parquet` after a complete run; watermark per day.

## Layout

| File | Role |
|------|------|
| `config.example.py` | Committable template; copy to local `config.py` |
| `config.py` | Local settings (gitignored): prefixes, columns, logging |
| `queries.py` | USQL string builders (`discovery_query`, `session_actions_query`) |
| `client.py` | Dynatrace USQL `/table` client with adaptive time windows |
| `cache.py` | Parquet + JSON sidecar cache and watermark |
| `utils.py` | ISO datetime to UTC epoch ms |
| `main.py` | CLI: fetch actions and optionally build flows |
| `build_flows.py` | CLI: reconstruct flows from Parquet or cache chunks |
| `funnel/` | FlussoP1 breakdown, tagging, per-flusso metrics |
| `logs/` | Per-run log files (gitignored) |

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.py config.py
```

Edit `config.py` and set `DISCOVERY_NAME_PREFIXES` to your application action-name
prefixes (trailing-only, no leading `%`).

Create a `.env` in the project root:

```
DT_ENV_ID=your-environment-id
DT_API_TOKEN=your-api-token
```

The token needs the `DTAQLAccess` (User sessions) scope.

## CLI usage

Fetch one calendar day (24h, Europe/Rome by default):

```bash
python3 main.py \
  --start 2026-07-21T00:00:00+02:00 \
  --end 2026-07-22T00:00:00+02:00 \
  --build-flows
```

Fetch and write actions only:

```bash
python3 main.py --start 2026-07-21T00:00:00+02:00 --end 2026-07-22T00:00:00+02:00
```

Rebuild flows from an existing actions Parquet:

```bash
python3 main.py --input output/user_actions_2026-07-21.parquet --build-flows
```

Rebuild flows from cache (consolidated or staging):

```bash
python3 build_flows.py --day 2026-07-21 --stem 2026-07-21
```

Useful options:

| Flag | Meaning |
|------|---------|
| `--force` | Ignore Parquet cache and re-fetch |
| `--chunk-size N` | Session ids per actions query (default 40) |
| `--build-flows` | Run funnel reconstruction and write flow Parquets |
| `--input PATH` | Skip fetch; read actions from Parquet |
| `--log-dir DIR` | Log directory (default `logs/`) |
| `--cache-dir DIR` | Override cache root (default `.cache/usql`) |
| `--keep-staging` | Keep `_staging/` chunks after successful fetch |
| `--day YYYY-MM-DD` | Day bucket for `build_flows` / Splunk export from cache |

Pipeline:

1. Discovery query (tagged users + name prefixes), cached as `discovery.parquet`.
2. Session ids split into chunks; each chunk is fetched into `_staging/`.
3. On success: chunks are merged into `actions.parquet` and staging is cleared
   (unless `--keep-staging`).
4. With `--build-flows`: `funnel.reconstruct_flows()` writes `flows_*`,
   `matched_actions_*`, and `step_breakdown_*` under `output/`.

## Funnel reconstruction

The `funnel/` package ports the faithful FlussoP1 breakdown (C#
`StepBreakdownProcessor`). It assumes:

- Every action row has a Dynatrace **user tag** (`userId`); discovery already
  filters `usersession.userId IS NOT NULL`.
- Aggregation is per **userId** (not `sessionId`); a flusso may span sessions.
- `flusso_id` day bucketing uses `FUNNEL_DAY_TZ` in `config.py` (default
  `Europe/Rome`) for 24h calendar-day windows.

```python
from funnel import reconstruct_flows

result = reconstruct_flows(actions_df)
result.flows          # one row per emission attempt
result.matched        # actions tagged with flusso_id / step_index
result.step_breakdown # one row per flusso × funnel step
```

Logs (under `logs/`):

- `etl_<utc>_issues.log` – WARNING+ only (incomplete windows, sampling shrinks,
  fetch failures).
- `etl_<utc>.log` – full run trace (DEBUG+), including USQL explain messages.

`fetch` treats a window as incomplete when `len(rows) >= page_size` or
`extrapolationLevel != 1`, shrinks the time window, and logs a WARNING. If the
minimum window is still incomplete, the CLI exits with an error.

## USQL limits (important)

- Table API: at most **5000** rows per response; use time splitting (already in
  `fetch`) rather than relying on `pageOffset` past that cap.
- Without `LIMIT` in the query, Dynatrace applies an implicit limit of **50**.
- Discovery filters on `useraction.startTime` via `{start_ms}`/`{end_ms}`
  placeholders (substituted per window by the client). No `GROUP BY`/`TOP`:
  duplicate `sessionId` rows are removed with `drop_duplicates` after fetch.
- `extrapolationLevel != 1` means sampled data; `fetch` shrinks the window.
- Prefer trailing-only `LIKE 'prefix%'` (no leading `%`) for performance.

Use `page_size=5000` (default) for discovery and actions so truncation matches
`LIMIT 5000`.

## Cache usage (optional)

Per-day layout under `.cache/usql/{YYYY-MM-DD}/`:

- `discovery.parquet` – discovery result
- `actions.parquet` – consolidated actions (written when fetch completes)
- `_staging/` – per-chunk files during fetch (removed after consolidate unless `--keep-staging`)

```python
from cache import UsqlCache, day_key_from_ms

day = day_key_from_ms(start_ms)
cache = UsqlCache(day)
df = cache.get_discovery(query, start_ms, end_ms)
if df is None:
    df = client.fetch(query, start_ms, end_ms, page_size=5000)
    cache.put_discovery(df, query, start_ms, end_ms)

# After all action chunks: cache.consolidate_actions(actions, start_ms, end_ms)
wm = cache.get_watermark(query)
```

Legacy flat files under `.cache/usql/` can be moved with:

```bash
python3 -c "from cache import migrate_legacy_flat_cache; print(migrate_legacy_flat_cache())"
```

## Splunk ingest (planned)

`splunk_ingest` under `test_porting/` will be wired to `funnel.reconstruct_flows()`
once the continuous 24h pipeline is stable.
