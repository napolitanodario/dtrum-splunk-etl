# dtrum_splunk_etl

Python ETL helpers to pull Dynatrace RUM user-session data via USQL and prepare
it for downstream analysis (e.g. FlussoP1 funnel filtering, Splunk export).

## What it does

1. **Discovery** – find identified user sessions whose actions match FlussoP1
   name prefixes.
2. **Session actions** – fetch full action timelines for those session ids
   (query builder ready; wire into the pipeline as needed).
3. **Complete fetch** – `DynatraceUSQLClient.fetch` walks a time range with an
   adaptive window so results are not truncated or sampled.
4. **Local cache** – optional Parquet cache with provenance sidecars and a
   per-query watermark for continuous runs.

## Layout

| File | Role |
|------|------|
| `config.example.py` | Committable template; copy to local `config.py` |
| `config.py` | Local settings (gitignored): prefixes, columns, logging |
| `queries.py` | USQL string builders (`discovery_query`, `session_actions_query`) |
| `client.py` | Dynatrace USQL `/table` client with adaptive time windows |
| `cache.py` | Parquet + JSON sidecar cache and watermark |
| `utils.py` | ISO datetime to UTC epoch ms |
| `main.py` | CLI: discover sessions and download/cache user actions |
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

```bash
python3 main.py \
  --start 2026-07-13T09:00:00+02:00 \
  --end 2026-07-13T10:00:00+02:00
```

Useful options:

| Flag | Meaning |
|------|---------|
| `--force` | Ignore Parquet cache and re-fetch |
| `--chunk-size N` | Session ids per actions query (default 40) |
| `--log-dir DIR` | Log directory (default `logs/`) |
| `--cache-dir DIR` | Override cache root (default `.cache/usql`) |

Pipeline:

1. Discovery query (identified users + name prefixes), cached as `discovery`.
2. Session ids split into chunks; each chunk fetches actions and is cached.
3. Results are concatenated and de-duplicated in memory for the run summary.

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

```python
from cache import UsqlCache

cache = UsqlCache()
df = cache.get(query, start_ms, end_ms, label="discovery")
if df is None:
    df = client.fetch(query, start_ms, end_ms, page_size=5000)
    cache.put(df, query, start_ms, end_ms, label="discovery")

# Continuous ETL: resume after the last covered end_ms
wm = cache.get_watermark(query)
```

Cache files live under `.cache/usql/` (gitignored).

## Typical next steps

1. Discover session ids for the desired window.
2. Chunk ids and call `session_actions_query(ids, ACTION_COLUMNS)`.
3. Fetch actions (with time splitting / chunking as needed).
4. Apply funnel / business filters in Python on the full action set.
