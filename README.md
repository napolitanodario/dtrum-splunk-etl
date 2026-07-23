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
| `main.py` | Example: run discovery for a sample hour |

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

## Run the discovery example

```bash
python3 main.py
```

`main.py` builds `discovery_query(...)`, calls `fetch` with `page_size` equal to
`DISCOVERY_TOP_N` (1000), and prints the session id DataFrame.

## USQL limits (important)

- Table API: at most **5000** rows per response; use time splitting (already in
  `fetch`) rather than relying on `pageOffset` past that cap.
- Without `LIMIT` in the query, Dynatrace applies an implicit limit of **50**.
- `GROUP BY` on `userSessionId` needs `TOP(field, n)` with **n <= 1000**.
- `extrapolationLevel != 1` means sampled data; `fetch` shrinks the window.
- Prefer trailing-only `LIKE 'prefix%'` (no leading `%`) for performance.

For discovery queries that use `TOP(..., 1000)`, always pass `page_size=1000`
into `fetch` so a full page is treated as potentially incomplete.

## Cache usage (optional)

```python
from cache import UsqlCache

cache = UsqlCache()
df = cache.get(query, start_ms, end_ms, label="discovery")
if df is None:
    df = client.fetch(query, start_ms, end_ms, page_size=1000)
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
