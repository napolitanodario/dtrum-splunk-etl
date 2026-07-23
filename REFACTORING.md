# Refactoring: Robust USQL Fetch

## Goal

Guarantee that, given a time frame, a USQL query returns all matching data.
The current fetch can silently return incomplete or sampled results.

## Background (Dynatrace USQL constraints)

- The `/userSessionQueryLanguage/table` endpoint returns at most 5000 rows per
  request. This is a hard cap; `pageOffset` and `pageSize` cannot exceed it.
  Splitting the time frame is the only reliable way to retrieve everything.
- Without an explicit `LIMIT` clause the API applies an implicit limit of 50 rows.
- `extrapolationLevel != 1` means the response is sampled and not exact. This can
  happen even below 5000 rows and must be treated as a signal to shrink the time
  frame, not merely logged.
- The time frame filter applies to the `usersession` table by default, even for
  queries over `useraction`. Time splitting therefore segments by session start
  time, and a final de-duplication step covers boundary overlaps.

## Design decisions

- Keep the existing USQL approach and improve the time-window strategy (no
  migration to the User Session Export stream).
- Fail fast: if a window reaches the minimum size and is still truncated or
  sampled, raise an error instead of storing incomplete data.

## Planned changes

### 1. Rewrite `DynatraceUSQLClient.fetch()` in `client.py`

Replace the current fetch (and remove the old commented-out fetch) with an
adaptive time-window algorithm:

- Split trigger: a window is considered incomplete when it returns at least
  `page_size` rows or when `extrapolationLevel != 1`.
- Shrink on incomplete windows by halving the window size and retrying the same
  start, down to a configurable floor.
- Grow adaptively: after a comfortably small window, increase the window size
  again to reduce the number of requests. This fixes the current behavior where
  the window never returns to its original size.
- Use a millisecond-level floor (much smaller than the current one-minute floor)
  because timestamps are expressed in milliseconds.
- Fail fast: raise a descriptive error when a window is still incomplete at the
  minimum size, including the window bounds, row count, and extrapolation level.
- De-duplicate the accumulated rows at the end to protect against boundary
  overlaps.

Proposed signature:

```python
def fetch(self, query, start_ms, end_ms, offset_utc_min=None,
          initial_window_min=10, min_window_ms=1000, page_size=PAGE_SIZE):
```

### 2. Fix `_get_page()` in `client.py`

- Remove the debug `print` and use structured debug logging instead.
- Send the query parameters when they are explicitly provided, so that a value of
  zero is not dropped by a truthiness check:

```python
if offset_utc_min is not None:
    params["offsetUTC"] = offset_utc_min
if page_size is not None:
    params["pageSize"] = page_size
if page_offset is not None:
    params["pageOffset"] = page_offset
```

### 3. Add an explicit `LIMIT` in `queries.py`

Append `LIMIT 5000` to `discovery_query()` and `session_actions_query()` to
avoid the implicit 50-row limit and align with `page_size`.

### 4. No functional change in `main.py`

It keeps calling `fetch()` with defaults and automatically benefits from the new
completeness guarantees and de-duplication.

## Resulting guarantee

Every accepted window satisfies both conditions: fewer than 5000 rows and
`extrapolationLevel == 1` (exact, non-truncated data). The only unrecoverable
case (more matching rows than the cap within the smallest window, or persistent
sampling at the floor) becomes an explicit error rather than a silent data loss.

## Out of scope (future work)

- For genuinely large exports, the native alternative is the Dynatrace User
  Session Export stream.
- Optional completeness check via a `COUNT(*)` per window compared with the
  number of retrieved rows.
