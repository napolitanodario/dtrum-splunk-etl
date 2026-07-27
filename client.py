"""Low-level Dynatrace USQL table API client.

Fetches complete, non-sampled result sets by walking a time range with an
adaptive window. The table endpoint caps each response (typically 5000 rows)
and may return extrapolated data; paging past that cap is not reliable, so
this client shrinks the time window until every accepted page is exact.
"""

import logging
import time
from typing import Optional
from urllib.parse import quote, urlencode

import pandas as pd
import requests

log = logging.getLogger("usat")

# Hard cap on rows returned by a single /table response.
PAGE_SIZE = 5_000
# Starting window size for the adaptive walk (minutes).
INITIAL_WINDOW_MIN = 10
# Smallest window before fetch fails instead of returning incomplete data.
MIN_WINDOW_MS = 1_000
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 5  # seconds between retries on 429/503


class DynatraceUSQLClient:
    """HTTP client for GET /api/v1/userSessionQueryLanguage/table."""

    def __init__(self, env_id: str, api_token: str, timeout: int = 60):
        self.base_url = (
            f"https://{env_id}.live.dynatrace.com"
            f"/api/v1/userSessionQueryLanguage/table"
        )
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Api-Token {api_token}",
            "Accept": "application/json",
        })

    def fetch(
            self,
            query: str,
            start_ms: int,
            end_ms: int,
            offset_utc_min: Optional[int] = None,
            initial_window_min: int = INITIAL_WINDOW_MIN,
            min_window_ms: int = MIN_WINDOW_MS,
            page_size: int = PAGE_SIZE,
    ) -> pd.DataFrame:
        """Fetch every matching row in [start_ms, end_ms].

        Walks the range with an adaptive time window:
        - shrinks and retries the same start when the page is truncated
          (len(rows) >= page_size) or sampled (extrapolationLevel != 1);
        - grows again toward the initial size when responses stay small;
        - de-duplicates rows at the end (window boundaries and repeated
          sessionId rows from discovery without GROUP BY).

        If the query contains {start_ms}/{end_ms} placeholders (discovery),
        they are replaced with the current window bounds so action-time
        filters stay aligned with the API timeframe.
        """
        all_rows = []
        columns = None

        initial_window_ms = initial_window_min * 60 * 1000
        window_ms = initial_window_ms
        current_start = start_ms

        while current_start < end_ms:
            current_end = min(current_start + window_ms, end_ms)

            data = self._get_page(
                query, current_start, current_end, offset_utc_min, page_size
            )

            if columns is None:
                columns = data.get("columnNames", [])

            rows = data.get("values", [])
            extrapolation_level = data.get("extrapolationLevel")

            truncated = len(rows) >= page_size
            sampled = extrapolation_level != 1

            # Incomplete data: shrink the window and retry from the same start.
            if truncated or sampled:
                if window_ms <= min_window_ms:
                    raise RuntimeError(
                        f"Cannot retrieve complete data for window "
                        f"[{current_start}, {current_end}] at the minimum size "
                        f"({min_window_ms} ms): rows={len(rows)}, "
                        f"extrapolationLevel={extrapolation_level}. "
                        f"Add more selective filters to the query."
                    )
                window_ms = max(window_ms // 2, min_window_ms)
                # WARNING so file issue logs capture sampling / truncation.
                log.warning(
                    "Incomplete window [%s, %s]: rows=%d, "
                    "extrapolationLevel=%s, page_size=%d. "
                    "Shrinking window to %d ms and retrying.",
                    current_start, current_end, len(rows),
                    extrapolation_level, page_size, window_ms,
                )
                continue

            all_rows.extend(rows)
            current_start = current_end

            # Widen again on sparse stretches to reduce request count.
            if len(rows) < page_size // 2:
                window_ms = min(window_ms * 2, initial_window_ms)

        df = pd.DataFrame(all_rows, columns=columns)
        return df.drop_duplicates().reset_index(drop=True)

    # Dynatrace doc: https://docs.dynatrace.com/docs/shortlink/api-usql-table
    def _get_page(
            self,
            query: str,
            start_ms: int,
            end_ms: int,
            offset_utc_min: Optional[int] = None,
            page_size: Optional[int] = None,
    ) -> dict:
        """Execute one USQL table request for a single time window.

        Always requests explain=true and logs Dynatrace explanations.
        Retries on HTTP 429/503 with a fixed backoff.
        Substitutes {start_ms}/{end_ms} in the query when present.
        """
        bound_query = (
            query
            .replace("{start_ms}", str(start_ms))
            .replace("{end_ms}", str(end_ms))
        )
        log.debug(
            "request window: start=%s end=%s offsetUTC=%s pageSize=%s",
            start_ms, end_ms, offset_utc_min, page_size,
        )

        params = {
            "query": bound_query,
            "startTimestamp": start_ms,
            "endTimestamp": end_ms,
            "explain": "true",
        }
        if offset_utc_min is not None:
            params["offsetUTC"] = offset_utc_min
        if page_size is not None:
            params["pageSize"] = page_size

        # quote_via=quote keeps spaces as %20 (the API rejects '+').
        qs = urlencode(params, quote_via=quote)
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            resp = self.session.get(f"{self.base_url}?{qs}", timeout=self.timeout)
            if resp.status_code == 200:
                data = resp.json()
                for explanation in data.get("explanations") or []:
                    log.info("USQL explain: %s", explanation)
                return data

            if resp.status_code in (429, 503) and attempt < RETRY_ATTEMPTS:
                log.warning(
                    "HTTP %s - retry %d/%d in %ds",
                    resp.status_code, attempt, RETRY_ATTEMPTS, RETRY_BACKOFF,
                )
                time.sleep(RETRY_BACKOFF)
                continue

            resp.raise_for_status()

        raise RuntimeError("Max retries exceeded")
