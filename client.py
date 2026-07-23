"""Low-level Dynatrace USQL table API client."""

import time
import logging
from typing import Optional
from urllib.parse import urlencode, quote

import requests
import pandas as pd

log = logging.getLogger("usat")

PAGE_SIZE = 5_000        # API maximum rows per request
MIN_WINDOW_MS = 10_000    # smallest time window before giving up (1 second)
INITIAL_WINDOW_MIN = 10  # starting time window size in minutes
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 5       # seconds between retries on 429/503


class DynatraceUSQLClient:
    def __init__(self, env_id: str, api_token: str, timeout: int = 60):
        self.base_url = (
            f"https://{env_id}.live.dynatrace.com/api/v1/userSessionQueryLanguage/table"
        )
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Api-Token {api_token}",
            "Accept": "application/json",
        })

    # Dynatrace doc: https://docs.dynatrace.com/docs/shortlink/api-usql-table#parameters
    def _get_page(
            self, query: str, start_ms: int, end_ms: int,
            offset_utc_min: Optional[int] = None,
            page_size: Optional[int] = None,
            page_offset: Optional[int] = None
    ) -> dict:

        log.debug("request window: start=%s end=%s offsetUTC=%s pageSize=%s pageOffset=%s",
                  start_ms, end_ms, offset_utc_min, page_size, page_offset)

        params = {
            "query": query,
            "startTimestamp": start_ms,
            "endTimestamp": end_ms
        }

        if offset_utc_min is not None:
            params["offsetUTC"] = offset_utc_min

        if page_size is not None:
            params["pageSize"] = page_size

        if page_offset is not None:
            params["pageOffset"] = page_offset

        # quote_via=quote keeps spaces as %20 (the API rejects '+').
        qs = urlencode(params, quote_via=quote)
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            resp = self.session.get(f"{self.base_url}?{qs}", timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()

            elif resp.status_code in (429, 503) and attempt < RETRY_ATTEMPTS:
                log.warning("HTTP %s - retry %d/%d in %ds",
                            resp.status_code, attempt, RETRY_ATTEMPTS, RETRY_BACKOFF)
                time.sleep(RETRY_BACKOFF)
                continue

            resp.raise_for_status()

        raise RuntimeError("Max retries exceeded")

    def fetch(
            self, query: str, start_ms: int, end_ms: int,
            offset_utc_min: Optional[int] = None,
            initial_window_min: int = INITIAL_WINDOW_MIN,
            min_window_ms: int = MIN_WINDOW_MS,
            page_size: int = PAGE_SIZE,
    ) -> pd.DataFrame:
        """Fetch every row a query matches within a time frame.

        The API caps a single response at PAGE_SIZE rows and may return sampled
        (extrapolated) data. Since paging cannot go past that cap, the only
        reliable way to retrieve everything is to split the time frame. This
        walks the range with an adaptive window: it shrinks the window whenever a
        response is truncated or sampled, and grows it again once responses are
        comfortably small, so completeness is guaranteed without wasting requests.
        """
        all_rows = []
        columns = None

        initial_window_ms = initial_window_min * 60 * 1000
        window_ms = initial_window_ms
        current_start = start_ms

        while current_start < end_ms:
            current_end = min(current_start + window_ms, end_ms)

            data = self._get_page(
                query, current_start, current_end, offset_utc_min, page_size, 0
            )

            if columns is None:
                columns = data.get("columnNames", [])

            rows = data.get("values", [])
            extrapolation_level = data.get("extrapolationLevel")

            truncated = len(rows) >= page_size
            sampled = extrapolation_level != 1

            # An incomplete window means the data is untrustworthy; shrink and retry
            # the same start so nothing is missed.
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
                log.info("Incomplete window (rows=%d, extrapolationLevel=%s). "
                         "Shrinking window to %d ms and retrying.",
                         len(rows), extrapolation_level, window_ms)
                continue

            all_rows.extend(rows)
            current_start = current_end

            # Grow the window back when responses stay well below the cap, to keep
            # the number of requests low over sparse stretches.
            if len(rows) < page_size // 2:
                window_ms = min(window_ms * 2, initial_window_ms)

        df = pd.DataFrame(all_rows, columns=columns)
        return df.drop_duplicates().reset_index(drop=True)
