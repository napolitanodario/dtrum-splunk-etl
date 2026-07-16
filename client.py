"""Low-level Dynatrace USQL table API client."""

import time
import logging
from typing import Optional
from urllib.parse import urlencode, quote

import requests
import pandas as pd

log = logging.getLogger("usat")

PAGE_SIZE = 5000        # API maximum
MAX_PAGES = 200         # safety cap for unbounded queries
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

        print(start_ms, end_ms, offset_utc_min, page_size, page_offset)

        params = {
            "query": query,
            "startTimestamp": start_ms,
            "endTimestamp": end_ms
        }

        if offset_utc_min:
            params["offsetUTC"] = offset_utc_min

        if page_size:
            params["pageSize"] = page_size

        if page_offset:
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

    """
    def fetch(
            self, query: str, start_ms: int, end_ms: int,
            offset_utc_min: Optional[int] = None, paginated: bool = False
    ) -> pd.DataFrame:

        all_rows = []
        columns = None
        offset = 0

        while True:
            data = self._get_page(query, start_ms, end_ms, offset_utc_min, PAGE_SIZE, offset)
            if columns is None:
                columns = data.get("columnNames", [])
            rows = data.get("values", [])
            all_rows.extend(rows)

            if data.get("extrapolationLevel") != 1:
                log.warning("extrapolationLevel=%d - data is sampled",data["extrapolationLevel"])

            if not paginated or len(rows) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            if offset // PAGE_SIZE >= MAX_PAGES:
                log.warning("Reached MAX_PAGES=%d, stopping pagination", MAX_PAGES)
                break

        return pd.DataFrame(all_rows, columns=columns)
    """

    def fetch(
            self, query: str, start_ms: int, end_ms: int,
            offset_utc_min: Optional[int] = None,
            bucket_size_min: int = 10,  # Configurabile: base di 10 minuti
            min_bucket_ms: int = 60000  # Limite minimo di 1 minuto (60 * 1000 ms)
    ) -> pd.DataFrame:

        all_rows = []
        columns = None

        # Convertiamo la dimensione del bucket configurata in millisecondi
        bucket_ms = bucket_size_min * 60 * 1000
        current_bucket_ms = bucket_ms

        current_start = start_ms

        while current_start < end_ms:
            # Calcoliamo la fine del bucket temporale corrente
            current_end = min(current_start + current_bucket_ms, end_ms)

            # Facciamo la richiesta senza usare l'offset di paginazione (lo passiamo come 0)
            data = self._get_page(query, current_start, current_end, offset_utc_min, PAGE_SIZE, 0)

            if columns is None:
                columns = data.get("columnNames", [])

            rows = data.get("values", [])

            if data.get("extrapolationLevel") != 1:
                log.warning("extrapolationLevel=%d - data is sampled for interval %s to %s",
                            data.get("extrapolationLevel"), current_start, current_end)

            # CONTROLLO LIMITE E RIDUZIONE BUCKET
            if len(rows) >= PAGE_SIZE:
                updated_bucket_ms = current_bucket_ms // 2

                if updated_bucket_ms < min_bucket_ms:
                    msg = "Bucket ms too small, stopping pagination"
                    raise RuntimeError(msg)

                # Altrimenti applichiamo la riduzione e riproviamo
                current_bucket_ms = updated_bucket_ms
                log.info("Limite PAGE_SIZE raggiunto (%d righe). Riduco il bucket a %d ms e riprovo...",
                         len(rows), current_bucket_ms)
                continue  # Salta l'aggiunta dei dati e riprova con lo stesso current_start
            else:
                # SUCCESSO: Il bucket era abbastanza piccolo
                all_rows.extend(rows)
                current_start = current_end

                # Resettiamo il bucket alla dimensione originale configurata
                # current_bucket_ms = bucket_ms

        return pd.DataFrame(all_rows, columns=columns)
