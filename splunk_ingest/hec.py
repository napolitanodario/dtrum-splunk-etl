"""Standalone Splunk HEC client: batched, gzipped, retrying. No project coupling."""

from __future__ import annotations

import gzip
import json
import logging
import time
from typing import Iterable

import requests

log = logging.getLogger("splunk_ingest")


class HECClient:
    """Ships event envelopes to a Splunk HTTP Event Collector."""

    def __init__(
        self,
        url: str,
        token: str,
        *,
        verify: bool | str = True,
        timeout: int = 60,
        batch_size: int = 500,
        max_bytes: int = 900_000,
        gzip_payload: bool = True,
        retry_attempts: int = 3,
        retry_backoff: int = 5,
        channel: str | None = None,
    ):
        self.url = url
        self.verify = verify
        self.timeout = timeout
        self.batch_size = batch_size
        self.max_bytes = max_bytes
        self.gzip_payload = gzip_payload
        self.retry_attempts = retry_attempts
        self.retry_backoff = retry_backoff
        self.session = requests.Session()
        headers = {"Authorization": f"Splunk {token}"}
        if channel:
            headers["X-Splunk-Request-Channel"] = channel
        self.session.headers.update(headers)

    def send(self, events: Iterable[dict]) -> int:
        """Serialize and POST events in batches. Returns the number sent."""
        total = 0
        for batch in self._batches(events):
            self._post(batch)
            total += len(batch)
        if total:
            log.info("HEC: sent %d events", total)
        return total

    def _batches(self, events: Iterable[dict]):
        batch: list[str] = []
        size = 0
        for e in events:
            line = json.dumps(e, ensure_ascii=False, separators=(",", ":"))
            b = len(line.encode("utf-8")) + 1
            if batch and (len(batch) >= self.batch_size or size + b > self.max_bytes):
                yield batch
                batch, size = [], 0
            batch.append(line)
            size += b
        if batch:
            yield batch

    def _post(self, lines: list[str]) -> None:
        payload = "\n".join(lines).encode("utf-8")
        headers: dict[str, str] = {}
        data = payload
        if self.gzip_payload:
            data = gzip.compress(payload)
            headers["Content-Encoding"] = "gzip"
        for attempt in range(1, self.retry_attempts + 1):
            resp = self.session.post(
                self.url,
                data=data,
                headers=headers,
                timeout=self.timeout,
                verify=self.verify,
            )
            if resp.status_code == 200:
                return
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < self.retry_attempts:
                log.warning(
                    "HEC HTTP %s - retry %d/%d in %ds",
                    resp.status_code,
                    attempt,
                    self.retry_attempts,
                    self.retry_backoff,
                )
                time.sleep(self.retry_backoff)
                continue
            resp.raise_for_status()
        raise RuntimeError("HEC max retries exceeded")
