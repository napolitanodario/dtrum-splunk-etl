"""Unattended daily orchestrator: USQL fetch -> Splunk ingest -> cache prune."""

__all__ = ["run_daily"]

from .orchestrate import run_daily
