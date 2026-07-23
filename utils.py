"""Time helpers for Dynatrace USQL timestamps (UTC milliseconds)."""

from datetime import datetime


def parse_iso_string(iso_datetime_string: str) -> datetime:
    """Parse an ISO 8601 string into a timezone-aware datetime.

    Example: 2026-07-14T15:30:00+02:00
    """
    return datetime.fromisoformat(iso_datetime_string)


def datetime_to_timestamp_ms_utc(dt: datetime) -> int:
    """Convert a timezone-aware datetime to Unix epoch milliseconds (UTC)."""
    return int(dt.timestamp() * 1000)


def iso_string_to_timestamp_ms_utc(iso_datetime_string: str) -> int:
    """Parse an ISO 8601 string and return Unix epoch milliseconds (UTC)."""
    return datetime_to_timestamp_ms_utc(parse_iso_string(iso_datetime_string))
