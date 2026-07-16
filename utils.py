
from datetime import datetime

def datetime_to_timestamp_ms_utc(dt: datetime) -> int:
    """Converts a datetime object (with timezone) in timestamp Unix milliseconds."""
    return int(dt.timestamp() * 1000)


# example for italian time: "2026-07-14T15:30:00+02:00"
def parse_iso_string(iso_datetime_string: str) -> datetime:
    """Converts an ISO 8601 string into a datetime object."""
    return datetime.fromisoformat(iso_datetime_string)

def iso_string_to_timestamp_ms_utc(iso_datetime_string: str) -> int:
    """Converts an ISO 8601 string into a timestamp Unix milliseconds."""
    return datetime_to_timestamp_ms_utc(parse_iso_string(iso_datetime_string))