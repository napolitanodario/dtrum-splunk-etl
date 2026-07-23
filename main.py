"""Example entrypoint: discover FlussoP1 sessions in a time range."""

from config import (
    DISCOVERY_NAME_PREFIXES,
    DISCOVERY_TOP_N,
    get_credentials,
)
from client import DynatraceUSQLClient
from queries import discovery_query
from utils import iso_string_to_timestamp_ms_utc

# Example analysis window (Europe/Rome offset in the ISO string).
TIME_START = iso_string_to_timestamp_ms_utc("2026-07-13T09:00:00+02:00")
TIME_END = iso_string_to_timestamp_ms_utc("2026-07-13T10:00:00+02:00")


def main() -> None:
    env_id, api_token = get_credentials()
    client = DynatraceUSQLClient(env_id, api_token)

    query = discovery_query(DISCOVERY_NAME_PREFIXES, top_n=DISCOVERY_TOP_N)
    # page_size must match TOP(n) so fetch detects truncated aggregations.
    result = client.fetch(
        query=query,
        start_ms=TIME_START,
        end_ms=TIME_END,
        page_size=DISCOVERY_TOP_N,
    )

    print(query)
    print(result)
    print(f"sessions: {len(result)}")


if __name__ == "__main__":
    main()
