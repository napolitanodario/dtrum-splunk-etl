
from config import *
from client import DynatraceUSQLClient
from queries import discovery_query
from utils import *

env_id, api_token = get_credentials()

query = discovery_query(DISCOVERY_NAME_LIKE)
print(query)


time_start = iso_string_to_timestamp_ms_utc("2026-07-13T09:00:00+02:00")
time_end = iso_string_to_timestamp_ms_utc("2026-07-13T10:00:00+02:00")


def main():


    client = DynatraceUSQLClient(env_id, api_token)
    result = client.fetch(
        query=query,
        start_ms=time_start,
        end_ms=time_end
    )



    result.to_csv("test2.csv")

    unique_sessions = set(result['sessionId'])
    print(len(unique_sessions))



if __name__ == '__main__':
    main()
