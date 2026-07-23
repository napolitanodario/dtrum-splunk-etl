"""USQL query builders for discovery and session-action fetches.

Builders return query strings only; execution is handled by DynatraceUSQLClient.
Discovery comes before session_actions in the ETL pipeline.
"""

from typing import Iterable, Mapping


def _quote_list(values: Iterable[str]) -> str:
    """Quote string values for a USQL IN (...) list."""
    return ", ".join(f"'{v}'" for v in values)


def _select_list(columns: Mapping[str, str]) -> str:
    """Build a SELECT list from alias -> USQL expression."""
    return ",\n  ".join(f"{expr} AS {alias}" for alias, expr in columns.items())


def discovery_query(name_prefixes: Iterable[str], top_n: int = 1000) -> str:
    """Distinct session ids for identified users hitting any name prefix.

    Uses trailing-only LIKE patterns (prefix%). TOP(top_n) raises the USQL
    aggregation cap on userSessionId (max 1000). Pair with client.fetch
    page_size=top_n and time-window splitting for complete coverage.
    """
    clauses = " OR ".join(
        f"useraction.name LIKE '{prefix}%'" for prefix in name_prefixes
    )
    return f"""
SELECT TOP(usersession.userSessionId, {top_n}) AS sessionId
FROM useraction
WHERE usersession.userId IS NOT NULL
  AND usersession.userId != ''
  AND (
       {clauses}
  )
GROUP BY sessionId
LIMIT {top_n}
""".strip()


def session_actions_query(
        session_ids: Iterable[str],
        columns: Mapping[str, str],
) -> str:
    """All actions for the given sessions, ordered by startTime ascending.

    LIMIT 5000 matches the table API row cap. Callers with many sessions or
    dense actions should chunk session_ids and/or rely on time splitting.
    """
    ids = _quote_list(session_ids)
    select_list = _select_list(columns)
    return f"""
SELECT
  {select_list}
FROM useraction
WHERE usersession.userSessionId IN ({ids})
ORDER BY startTime ASC
LIMIT 5000
""".strip()
