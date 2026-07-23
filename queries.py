"""USQL query builders for session-level and discovery fetches."""

from typing import Iterable

# Action-level columns pulled for every fetched session. Aliases match the names
# the analysis layer expects (see analysis.loading). The trailing block holds optional
# enrichment fields; the analysis layer degrades gracefully when they are absent (old cache).
_ACTION_COLUMNS = """
  usersession.userId AS userId,
  usersession.userSessionId AS sessionId,
  useraction.name AS name,
  useraction.type AS type,
  useraction.duration AS duration,
  useraction.startTime AS startTime,
  useraction.endTime AS endTime,
  useraction.networkTime AS networkTime,
  useraction.frontendTime AS frontendTime,
  useraction.serverTime AS serverTime,
  useraction.targetUrl AS targetUrl,
  useraction.apdexCategory AS apdex,
  useraction.requestErrorCount AS requestErrorCount,
  useraction.javascriptErrorCount AS javascriptErrorCount,
  useraction.visuallyCompleteTime AS visuallyCompleteTime,
  useraction.documentInteractiveTime AS domInteractiveTime,
  usersession.userActionCount AS sessionActionCount,
  usersession.totalErrorCount AS sessionTotalErrors,
  usersession.duration AS sessionDuration,
  usersession.bounce AS bounce,
  usersession.browserType AS browserType,
  usersession.country AS country,
  usersession.city AS city
""".strip()


def _quote_list(values: Iterable[str]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def session_actions_query(session_ids: Iterable[str]) -> str:
    """All actions for the given sessions, ordered chronologically."""
    ids = _quote_list(session_ids)
    return f"""
SELECT
{_ACTION_COLUMNS}
FROM useraction
WHERE usersession.userSessionId IN ({ids})
ORDER BY startTime ASC
LIMIT 5000
""".strip()


def discovery_query(name_like: Iterable[str]) -> str:
    """Distinct session ids whose actions match any of the name fragments."""
    clauses = " OR ".join(f"useraction.name LIKE '{frag}'" for frag in name_like)
    return f"""
SELECT
  usersession.userSessionId AS sessionId
FROM useraction
WHERE usersession.userId IS NOT NULL
  AND ({clauses})
GROUP BY sessionId
LIMIT 5000
""".strip()
