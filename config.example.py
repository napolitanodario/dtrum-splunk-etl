"""Example runtime configuration (safe to commit).

Copy this file to config.py and adapt DISCOVERY_NAME_PREFIXES to your app:

    cp config.example.py config.py

config.py is gitignored: keep environment-specific filters there.
Credentials still come from .env (DT_ENV_ID, DT_API_TOKEN).
"""

import logging
import os

from dotenv import load_dotenv

# Trailing-only action-name prefixes for discovery (no leading wildcard).
# Used as LIKE '{prefix}%' in discovery_query. Replace with your own paths.
DISCOVERY_NAME_PREFIXES = [
    "/myapp/checkout",
    "loading of page /myapp/checkout",
    "Loading of page /MyApp/Checkout",
]

# alias -> USQL expression for session_actions_query SELECT list.
ACTION_COLUMNS = {
    "userId": "usersession.userId",
    "sessionId": "usersession.userSessionId",
    "name": "useraction.name",
    "type": "useraction.type",
    "duration": "useraction.duration",
    "startTime": "useraction.startTime",
    "endTime": "useraction.endTime",
    "networkTime": "useraction.networkTime",
    "frontendTime": "useraction.frontendTime",
    "serverTime": "useraction.serverTime",
    "targetUrl": "useraction.targetUrl",
    "apdex": "useraction.apdexCategory",
    "requestErrorCount": "useraction.requestErrorCount",
    "javascriptErrorCount": "useraction.javascriptErrorCount",
    "visuallyCompleteTime": "useraction.visuallyCompleteTime",
    "domInteractiveTime": "useraction.documentInteractiveTime",
    "sessionActionCount": "usersession.userActionCount",
    "sessionTotalErrors": "usersession.totalErrorCount",
    "sessionDuration": "usersession.duration",
    "bounce": "usersession.bounce",
    "browserType": "usersession.browserType",
    "country": "usersession.country",
    "city": "usersession.city",
}

# Row limit for discovery LIMIT clause (align with client PAGE_SIZE = 5000).
DISCOVERY_LIMIT = 5000

# IANA timezone for flusso_id day bucketing (calendar day for 24h ETL windows).
FUNNEL_DAY_TZ = "Europe/Rome"

load_dotenv()

# Logging is configured by the CLI (main.setup_logging). Keep a named logger
# for modules that import config early.
log = logging.getLogger("usat")


def get_credentials() -> tuple[str, str]:
    """Load Dynatrace environment id and API token from the environment."""
    env_id = os.getenv("DT_ENV_ID")
    api_token = os.getenv("DT_API_TOKEN")

    if not env_id or not api_token:
        raise RuntimeError(
            "Missing DT_ENV_ID or DT_API_TOKEN. Define them in the .env file."
        )
    return env_id, api_token
