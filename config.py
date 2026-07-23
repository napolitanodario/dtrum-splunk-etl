"""Runtime configuration, credentials, and query parameters."""

import logging
import os

from dotenv import load_dotenv

# Trailing-only action-name prefixes for discovery (no leading wildcard).
# Used as LIKE '{prefix}%' in discovery_query.
DISCOVERY_NAME_PREFIXES = [
    "/newagenm.web/flussop1",
    "/NewAge.Web/FlussoP1",
    "/NewAgeNM.Web/FlussoP1",
    "loading of page /newagenm.web/flussop1",
    "loading of page //newagenm.web/flussop1",
    "Loading of page /NewAgeNM.Web/FlussoP1",
    "Loading of page /NewAge.Web/FlussoP1",
    "spostagaranzia on page /newagenm.web/flussop1",
    "calcolopremio on page /newagenm.web/flussop1",
    "aggiungibene on page /newagenm.web/flussop1",
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

# Must match TOP(..., n) / LIMIT n in discovery_query.
DISCOVERY_TOP_N = 1000

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

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
