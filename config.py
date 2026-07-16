import logging
from dotenv import load_dotenv
from typing import *
import os

DISCOVERY_NAME_LIKE = ["%FlussoP1%", "%flussop1%"]


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

log = logging.getLogger("usat")


def get_credentials() -> tuple[str, str]:
    env_id = os.getenv("DT_ENV_ID")
    api_token = os.getenv("DT_API_TOKEN")

    if not env_id or not api_token:
        raise RuntimeError(
            "Missing DT_ENV_ID or DT_API_TOKEN. Define them in the .env file."
        )
    return env_id, api_token
