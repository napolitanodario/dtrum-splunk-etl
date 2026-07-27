"""Turn raw Dynatrace action rows into a normalised frame for funnel breakdown."""

import logging

import pandas as pd

from funnel.normalize import normalize_action_name

log = logging.getLogger("usat")

REQUIRED_COLUMNS = [
    "name", "type", "duration", "startTime", "endTime",
    "networkTime", "frontendTime", "serverTime", "targetUrl",
    "userId",
]

_RENAME = {
    "name": "actionName",
    "type": "actionType",
    "duration": "actionDuration",
    "startTime": "actionStartTime",
    "endTime": "actionEndTime",
}

_NUMERIC = [
    "actionDuration", "actionStartTime", "actionEndTime",
    "serverTime", "networkTime", "frontendTime",
]


def normalize_actions(raw: pd.DataFrame) -> pd.DataFrame:
    """Rename columns, coerce types, add rowId/actionKey, sort chronologically.

    Rows without a Dynatrace user tag (userId) are dropped with a warning.
    Discovery already filters tagged sessions; occasional untagged action rows
    can still appear in session-action fetches.
    """
    raw = raw.reset_index(drop=True)
    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError(f"Input is missing required columns: {missing}")

    tagged = raw["userId"].notna() & (raw["userId"].astype(str).str.strip() != "")
    n_bad = int((~tagged).sum())
    if n_bad:
        log.warning("Dropped %d action row(s) without userId", n_bad)
        raw = raw.loc[tagged].reset_index(drop=True)
    if raw.empty:
        raise ValueError("No tagged action rows remain after dropping rows without userId.")

    df = raw.rename(columns=_RENAME)

    for col in _NUMERIC:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["actionName", "actionType", "targetUrl", "userId"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    df["rowId"] = range(len(df))
    df["actionStartDt"] = pd.to_datetime(df["actionStartTime"], unit="ms", utc=True)
    df["actionEndDt"] = pd.to_datetime(df["actionEndTime"], unit="ms", utc=True)
    df["durationSec"] = df["actionDuration"] / 1000.0
    df["actionKey"] = df["actionName"].map(normalize_action_name)

    return df.sort_values("actionStartTime").reset_index(drop=True)
