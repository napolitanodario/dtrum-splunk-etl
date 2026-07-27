"""Turn raw Dynatrace action rows into a normalised frame for funnel breakdown."""

import pandas as pd

from funnel.normalize import normalize_action_name

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

  Every row must carry a Dynatrace user tag (userId). Sessions without a tag
  are excluded upstream by discovery; this layer fails fast if userId is absent.
    """
    raw = raw.reset_index(drop=True)
    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError(f"Input is missing required columns: {missing}")

    tagged = raw["userId"].notna() & (raw["userId"].astype(str).str.strip() != "")
    if not tagged.all():
        n_bad = int((~tagged).sum())
        raise ValueError(
            f"{n_bad} action row(s) lack userId; only tagged users are supported."
        )

    df = raw.rename(columns=_RENAME)

    for col in _NUMERIC:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["actionName", "actionType", "targetUrl", "userId"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    df["rowId"] = range(len(raw))
    df["actionStartDt"] = pd.to_datetime(df["actionStartTime"], unit="ms", utc=True)
    df["actionEndDt"] = pd.to_datetime(df["actionEndTime"], unit="ms", utc=True)
    df["durationSec"] = df["actionDuration"] / 1000.0
    df["actionKey"] = df["actionName"].map(normalize_action_name)

    return df.sort_values("actionStartTime").reset_index(drop=True)
