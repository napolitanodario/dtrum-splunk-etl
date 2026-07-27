"""Join breakdown assignments back onto normalised action rows."""

import pandas as pd

_CARRY = [
    "rowId", "sessionId", "userId", "actionName", "actionKey", "actionType",
    "actionStartTime", "actionEndTime", "actionStartDt", "actionEndDt",
    "durationSec", "serverTime", "networkTime", "frontendTime",
    "requestErrorCount", "javascriptErrorCount", "targetUrl",
    "browserType", "country", "city", "bounce",
]


def matched_actions_frame(
        session: pd.DataFrame,
        assignments: pd.DataFrame,
) -> pd.DataFrame:
    """One row per (action, step) assignment with flusso/step tags."""
    if assignments is None or len(assignments) == 0:
        return pd.DataFrame(columns=["flusso_id", "step_index", "label"] + _CARRY)
    carry = [c for c in _CARRY if c in session.columns]
    out = assignments.merge(session[carry], left_on="row_id", right_on="rowId", how="left")
    return out.sort_values(["step_index", "actionStartTime"], kind="stable").reset_index(drop=True)
