"""Per-flusso feature rows from matched funnel actions."""

import pandas as pd

from funnel.categories import category_flags

# Reaching this step index marks a completed emission (override locally if needed).
COMPLETION_STEP = 6


def build_flow_features(matched: pd.DataFrame) -> pd.DataFrame:
    """One row per flusso: timings, depth, errors, insured-good category flags."""
    if matched is None or not len(matched):
        return pd.DataFrame()

    rows = []
    for fid, g in matched.groupby("flusso_id"):
        g = g.sort_values("actionStartTime") if "actionStartTime" in g.columns else g
        keys = [k for k in g["actionKey"].tolist() if k]
        elapsed = (g["actionEndDt"].max() - g["actionStartDt"].min()).total_seconds()
        active = float(pd.to_numeric(g["durationSec"], errors="coerce").sum())
        max_step = int(g["step_index"].max())
        session_ids = sorted({str(s) for s in g["sessionId"].dropna().unique()})

        rec = {
            "flusso_id": fid,
            "userId": g["userId"].iloc[0] if "userId" in g.columns else None,
            "sessionIds": session_ids,
            "sessionId": session_ids[0] if session_ids else None,
            "n_actions": len(g),
            "n_unique_actions": g["actionKey"].nunique(),
            "duration_s": round(elapsed, 2),
            "active_s": round(active, 2),
            "dead_time_s": round(elapsed - active, 2),
            "max_step": max_step,
            "n_steps": int(g["step_index"].nunique()),
            "completed": bool(max_step >= COMPLETION_STEP),
            "abandoned": bool(max_step < COMPLETION_STEP),
            "request_errors": _int_sum(g, "requestErrorCount"),
            "js_errors": _int_sum(g, "javascriptErrorCount"),
            "tokens": keys,
        }
        rec["total_errors"] = rec["request_errors"] + rec["js_errors"]
        rec["has_error"] = rec["total_errors"] > 0
        rec.update(category_flags(keys))
        rows.append(rec)

    return pd.DataFrame(rows).reset_index(drop=True)


def _int_sum(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
