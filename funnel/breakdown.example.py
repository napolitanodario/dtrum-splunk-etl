"""Example funnel breakdown module (safe to commit).

Copy to breakdown.py and replace with your private step-reconstruction logic:

    cp funnel/breakdown.example.py funnel/breakdown.py

``breakdown.py`` is gitignored. The public contract expected by
``funnel.reconstruct`` is ``build_breakdown(session) -> (step_breakdown, assignments)``.
"""

from __future__ import annotations

import pandas as pd


def build_breakdown(session: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return per-step records and row-level step assignments for a normalised session.

    Expected columns on ``session`` include at least: userId, sessionId, actionName
    (or equivalent), actionStartTime, targetUrl / rowId as used by your matcher.

    ``step_breakdown`` rows typically include: user, session_id, flusso_id,
    flusso_number, block_start, step_index, pagina, label, matched_actions.

    ``assignments`` rows typically include: row_id, flusso_id, step_index, label.
    """
    raise NotImplementedError(
        "funnel/breakdown.py is not installed. "
        "Copy funnel/breakdown.example.py to funnel/breakdown.py and provide "
        "your private reconstruction implementation."
    )
