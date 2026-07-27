"""Example funnel step definitions (safe to commit).

Copy to definitions.py and replace with your application-specific steps:

    cp funnel/definitions.example.py funnel/definitions.py

``definitions.py`` is gitignored.
"""

from __future__ import annotations

# TargetUrl fragment used to disambiguate late-funnel steps that share a path with step 1.
STAMPA_TARGET_URL = ""
STAMPA_FRAGMENT = "checkout-confirm"

# Per-step match mode: True = exact action name, False = substring.
STEP_MATCH_EXACT = {1: True, 2: False, 3: True}

FUNNEL_ENTRIES: list[dict] = [
    {
        "step_index": 1,
        "pagina": "01 - Start",
        "label": "01 - Entry",
        "patterns": ["Loading of page /myapp/checkout"],
        "target_url": None,
    },
    {
        "step_index": 2,
        "pagina": "01 - Start",
        "label": "02 - Details",
        "patterns": ["/myapp/checkout/details"],
        "target_url": None,
    },
    {
        "step_index": 3,
        "pagina": "02 - Confirm",
        "label": "03 - Confirm",
        "patterns": ["/myapp/checkout/confirm"],
        "target_url": STAMPA_FRAGMENT,
    },
]


def _can_repeat(step_index: int, has_target: bool) -> bool:
    if step_index >= 3:
        return False
    return True


def _build_step_variants() -> list[dict]:
    variants = []
    for e in FUNNEL_ENTRIES:
        if not e.get("patterns"):
            continue
        idx = e["step_index"]
        tgt = (e.get("target_url") or "").lower()
        variants.append({
            "index": idx,
            "label": e["label"],
            "pagina": e["pagina"],
            "patterns": [p.strip().lower() for p in e["patterns"]],
            "target_url": tgt,
            "match_exact": STEP_MATCH_EXACT.get(idx, True),
            "can_repeat": _can_repeat(idx, bool(tgt)),
        })
    return variants


STEP_VARIANTS = _build_step_variants()
NON_REPEAT_VARIANTS = [v for v in STEP_VARIANTS if not v["can_repeat"]]
PAGES_ORDER = sorted({v["pagina"] for v in STEP_VARIANTS})

STEP_INFO: dict[int, tuple[str, str]] = {}
for _v in STEP_VARIANTS:
    STEP_INFO.setdefault(_v["index"], (_v["pagina"], _v["label"]))

STEP_DEFS = [
    {
        "index": v["index"],
        "label": v["label"],
        "pagina": v["pagina"],
        "patterns": v["patterns"],
        "target_url": v["target_url"],
        "match_exact": v["match_exact"],
    }
    for v in STEP_VARIANTS
]

STEP_LABELS = sorted({v["label"] for v in STEP_VARIANTS})
