"""Action-name normalisation for dedup and cross-session action keys."""


def normalize_action_name(name: str) -> str:
    s = (name or "").strip().lower()
    if s.startswith("loading of page "):
        s = s[len("loading of page "):]
    elif s.startswith("loading of "):
        s = s[len("loading of "):]
    q = s.find("?")
    if q >= 0:
        s = s[:q]
    return s.replace("//", "/").strip().rstrip("/")
