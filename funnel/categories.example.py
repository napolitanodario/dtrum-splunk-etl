"""Example action-name category keywords (safe to commit).

Copy to categories.py and adapt keywords to your domain:

    cp funnel/categories.example.py funnel/categories.py

``categories.py`` is gitignored.
"""

from typing import Iterable

ACTION_CATEGORIES: dict[str, list[str]] = {
    "product_a": ["product-a", "sku-a"],
    "product_b": ["product-b", "sku-b"],
    "address": ["address", "zipcode"],
}

CATEGORY_NAMES: list[str] = list(ACTION_CATEGORIES)


def category_flags(action_keys: Iterable[str]) -> dict[str, int]:
    blob = " || ".join(k for k in action_keys if k)
    return {cat: int(any(kw in blob for kw in kws)) for cat, kws in ACTION_CATEGORIES.items()}
