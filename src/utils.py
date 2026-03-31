from __future__ import annotations

import re
from typing import Optional, Tuple

from src.config import TARGET_COMPETITIONS


def normalise_text(text: str | None) -> str:
    if text is None:
        return ""
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def classify_competition(raw_series_name: str | None) -> Optional[Tuple[str, str]]:
    """
    Return (competition, season_label) if the series matches one of the target
    competitions, otherwise None.
    """
    series = normalise_text(raw_series_name)

    for competition, rule in TARGET_COMPETITIONS.items():
        has_keyword = any(keyword in series for keyword in rule["keywords"])
        has_season = any(pattern in series for pattern in rule["season_patterns"])

        if has_keyword and has_season:
            return competition, rule["season_label"]

    return None


def safe_get(obj: dict, key: str, default=None):
    return obj.get(key, default)