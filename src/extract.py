from __future__ import annotations

import requests
from src.config import API_KEY, SERIES_ENDPOINT, SERIES_INFO_ENDPOINT, MATCH_SCORECARD_ENDPOINT, TARGET_COMPETITIONS


def get_series(search_term: str, offset: int = 0) -> list[dict]:
    if not API_KEY:
        raise ValueError("Missing API key. Set CRICKETDATA_API_KEY in your environment.")

    params = {
        "apikey": API_KEY,
        "offset": offset,
        "search": search_term,
    }

    response = requests.get(SERIES_ENDPOINT, params=params, timeout=(5, 20))
    response.raise_for_status()

    payload = response.json()

    if payload.get("status") != "success":
        raise ValueError(f"API returned unexpected response: {payload}")

    return payload.get("data", [])


def filter_target_series(series_list: list[dict]) -> list[dict]:
    filtered = []
    seen_ids = set()

    for series in series_list:
        series_id = series.get("id")
        series_name = series.get("name", "")
        name_lower = series_name.lower()

        if "women" in name_lower or "womens" in name_lower:
            continue

        for comp_config in TARGET_COMPETITIONS.values():
            keyword_match = any(
                keyword.lower() in name_lower
                for keyword in comp_config["keywords"]
            )

            season_match = any(
                pattern.lower() in name_lower
                for pattern in comp_config["season_patterns"]
            )

            if keyword_match and season_match:
                if series_id not in seen_ids:
                    filtered.append(series)
                    seen_ids.add(series_id)
                break

    return filtered

def get_series_info(series_id: str) -> dict:
    if not API_KEY:
        raise ValueError("Missing API key. Set CRICKETDATA_API_KEY in your environment.")

    params = {
        "apikey": API_KEY,
        "offset": 0,
        "id": series_id,
    }

    response = requests.get(SERIES_INFO_ENDPOINT, params=params, timeout=(5, 20))
    response.raise_for_status()

    payload = response.json()

    if payload.get("status") != "success":
        raise ValueError(f"API returned unexpected response: {payload}")

    return payload.get("data", {})


def get_matches_from_series_info(series_id: str) -> list[dict]:
    data = get_series_info(series_id)
    return data.get("matchList", [])


def get_match_scorecard(match_id: str) -> dict:
    if not API_KEY:
        raise ValueError("Missing API key. Set CRICKETDATA_API_KEY in your environment.")

    params = {
        "apikey": API_KEY,
        "offset": 0,
        "id": match_id,
    }

    response = requests.get(MATCH_SCORECARD_ENDPOINT, params=params, timeout=(5, 20))
    response.raise_for_status()

    payload = response.json()

    if payload.get("status") != "success":
        raise ValueError(f"API returned unexpected response: {payload}")

    return payload.get("data", {})
    data = get_series_info(series_id)
    return data.get("matchList", [])