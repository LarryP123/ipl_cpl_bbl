from __future__ import annotations

import logging
import time

import requests

from src.config import Settings, get_settings
from src.utils import normalise_text


logger = logging.getLogger(__name__)


class CricApiClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session = requests.Session()

    def _request(self, url: str, params: dict, resource_name: str) -> dict:
        if not self.settings.api_key:
            raise ValueError(
                "Missing API key. Set CRICKETDATA_API_KEY in your environment."
            )

        last_error: Exception | None = None

        for attempt in range(1, self.settings.request_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params={"apikey": self.settings.api_key, **params},
                    timeout=self.settings.request_timeout,
                )
                response.raise_for_status()
                payload = response.json()

                if payload.get("status") != "success":
                    raise ValueError(f"Unexpected API response for {resource_name}: {payload}")

                data = payload.get("data")
                if data is None:
                    raise ValueError(f"API returned no data for {resource_name}.")

                return data
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt == self.settings.request_retries:
                    break

                sleep_seconds = self.settings.request_backoff_seconds * attempt
                logger.warning(
                    "Retrying %s after attempt %s/%s failed: %s",
                    resource_name,
                    attempt,
                    self.settings.request_retries,
                    exc,
                )
                time.sleep(sleep_seconds)

        raise RuntimeError(f"Failed to fetch {resource_name}") from last_error

    def get_series(self, search_term: str, offset: int = 0) -> list[dict]:
        data = self._request(
            self.settings.series_endpoint,
            {"offset": offset, "search": search_term},
            f"series search '{search_term}'",
        )
        if not isinstance(data, list):
            raise ValueError(f"Series search returned unexpected payload: {data}")
        return data

    def get_series_info(self, series_id: str) -> dict:
        data = self._request(
            self.settings.series_info_endpoint,
            {"offset": 0, "id": series_id},
            f"series info '{series_id}'",
        )
        if not isinstance(data, dict):
            raise ValueError(f"Series info returned unexpected payload: {data}")
        return data

    def get_matches_from_series_info(self, series_id: str) -> list[dict]:
        data = self.get_series_info(series_id)
        match_list = data.get("matchList", [])
        if not isinstance(match_list, list):
            raise ValueError(f"Match list returned unexpected payload: {match_list}")
        return match_list

    def get_match_scorecard(self, match_id: str) -> dict:
        data = self._request(
            self.settings.match_scorecard_endpoint,
            {"offset": 0, "id": match_id},
            f"match scorecard '{match_id}'",
        )
        if not isinstance(data, dict):
            raise ValueError(f"Match scorecard returned unexpected payload: {data}")
        return data


def build_client(settings: Settings | None = None) -> CricApiClient:
    return CricApiClient(settings or get_settings())


def filter_target_series(
    series_list: list[dict],
    settings: Settings | None = None,
) -> list[dict]:
    active_settings = settings or get_settings()
    filtered: list[dict] = []
    seen_ids: set[str] = set()

    for series in series_list:
        series_id = series.get("id")
        series_name = normalise_text(series.get("name"))

        if not series_id or "women" in series_name or "womens" in series_name:
            continue

        for config in active_settings.target_competitions.values():
            has_keyword = any(keyword in series_name for keyword in config.keywords)
            has_season = any(pattern in series_name for pattern in config.season_patterns)

            if has_keyword and has_season and series_id not in seen_ids:
                filtered.append(series)
                seen_ids.add(series_id)
                break

    return filtered
