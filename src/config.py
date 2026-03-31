import os

API_KEY = os.getenv("CRICKETDATA_API_KEY")
BASE_URL = "https://api.cricapi.com/v1"
SERIES_INFO_ENDPOINT = f"{BASE_URL}/series_info"
MATCHES_BY_SERIES_ENDPOINT = f"{BASE_URL}/matches"
MATCH_SCORECARD_ENDPOINT = f"{BASE_URL}/match_scorecard"
MATCHES_ENDPOINT = f"{BASE_URL}/currentMatches"
SERIES_ENDPOINT = f"{BASE_URL}/series"

TARGET_COMPETITIONS = {
    "IPL": {
        "keywords": ["indian premier league"],
        "season_patterns": ["2025"],
        "season_label": "2025",
    },
    "CPL": {
        "keywords": ["caribbean premier league"],
        "season_patterns": ["2025"],
        "season_label": "2025",
    },
    "BBL": {
        "keywords": ["big bash league"],
        "season_patterns": ["2024-25"],
        "season_label": "2024-25",
    },
}

DB_PATH = "data/cricket.db"
MATCHES_ENDPOINT = f"{BASE_URL}/matches"