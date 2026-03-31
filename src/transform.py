from __future__ import annotations

import re

from src.config import TARGET_COMPETITIONS


def get_competition_metadata(series_name: str) -> dict:
    name_lower = series_name.lower()

    for competition, config in TARGET_COMPETITIONS.items():
        keyword_match = any(
            keyword.lower() in name_lower
            for keyword in config["keywords"]
        )
        season_match = any(
            pattern.lower() in name_lower
            for pattern in config["season_patterns"]
        )

        if keyword_match and season_match:
            return {
                "competition": competition,
                "season_label": config["season_label"],
            }

    return {
        "competition": None,
        "season_label": None,
    }


def to_int(value):
    if value in (None, "", "null", "-"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def to_float(value):
    if value in (None, "", "null", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_name(value):
    if value is None:
        return None

    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        return value.get("name") or value.get("teamName") or value.get("shortname")

    return str(value)


def extract_batting_team_from_inning_label(inning_label: str | None) -> str | None:
    """
    Examples:
    - 'Chennai Super Kings Inning 1' -> 'Chennai Super Kings'
    - 'Adelaide Strikers Inning 1' -> 'Adelaide Strikers'
    """
    if not inning_label:
        return None

    cleaned = re.sub(r"\s+Inning\s+\d+$", "", inning_label).strip()
    return cleaned or None


def infer_bowling_team(batting_team: str | None, team_1: str | None, team_2: str | None) -> str | None:
    if batting_team and team_1 and batting_team == team_1:
        return team_2
    if batting_team and team_2 and batting_team == team_2:
        return team_1
    return None


def parse_scorecard(
    match_id: str,
    scorecard_data: dict,
    team_1: str | None,
    team_2: str | None,
) -> tuple[list[dict], list[dict], list[dict]]:
    innings_rows: list[dict] = []
    batting_rows: list[dict] = []
    bowling_rows: list[dict] = []

    scorecard = scorecard_data.get("scorecard", [])

    for i, innings in enumerate(scorecard, start=1):
        inning_label = innings.get("inning")
        batting_team = extract_batting_team_from_inning_label(inning_label)
        bowling_team = infer_bowling_team(batting_team, team_1, team_2)

        innings_rows.append(
            {
                "innings_id": f"{match_id}_{i}",
                "match_id": match_id,
                "innings_number": i,
                "batting_team": batting_team,
                "bowling_team": bowling_team,
                "runs": to_int(innings.get("r")),
                "wickets": to_int(innings.get("w")),
                "overs": to_float(innings.get("o")),
                "target": to_int(innings.get("target")),
                "run_rate": to_float(innings.get("runRate")),
            }
        )

        for j, batter in enumerate(innings.get("batting", []), start=1):
            batter_name = extract_name(batter.get("batsman")) or extract_name(batter.get("name"))

            batting_rows.append(
                {
                    "batting_id": f"{match_id}_{i}_bat_{j}",
                    "match_id": match_id,
                    "innings_number": i,
                    "player_name": batter_name,
                    "team": batting_team,
                    "runs": to_int(batter.get("r")),
                    "balls": to_int(batter.get("b")),
                    "fours": to_int(batter.get("4s")),
                    "sixes": to_int(batter.get("6s")),
                    "strike_rate": to_float(batter.get("sr")),
                    "dismissal": batter.get("dismissal-text") or batter.get("dismissal"),
                }
            )

        for j, bowler in enumerate(innings.get("bowling", []), start=1):
            bowler_name = extract_name(bowler.get("bowler")) or extract_name(bowler.get("name"))

            bowling_rows.append(
                {
                    "bowling_id": f"{match_id}_{i}_bowl_{j}",
                    "match_id": match_id,
                    "innings_number": i,
                    "player_name": bowler_name,
                    "team": bowling_team,
                    "overs": to_float(bowler.get("o")),
                    "maidens": to_int(bowler.get("m")),
                    "runs_conceded": to_int(bowler.get("r")),
                    "wickets": to_int(bowler.get("w")),
                    "economy": to_float(bowler.get("eco")),
                }
            )

    return innings_rows, batting_rows, bowling_rows