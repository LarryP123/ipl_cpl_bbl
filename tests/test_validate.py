from __future__ import annotations

import pandas as pd

from src.validate import validate_batting_scorecard


def test_validate_batting_scorecard_returns_warning_for_suspicious_rows():
    dataframe = pd.DataFrame(
        [
            {
                "batting_id": "bat-1",
                "match_id": "match-1",
                "innings_number": 1,
                "player_name": "Player A",
                "team": "Team A",
                "runs": 20,
                "balls": 0,
                "fours": 1,
                "sixes": 3,
                "strike_rate": 250.0,
            }
        ]
    )

    result = validate_batting_scorecard(dataframe)

    assert result["status"] == "warning"
    assert result["warning_counts"]["runs_scored_from_zero_balls"] == 1
    assert result["warning_samples"]["runs_scored_from_zero_balls"][0]["player_name"] == "Player A"


def test_validate_batting_scorecard_returns_failed_for_duplicate_rows():
    dataframe = pd.DataFrame(
        [
            {
                "batting_id": "bat-1",
                "match_id": "match-1",
                "innings_number": 1,
                "player_name": "Player A",
                "team": "Team A",
                "runs": 20,
                "balls": 10,
                "fours": 1,
                "sixes": 1,
                "strike_rate": 200.0,
            },
            {
                "batting_id": "bat-1",
                "match_id": "match-1",
                "innings_number": 1,
                "player_name": "Player A",
                "team": "Team A",
                "runs": 20,
                "balls": 10,
                "fours": 1,
                "sixes": 1,
                "strike_rate": 200.0,
            },
        ]
    )

    result = validate_batting_scorecard(dataframe)

    assert result["status"] == "failed"
    assert "Duplicate batting_id values found: 1" in result["hard_failures"]
