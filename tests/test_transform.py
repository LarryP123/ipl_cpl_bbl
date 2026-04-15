from __future__ import annotations

from src.transform import get_competition_metadata, parse_scorecard


def test_get_competition_metadata_matches_expected_competition(test_settings):
    metadata = get_competition_metadata("Indian Premier League 2025 (IPL)", settings=test_settings)
    assert metadata == {"competition": "IPL", "season_label": "2025"}


def test_parse_scorecard_builds_structured_rows():
    scorecard = {
        "scorecard": [
            {
                "inning": "Mumbai Indians Inning 1",
                "r": "180",
                "w": "5",
                "o": "20",
                "target": None,
                "runRate": "9.0",
                "batting": [
                    {
                        "batsman": {"name": "Player A"},
                        "r": "75",
                        "b": "45",
                        "4s": "8",
                        "6s": "3",
                        "sr": "166.67",
                        "dismissal-text": "caught",
                    }
                ],
                "bowling": [
                    {
                        "bowler": {"name": "Bowler A"},
                        "o": "4",
                        "m": "0",
                        "r": "32",
                        "w": "2",
                        "eco": "8.0",
                    }
                ],
            }
        ]
    }

    innings_rows, batting_rows, bowling_rows = parse_scorecard(
        match_id="match-1",
        scorecard_data=scorecard,
        team_1="Mumbai Indians",
        team_2="Chennai Super Kings",
    )

    assert innings_rows[0]["batting_team"] == "Mumbai Indians"
    assert batting_rows[0]["player_name"] == "Player A"
    assert batting_rows[0]["strike_rate"] == 166.67
    assert bowling_rows[0]["team"] == "Chennai Super Kings"
