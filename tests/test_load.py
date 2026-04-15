from __future__ import annotations

import sqlite3

from src.load import create_tables, get_engine, load_matches, replace_scorecards


def test_replace_scorecards_is_rerunnable_without_duplicates(test_settings):
    engine = get_engine(test_settings)
    create_tables(engine=engine)

    load_matches(
        [
            {
                "match_id": "match-1",
                "raw_series_name": "Indian Premier League 2025 (IPL)",
                "competition": "IPL",
                "season_label": "2025",
                "match_date": "2025-04-01",
                "match_type": "t20",
                "team_1": "Mumbai Indians",
                "team_2": "Chennai Super Kings",
                "venue": "Mumbai",
                "result_text": "Mumbai won",
                "status": "completed",
            }
        ],
        engine=engine,
    )

    innings_rows = [
        {
            "innings_id": "match-1_1",
            "match_id": "match-1",
            "innings_number": 1,
            "batting_team": "Mumbai Indians",
            "bowling_team": "Chennai Super Kings",
            "runs": 180,
            "wickets": 5,
            "overs": 20.0,
            "target": None,
            "run_rate": 9.0,
        }
    ]
    batting_rows = [
        {
            "batting_id": "match-1_1_bat_1",
            "match_id": "match-1",
            "innings_number": 1,
            "player_name": "Player A",
            "team": "Mumbai Indians",
            "runs": 75,
            "balls": 45,
            "fours": 8,
            "sixes": 3,
            "strike_rate": 166.67,
            "dismissal": "caught",
        }
    ]
    bowling_rows = [
        {
            "bowling_id": "match-1_1_bowl_1",
            "match_id": "match-1",
            "innings_number": 1,
            "player_name": "Bowler A",
            "team": "Chennai Super Kings",
            "overs": 4.0,
            "maidens": 0,
            "runs_conceded": 32,
            "wickets": 2,
            "economy": 8.0,
        }
    ]

    replace_scorecards(innings_rows, batting_rows, bowling_rows, engine=engine)
    replace_scorecards(
        innings_rows,
        batting_rows
        + [
            {
                "batting_id": "match-1_1_bat_2",
                "match_id": "match-1",
                "innings_number": 1,
                "player_name": "Player B",
                "team": "Mumbai Indians",
                "runs": 30,
                "balls": 18,
                "fours": 2,
                "sixes": 2,
                "strike_rate": 166.67,
                "dismissal": "not out",
            }
        ],
        bowling_rows,
        engine=engine,
    )

    connection = sqlite3.connect(test_settings.db_path)
    batting_count = connection.execute("SELECT COUNT(*) FROM batting_scorecard").fetchone()[0]
    innings_count = connection.execute("SELECT COUNT(*) FROM innings").fetchone()[0]
    connection.close()

    assert innings_count == 1
    assert batting_count == 2
