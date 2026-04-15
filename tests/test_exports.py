from __future__ import annotations

import sqlite3

from export_results import run_exports
from src.load import create_tables, get_engine, load_matches, replace_scorecards


def test_export_queries_generate_non_empty_csvs(test_settings):
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
            },
            {
                "match_id": "match-2",
                "raw_series_name": "Indian Premier League 2025 (IPL)",
                "competition": "IPL",
                "season_label": "2025",
                "match_date": "2025-04-05",
                "match_type": "t20",
                "team_1": "Mumbai Indians",
                "team_2": "Royal Challengers Bengaluru",
                "venue": "Bengaluru",
                "result_text": "Mumbai won",
                "status": "completed",
            },
            {
                "match_id": "match-3",
                "raw_series_name": "Indian Premier League 2025 (IPL)",
                "competition": "IPL",
                "season_label": "2025",
                "match_date": "2025-04-08",
                "match_type": "t20",
                "team_1": "Mumbai Indians",
                "team_2": "Delhi Capitals",
                "venue": "Delhi",
                "result_text": "Delhi won",
                "status": "completed",
            },
            {
                "match_id": "match-4",
                "raw_series_name": "Indian Premier League 2025 (IPL)",
                "competition": "IPL",
                "season_label": "2025",
                "match_date": "2025-04-11",
                "match_type": "t20",
                "team_1": "Mumbai Indians",
                "team_2": "Punjab Kings",
                "venue": "Mohali",
                "result_text": "Mumbai won",
                "status": "completed",
            },
            {
                "match_id": "match-5",
                "raw_series_name": "Indian Premier League 2025 (IPL)",
                "competition": "IPL",
                "season_label": "2025",
                "match_date": "2025-04-14",
                "match_type": "t20",
                "team_1": "Mumbai Indians",
                "team_2": "Kolkata Knight Riders",
                "venue": "Mumbai",
                "result_text": "Mumbai won",
                "status": "completed",
            },
            {
                "match_id": "match-6",
                "raw_series_name": "Indian Premier League 2025 (IPL)",
                "competition": "IPL",
                "season_label": "2025",
                "match_date": "2025-04-18",
                "match_type": "t20",
                "team_1": "Mumbai Indians",
                "team_2": "Lucknow Super Giants",
                "venue": "Lucknow",
                "result_text": "Mumbai won",
                "status": "completed",
            },
            {
                "match_id": "match-7",
                "raw_series_name": "Indian Premier League 2025 (IPL)",
                "competition": "IPL",
                "season_label": "2025",
                "match_date": "2025-04-22",
                "match_type": "t20",
                "team_1": "Mumbai Indians",
                "team_2": "Sunrisers Hyderabad",
                "venue": "Hyderabad",
                "result_text": "Mumbai won",
                "status": "completed",
            },
            {
                "match_id": "match-8",
                "raw_series_name": "Indian Premier League 2025 (IPL)",
                "competition": "IPL",
                "season_label": "2025",
                "match_date": "2025-04-26",
                "match_type": "t20",
                "team_1": "Mumbai Indians",
                "team_2": "Rajasthan Royals",
                "venue": "Jaipur",
                "result_text": "Rajasthan won",
                "status": "completed",
            },
        ],
        engine=engine,
    )

    innings_rows = []
    batting_rows = []
    bowling_rows = []
    for match_number in range(1, 9):
        innings_rows.append(
            {
                "innings_id": f"match-{match_number}_1",
                "match_id": f"match-{match_number}",
                "innings_number": 1,
                "batting_team": "Mumbai Indians",
                "bowling_team": "Opponent",
                "runs": 180,
                "wickets": 5,
                "overs": 20.0,
                "target": None,
                "run_rate": 9.0,
            }
        )
        batting_rows.append(
            {
                "batting_id": f"match-{match_number}_1_bat_1",
                "match_id": f"match-{match_number}",
                "innings_number": 1,
                "player_name": "Player A",
                "team": "Mumbai Indians",
                "runs": 50 + match_number,
                "balls": 30,
                "fours": 4,
                "sixes": 3,
                "strike_rate": 170.0,
                "dismissal": "caught",
            }
        )
        bowling_rows.append(
            {
                "bowling_id": f"match-{match_number}_1_bowl_1",
                "match_id": f"match-{match_number}",
                "innings_number": 1,
                "player_name": "Bowler A",
                "team": "Opponent",
                "overs": 4.0,
                "maidens": 0,
                "runs_conceded": 35,
                "wickets": 1,
                "economy": 8.75,
            }
        )

    replace_scorecards(innings_rows, batting_rows, bowling_rows, engine=engine)

    summary = run_exports(test_settings)

    assert summary["total_exports"] == len(test_settings.export_queries)
    assert (test_settings.exports_dir / "01_best_overall_batters.csv").exists()

    connection = sqlite3.connect(test_settings.db_path)
    row = connection.execute(
        """
        SELECT player_name, innings, total_runs
        FROM analytics_player_batting_summary
        WHERE player_name = 'Player A'
        """
    ).fetchone()
    connection.close()

    assert row == ("Player A", 8, sum(50 + match_number for match_number in range(1, 9)))
