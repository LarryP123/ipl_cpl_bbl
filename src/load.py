from __future__ import annotations

from sqlalchemy import create_engine, text

from src.config import DB_PATH

engine = create_engine(f"sqlite:///{DB_PATH}")


def create_tables() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS matches (
                    match_id TEXT PRIMARY KEY,
                    raw_series_name TEXT,
                    competition TEXT,
                    season_label TEXT,
                    match_date TEXT,
                    match_type TEXT,
                    team_1 TEXT,
                    team_2 TEXT,
                    venue TEXT,
                    result_text TEXT,
                    status TEXT
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS innings (
                    innings_id TEXT PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    innings_number INTEGER,
                    batting_team TEXT,
                    bowling_team TEXT,
                    runs INTEGER,
                    wickets INTEGER,
                    overs REAL,
                    target INTEGER,
                    run_rate REAL,
                    FOREIGN KEY(match_id) REFERENCES matches(match_id)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS batting_scorecard (
                    batting_id TEXT PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    innings_number INTEGER,
                    player_name TEXT,
                    team TEXT,
                    runs INTEGER,
                    balls INTEGER,
                    fours INTEGER,
                    sixes INTEGER,
                    strike_rate REAL,
                    dismissal TEXT,
                    FOREIGN KEY(match_id) REFERENCES matches(match_id)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS bowling_scorecard (
                    bowling_id TEXT PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    innings_number INTEGER,
                    player_name TEXT,
                    team TEXT,
                    overs REAL,
                    maidens INTEGER,
                    runs_conceded INTEGER,
                    wickets INTEGER,
                    economy REAL,
                    FOREIGN KEY(match_id) REFERENCES matches(match_id)
                )
                """
            )
        )


def load_matches(rows: list[dict]) -> None:
    if not rows:
        return

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO matches (
                        match_id,
                        raw_series_name,
                        competition,
                        season_label,
                        match_date,
                        match_type,
                        team_1,
                        team_2,
                        venue,
                        result_text,
                        status
                    )
                    VALUES (
                        :match_id,
                        :raw_series_name,
                        :competition,
                        :season_label,
                        :match_date,
                        :match_type,
                        :team_1,
                        :team_2,
                        :venue,
                        :result_text,
                        :status
                    )
                    ON CONFLICT(match_id) DO UPDATE SET
                        raw_series_name = excluded.raw_series_name,
                        competition = excluded.competition,
                        season_label = excluded.season_label,
                        match_date = excluded.match_date,
                        match_type = excluded.match_type,
                        team_1 = excluded.team_1,
                        team_2 = excluded.team_2,
                        venue = excluded.venue,
                        result_text = excluded.result_text,
                        status = excluded.status
                    """
                ),
                row,
            )


def load_innings(rows: list[dict]) -> None:
    if not rows:
        return

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO innings (
                        innings_id,
                        match_id,
                        innings_number,
                        batting_team,
                        bowling_team,
                        runs,
                        wickets,
                        overs,
                        target,
                        run_rate
                    )
                    VALUES (
                        :innings_id,
                        :match_id,
                        :innings_number,
                        :batting_team,
                        :bowling_team,
                        :runs,
                        :wickets,
                        :overs,
                        :target,
                        :run_rate
                    )
                    ON CONFLICT(innings_id) DO UPDATE SET
                        batting_team = excluded.batting_team,
                        bowling_team = excluded.bowling_team,
                        runs = excluded.runs,
                        wickets = excluded.wickets,
                        overs = excluded.overs,
                        target = excluded.target,
                        run_rate = excluded.run_rate
                    """
                ),
                row,
            )


def load_batting_scorecard(rows: list[dict]) -> None:
    if not rows:
        return

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO batting_scorecard (
                        batting_id,
                        match_id,
                        innings_number,
                        player_name,
                        team,
                        runs,
                        balls,
                        fours,
                        sixes,
                        strike_rate,
                        dismissal
                    )
                    VALUES (
                        :batting_id,
                        :match_id,
                        :innings_number,
                        :player_name,
                        :team,
                        :runs,
                        :balls,
                        :fours,
                        :sixes,
                        :strike_rate,
                        :dismissal
                    )
                    ON CONFLICT(batting_id) DO UPDATE SET
                        player_name = excluded.player_name,
                        team = excluded.team,
                        runs = excluded.runs,
                        balls = excluded.balls,
                        fours = excluded.fours,
                        sixes = excluded.sixes,
                        strike_rate = excluded.strike_rate,
                        dismissal = excluded.dismissal
                    """
                ),
                row,
            )


def load_bowling_scorecard(rows: list[dict]) -> None:
    if not rows:
        return

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO bowling_scorecard (
                        bowling_id,
                        match_id,
                        innings_number,
                        player_name,
                        team,
                        overs,
                        maidens,
                        runs_conceded,
                        wickets,
                        economy
                    )
                    VALUES (
                        :bowling_id,
                        :match_id,
                        :innings_number,
                        :player_name,
                        :team,
                        :overs,
                        :maidens,
                        :runs_conceded,
                        :wickets,
                        :economy
                    )
                    ON CONFLICT(bowling_id) DO UPDATE SET
                        player_name = excluded.player_name,
                        team = excluded.team,
                        overs = excluded.overs,
                        maidens = excluded.maidens,
                        runs_conceded = excluded.runs_conceded,
                        wickets = excluded.wickets,
                        economy = excluded.economy
                    """
                ),
                row,
            )