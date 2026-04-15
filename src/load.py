from __future__ import annotations

import logging

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from src.config import Settings, get_settings


logger = logging.getLogger(__name__)


MATCH_COLUMNS = (
    "match_id",
    "raw_series_name",
    "competition",
    "season_label",
    "match_date",
    "match_type",
    "team_1",
    "team_2",
    "venue",
    "result_text",
    "status",
)

INNINGS_COLUMNS = (
    "innings_id",
    "match_id",
    "innings_number",
    "batting_team",
    "bowling_team",
    "runs",
    "wickets",
    "overs",
    "target",
    "run_rate",
)

BATTING_COLUMNS = (
    "batting_id",
    "match_id",
    "innings_number",
    "player_name",
    "team",
    "runs",
    "balls",
    "fours",
    "sixes",
    "strike_rate",
    "dismissal",
)

BOWLING_COLUMNS = (
    "bowling_id",
    "match_id",
    "innings_number",
    "player_name",
    "team",
    "overs",
    "maidens",
    "runs_conceded",
    "wickets",
    "economy",
)


def get_engine(settings: Settings | None = None) -> Engine:
    active_settings = settings or get_settings()
    active_settings.ensure_directories()
    engine = create_engine(active_settings.database_url)

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.close()

    return engine


def create_tables(engine: Engine | None = None, settings: Settings | None = None) -> None:
    active_engine = engine or get_engine(settings)

    with active_engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON;"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS matches (
                    match_id TEXT PRIMARY KEY,
                    raw_series_name TEXT NOT NULL,
                    competition TEXT,
                    season_label TEXT,
                    match_date TEXT,
                    match_type TEXT,
                    team_1 TEXT,
                    team_2 TEXT,
                    venue TEXT,
                    result_text TEXT,
                    status TEXT
                );
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS innings (
                    innings_id TEXT PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    innings_number INTEGER NOT NULL,
                    batting_team TEXT,
                    bowling_team TEXT,
                    runs INTEGER,
                    wickets INTEGER,
                    overs REAL,
                    target INTEGER,
                    run_rate REAL,
                    FOREIGN KEY (match_id) REFERENCES matches(match_id)
                );
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS batting_scorecard (
                    batting_id TEXT PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    innings_number INTEGER NOT NULL,
                    player_name TEXT,
                    team TEXT,
                    runs INTEGER,
                    balls INTEGER,
                    fours INTEGER,
                    sixes INTEGER,
                    strike_rate REAL,
                    dismissal TEXT,
                    FOREIGN KEY (match_id) REFERENCES matches(match_id)
                );
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS bowling_scorecard (
                    bowling_id TEXT PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    innings_number INTEGER NOT NULL,
                    player_name TEXT,
                    team TEXT,
                    overs REAL,
                    maidens INTEGER,
                    runs_conceded INTEGER,
                    wickets INTEGER,
                    economy REAL,
                    FOREIGN KEY (match_id) REFERENCES matches(match_id)
                );
                """
            )
        )

        for ddl in (
            "CREATE INDEX IF NOT EXISTS idx_matches_competition_season ON matches (competition, season_label);",
            "CREATE INDEX IF NOT EXISTS idx_innings_match_id ON innings (match_id, innings_number);",
            "CREATE INDEX IF NOT EXISTS idx_batting_match_id ON batting_scorecard (match_id, innings_number);",
            "CREATE INDEX IF NOT EXISTS idx_batting_player_team ON batting_scorecard (player_name, team);",
            "CREATE INDEX IF NOT EXISTS idx_bowling_match_id ON bowling_scorecard (match_id, innings_number);",
            "CREATE INDEX IF NOT EXISTS idx_bowling_player_team ON bowling_scorecard (player_name, team);",
        ):
            conn.execute(text(ddl))


def _validate_columns(rows: list[dict], required_columns: tuple[str, ...], dataset_name: str) -> None:
    if not rows:
        return

    missing_columns = sorted(set(required_columns) - set(rows[0].keys()))
    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: {', '.join(missing_columns)}"
        )


def load_matches(rows: list[dict], engine: Engine | None = None, settings: Settings | None = None) -> None:
    if not rows:
        logger.info("No match rows supplied for loading.")
        return

    _validate_columns(rows, MATCH_COLUMNS, "matches")
    active_engine = engine or get_engine(settings)

    statement = text(
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
    )

    with active_engine.begin() as conn:
        conn.execute(statement, rows)


def replace_scorecards(
    innings_rows: list[dict],
    batting_rows: list[dict],
    bowling_rows: list[dict],
    engine: Engine | None = None,
    settings: Settings | None = None,
) -> None:
    if not innings_rows and not batting_rows and not bowling_rows:
        logger.info("No scorecard rows supplied for loading.")
        return

    _validate_columns(innings_rows, INNINGS_COLUMNS, "innings")
    _validate_columns(batting_rows, BATTING_COLUMNS, "batting_scorecard")
    _validate_columns(bowling_rows, BOWLING_COLUMNS, "bowling_scorecard")

    active_engine = engine or get_engine(settings)
    match_ids = sorted(
        {
            row["match_id"]
            for row in [*innings_rows, *batting_rows, *bowling_rows]
            if row.get("match_id")
        }
    )

    delete_clause = ", ".join(f":match_id_{index}" for index, _ in enumerate(match_ids))
    delete_params = {f"match_id_{index}": match_id for index, match_id in enumerate(match_ids)}

    with active_engine.begin() as conn:
        if match_ids:
            for table_name in ("batting_scorecard", "bowling_scorecard", "innings"):
                conn.execute(
                    text(f"DELETE FROM {table_name} WHERE match_id IN ({delete_clause})"),
                    delete_params,
                )

        if innings_rows:
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
                    """
                ),
                innings_rows,
            )

        if batting_rows:
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
                    """
                ),
                batting_rows,
            )

        if bowling_rows:
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
                    """
                ),
                bowling_rows,
            )


def load_innings(rows: list[dict], engine: Engine | None = None, settings: Settings | None = None) -> None:
    replace_scorecards(rows, [], [], engine=engine, settings=settings)


def load_batting_scorecard(rows: list[dict], engine: Engine | None = None, settings: Settings | None = None) -> None:
    replace_scorecards([], rows, [], engine=engine, settings=settings)


def load_bowling_scorecard(rows: list[dict], engine: Engine | None = None, settings: Settings | None = None) -> None:
    replace_scorecards([], [], rows, engine=engine, settings=settings)
