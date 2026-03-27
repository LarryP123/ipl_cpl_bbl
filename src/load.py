import pandas as pd
from sqlalchemy import create_engine, text


def load_data(df: pd.DataFrame, db_path: str, mode: str = "replace") -> None:
    engine = create_engine(f"sqlite:///{db_path}")

    if df.empty:
        return

    mode = mode.lower()

    with engine.begin() as conn:
        if mode == "replace":
            df.to_sql("matches", con=conn, if_exists="replace", index=False)

        elif mode == "append":
            df.to_sql("matches", con=conn, if_exists="append", index=False)

        elif mode == "upsert":
            # Create table if it doesn't already exist
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS matches (
                    match_id TEXT PRIMARY KEY,
                    match_name TEXT,
                    match_type TEXT,
                    status TEXT,
                    venue TEXT,
                    date TEXT,
                    teams TEXT,
                    score_summary TEXT,
                    loaded_at TEXT
                )
            """))

            # Insert or update each row
            upsert_sql = text("""
                INSERT INTO matches (
                    match_id, match_name, match_type, status,
                    venue, date, teams, score_summary, loaded_at
                )
                VALUES (
                    :match_id, :match_name, :match_type, :status,
                    :venue, :date, :teams, :score_summary, :loaded_at
                )
                ON CONFLICT(match_id) DO UPDATE SET
                    match_name = excluded.match_name,
                    match_type = excluded.match_type,
                    status = excluded.status,
                    venue = excluded.venue,
                    date = excluded.date,
                    teams = excluded.teams,
                    score_summary = excluded.score_summary,
                    loaded_at = excluded.loaded_at
            """)

            records = df.to_dict(orient="records")
            for record in records:
                conn.execute(upsert_sql, record)

        else:
            raise ValueError("mode must be one of: replace, append, upsert")