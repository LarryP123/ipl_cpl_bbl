from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text

from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

app = FastAPI(title="Cricket Matches API")

BASE_DIR = Path(__file__).resolve().parent.parent
DB_FILE = BASE_DIR / "data" / "processed" / "pipeline.db"
DATABASE_URL = f"sqlite:///{DB_FILE}"

engine = create_engine(DATABASE_URL)


@app.get("/")
def root():
    return {"message": "Cricket Matches API is running"}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "database": str(DB_FILE),
        "db_exists": DB_FILE.exists()
    }


@app.get("/matches")
def get_matches(
    status: Optional[str] = Query(default=None),
    match_type: Optional[str] = Query(default=None),
    team: Optional[str] = Query(default=None),
    venue: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500)
):
    query = "SELECT * FROM matches WHERE 1=1"
    params = {}

    if status:
        query += " AND status LIKE :status"
        params["status"] = f"%{status}%"

    if match_type:
        query += " AND match_type LIKE :match_type"
        params["match_type"] = f"%{match_type}%"

    if team:
        query += " AND teams LIKE :team"
        params["team"] = f"%{team}%"

    if venue:
        query += " AND venue LIKE :venue"
        params["venue"] = f"%{venue}%"

    query += " ORDER BY loaded_at DESC LIMIT :limit"
    params["limit"] = limit

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = [dict(row._mapping) for row in result]

    return {
        "count": len(rows),
        "data": rows
    }


@app.get("/matches/live")
def get_live_matches(limit: int = Query(default=50, ge=1, le=500)):
    query = """
        SELECT * FROM matches
        WHERE status LIKE :live1
           OR status LIKE :live2
           OR status LIKE :live3
        ORDER BY loaded_at DESC
        LIMIT :limit
    """

    params = {
        "live1": "%live%",
        "live2": "%inning%",
        "live3": "%need%",
        "limit": limit
    }

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = [dict(row._mapping) for row in result]

    return {
        "count": len(rows),
        "data": rows
    }


@app.get("/matches/{match_id}")
def get_match(match_id: str):
    query = "SELECT * FROM matches WHERE match_id = :match_id"

    with engine.connect() as conn:
        result = conn.execute(text(query), {"match_id": match_id})
        row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Match not found")

    return dict(row._mapping)


@app.post("/refresh")
def refresh_data(mode: str = Query(default="replace", pattern="^(replace|append|upsert)$")):
    try:
        raw_df = extract_data()
        cleaned_df = transform_data(raw_df)
        load_data(cleaned_df, str(DB_FILE), mode=mode)

        return {
            "status": "success",
            "message": "Pipeline refreshed successfully",
            "mode": mode,
            "rows_loaded": len(cleaned_df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {str(e)}")