import pandas as pd
from datetime import datetime, timezone

def _team_names(team_info):
    if isinstance(team_info, list):
        names = [team.get("name") for team in team_info if isinstance(team, dict) and team.get("name")]
        return " vs ".join(names)
    return None

def _score_summary(score_data):
    if isinstance(score_data, list):
        parts = []
        for innings in score_data:
            if not isinstance(innings, dict):
                continue
            r = innings.get("r")
            w = innings.get("w")
            o = innings.get("o")
            inning = innings.get("inning")
            if r is not None and w is not None and o is not None:
                parts.append(f"{inning}: {r}/{w} ({o} ov)")
        return " | ".join(parts) if parts else None
    return None

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    cleaned = pd.DataFrame()

    cleaned["match_id"] = df.get("id")
    cleaned["match_name"] = df.get("name")
    cleaned["match_type"] = df.get("matchType")
    cleaned["status"] = df.get("status")
    cleaned["venue"] = df.get("venue")
    cleaned["date"] = df.get("date")
    cleaned["teams"] = df.get("teamInfo").apply(_team_names) if "teamInfo" in df.columns else None
    cleaned["score_summary"] = df.get("score").apply(_score_summary) if "score" in df.columns else None
    cleaned["loaded_at"] = datetime.now(timezone.utc).isoformat()

    cleaned = cleaned.drop_duplicates(subset=["match_id"])
    return cleaned