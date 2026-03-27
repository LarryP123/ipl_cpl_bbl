import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CRICKET_API_KEY")
BASE_URL = "https://api.cricapi.com/v1/currentMatches"

def extract_data() -> pd.DataFrame:
    if not API_KEY:
        raise ValueError("CRICKET_API_KEY is missing from .env")

    params = {
        "apikey": API_KEY,
        "offset": 0
    }

    print("Fetching live cricket data from CricAPI...")
    response = requests.get(BASE_URL, params=params, timeout=20)
    response.raise_for_status()

    payload = response.json()

    if payload.get("status") != "success":
        raise ValueError(f"API returned unexpected response: {payload}")

    matches = payload.get("data", [])

    if not matches:
        print("No current matches returned by the API.")
        return pd.DataFrame()

    return pd.DataFrame(matches)