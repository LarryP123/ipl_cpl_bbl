import pandas as pd

def extract_data():
    print("Extracting cricket data...")

    data = [
        {"match_id": 1, "team": "England", "runs": 250, "opponent": "India"},
        {"match_id": 2, "team": "Australia", "runs": 300, "opponent": "Pakistan"},
        {"match_id": 3, "team": "India", "runs": 275, "opponent": "England"},
        {"match_id": 4, "team": "Pakistan", "runs": 220, "opponent": "Australia"},
    ]

    return pd.DataFrame(data)