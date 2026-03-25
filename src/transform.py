import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["run_rate"] = df["runs"] / 50
    df["result"] = df["runs"].apply(lambda x: "High Score" if x >= 260 else "Low Score")
    return df