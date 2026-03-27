import logging
import argparse
from pathlib import Path

from extract import extract_data
from transform import transform_data
from load import load_data

BASE_DIR = Path(__file__).resolve().parent.parent
DB_FILE = BASE_DIR / "data" / "processed" / "pipeline.db"
LOG_FILE = BASE_DIR / "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def run_pipeline(mode: str = "replace"):
    try:
        logging.info("Fetching real cricket API data")

        df = extract_data()
        logging.info("Extracted %s raw rows", len(df))

        cleaned = transform_data(df)
        logging.info("Transformed %s rows", len(cleaned))

        load_data(cleaned, str(DB_FILE), mode=mode)
        logging.info("Loaded data into SQLite at %s using mode=%s", DB_FILE, mode)

        print(f"Pipeline complete using mode='{mode}'")

    except Exception as e:
        logging.exception("Pipeline failed: %s", e)
        print(f"Pipeline failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run cricket ETL pipeline")
    parser.add_argument(
        "--mode",
        choices=["replace", "append", "upsert"],
        default="replace",
        help="Database write mode"
    )
    args = parser.parse_args()

    run_pipeline(mode=args.mode)