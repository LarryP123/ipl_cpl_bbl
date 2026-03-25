import logging
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

def run_pipeline():
    logging.info("Processing cricket match data")

    df = extract_data()
    logging.info("Extracted %s rows", len(df))

    cleaned = transform_data(df)
    logging.info("Transformed %s rows", len(cleaned))

    load_data(cleaned, str(DB_FILE))
    logging.info("Loaded data into SQLite database at %s", DB_FILE)

    print("Pipeline complete!")

if __name__ == "__main__":
    run_pipeline()