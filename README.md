# Cricket Data Pipeline

A Python data pipeline that extracts, transforms, and stores cricket match data for analysis.

## Features
- Extracts cricket match data
- Calculates run rate and scoring classification
- Stores data in SQLite database
- Logs pipeline execution

##  Tech Stack
- Python
- Pandas
- SQLAlchemy
- SQLite

## Project Structure

my_data_pipeline/
├── src/
│ ├── extract.py
│ ├── transform.py
│ ├── load.py
│ └── main.py
├── data/
│ ├── raw/
│ └── processed/
├── requirements.txt
└── README.md

##  How to Run

```bash
git clone <your-repo-url>
cd my_data_pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/main.py