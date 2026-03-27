# Cricket Data Pipeline

This project is an end-to-end ETL and API service that ingests live cricket match data, transforms nested API responses into structured records, stores them in SQLite, and exposes them through FastAPI endpoints.

## Features
- Ingests live cricket data from external API
- End-to-end ETL pipeline (extract, transform, load)
- FastAPI service for querying data
- Filterable API queries (e.g. by player, team)
- Multiple load strategies: overwrite, append, upsert

## Architecture
Cricket API → Extract → Transform → Load → SQLite → FastAPI

##  Tech Stack
- Python
- Pandas
- SQLAlchemy
- SQLite
- FastAPI
- Uvicorn
- dotenv

## Project Structure

my_data_pipeline/
├── src/
│ ├── api.py
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
pip install -r requirements.txt
python src/main.py --mode upsert
uvicorn src.api:app --reload
```
## Example API Endpoints
GET /players
GET /players?team=India