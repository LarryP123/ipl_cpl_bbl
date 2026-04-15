# Cricket Data Pipeline: T20 Franchise Analytics

An end-to-end cricket analytics project that ingests match data from CricAPI, transforms nested scorecards into a relational SQLite model, builds a reusable SQL analytics layer, exports recruiter-friendly datasets, and serves an interactive Streamlit dashboard.

This repo is designed to showcase practical data engineering and analytics work rather than isolated notebooks. It demonstrates how to move from unstable API responses to clean, queryable outputs with validation, rerunnable loads, tests, and a lightweight reporting layer.

## What This Project Does

- Extracts T20 franchise cricket data for major leagues including IPL, BBL, CPL, BPL, PSL, LPL, and SA20
- Normalises nested API payloads into relational tables for matches, innings, batting scorecards, and bowling scorecards
- Builds a modular SQL analytics layer for player leaderboards, league comparisons, role breakdowns, and recent-form analysis
- Exports consistent CSV artifacts for downstream analysis and dashboard consumption
- Surfaces key insights in a Streamlit app aimed at fast exploration and portfolio presentation
- Runs lightweight data quality checks after refreshes to catch nulls, suspicious values, missing mappings, and incomplete scorecards

## Architecture

```text
CricAPI
  -> extract client with retries and error handling
  -> transform layer for parsing and league mapping
  -> SQLite load layer with indexes and rerunnable scorecard replacement
  -> SQL analytics views and leaderboard queries
  -> CSV exports + validation/report artifacts
  -> Streamlit dashboard
```

## Tech Stack

- Python
- Pandas
- Requests
- SQLAlchemy
- SQLite
- SQL
- Pytest
- Streamlit
- Plotly

## Project Structure

```text
app.py                     # Streamlit dashboard
export_results.py          # Analytics export runner
src/
  config.py                # Central settings and file paths
  extract.py               # API client and series discovery
  load.py                  # SQLite schema management and loading
  logging_utils.py         # Logging configuration
  main.py                  # End-to-end pipeline orchestration
  transform.py             # Scorecard parsing and league mapping
  utils.py                 # Shared filesystem / SQL helpers
  validate.py              # Config and data quality checks
sql/
  00_create_analytics_layer.sql
  01_*.sql - 14_*.sql      # Reusable analytics queries
tests/
  pytest coverage for config, transforms, loading, and exports
exports/                   # Reproducible CSV outputs
outputs/
  reports/                 # Pipeline and export summaries
  validation/              # Post-load data quality reports
data/
  cricket.db               # SQLite database
```

## Data Model

Core tables:

- `matches`: match-level metadata including competition and season mapping
- `innings`: innings-level totals and run-rate context
- `batting_scorecard`: batter innings records
- `bowling_scorecard`: bowler innings records

Analytics views:

- `analytics_batting_innings`: batting records enriched with competition, season, and match date
- `analytics_player_batting_summary`: reusable player-level batting aggregate
- `player_metrics`: compatibility alias for the main analytics summary view

## Reliability Features

- Centralised config and path management
- Logging-based pipeline flow instead of print-only execution
- Retry handling around unstable API calls
- Defensive parsing for malformed scorecards
- Rerunnable scorecard loading without duplicate accumulation
- Indexed SQLite tables for faster joins and exports
- Post-load validation checks written to JSON artifacts
- Pytest coverage for transforms, config, duplicate prevention, and SQL export sanity

## How To Run

### 1. Set your API key

```bash
export CRICKETDATA_API_KEY="your_api_key_here"
```

### 2. Refresh the raw database

```bash
python -m src.main
```

This writes:

- SQLite data to [data/cricket.db](/Users/laurencepengelly/cricket-data-pipeline/data/cricket.db)
- A pipeline refresh summary to [outputs/reports/pipeline_refresh_summary.json](/Users/laurencepengelly/cricket-data-pipeline/outputs/reports/pipeline_refresh_summary.json)
- A validation report to [outputs/validation/post_load_validation_report.json](/Users/laurencepengelly/cricket-data-pipeline/outputs/validation/post_load_validation_report.json)

The pipeline can complete with warnings when CricAPI is missing individual scorecards or when unusual-but-plausible batting records appear. Those cases are captured in the summary and validation artifacts instead of silently failing.

### 3. Build analytics exports

```bash
python export_results.py
```

This produces consistent CSVs in [exports](/Users/laurencepengelly/cricket-data-pipeline/exports) and an export manifest in [outputs/reports/export_manifest.json](/Users/laurencepengelly/cricket-data-pipeline/outputs/reports/export_manifest.json).

The `exports/` folder is intentionally committed so the dashboard and portfolio review can work without forcing a live API refresh. Local working artifacts in `data/` and `outputs/` are gitignored.

### 4. Launch the dashboard

```bash
streamlit run app.py
```

## Test Suite

Run the lightweight checks with:

```bash
pytest
```

The suite focuses on:

- Competition mapping and scorecard parsing
- Configuration validation
- Rerunnable load behaviour without duplicate scorecard rows
- SQL analytics/export sanity checks
- Validation severity handling for warning-level versus failure-level data quality issues

## Example Outputs

Key export artifacts include:

- `01_best_overall_batters.csv`
- `08_underrated_players.csv`
- `10_league_environment.csv`
- `13_recent_form_last_5.csv`
- `14_form_vs_season.csv`

Recommended screenshots for the README or portfolio write-up:

- Dashboard landing view with key takeaways and batting index leaderboard
- League context tab showing cross-league batting environment differences
- Player explorer tab for one player across competitions
- Validation report snippet showing data quality checks after refresh

## What This Project Demonstrates

- Designing a maintainable ETL pipeline around a third-party API
- Turning semi-structured sports data into a clean relational model
- Building analytics-ready SQL on top of operational tables
- Handling reruns, validation, indexing, and failure visibility in a solo project
- Publishing reproducible artifacts that support both dashboarding and offline analysis
- Presenting technical work in a portfolio-friendly way for data engineering and analytics roles

## Tradeoffs

- SQLite keeps the project lightweight and easy to run locally, but it is not intended as a multi-user production warehouse
- Data quality checks are intentionally lightweight and fast rather than a full validation framework
- The project prioritises reproducibility and clarity over orchestration tooling such as Airflow or dbt to stay realistic for a solo portfolio repo

## Good Follow-Up Enhancements

- Add historical snapshots to compare refreshes over time
- Version export manifests with timestamps for easier auditability
- Add bowling-focused analytics to complement the batting layer
- Containerise the app and pipeline for one-command setup
