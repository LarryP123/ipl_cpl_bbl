from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from src.config import Settings, get_settings
from src.utils import write_json


def validate_settings(settings: Settings) -> None:
    if not settings.db_path.parent.exists():
        raise ValueError(f"Data directory does not exist: {settings.db_path.parent}")

    if not settings.sql_dir.exists():
        raise ValueError(f"SQL directory does not exist: {settings.sql_dir}")

    if not settings.analytics_layer_sql.exists():
        raise ValueError(f"Analytics layer SQL file not found: {settings.analytics_layer_sql}")

    missing_queries = [
        str(path)
        for path in settings.export_queries.values()
        if not path.exists()
    ]
    if missing_queries:
        raise ValueError(f"Missing export SQL files: {', '.join(missing_queries)}")


def validate_batting_scorecard(df: pd.DataFrame) -> dict:
    if df.empty:
        raise ValueError("Batting scorecard dataset is empty.")

    required_cols = [
        "batting_id",
        "match_id",
        "innings_number",
        "player_name",
        "team",
        "runs",
        "balls",
        "fours",
        "sixes",
        "strike_rate",
    ]

    missing_columns = [column for column in required_cols if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    hard_failures: list[str] = []
    warnings: list[str] = []
    warning_samples: dict[str, list[dict]] = {}

    key_nulls = df[["batting_id", "match_id", "player_name", "team"]].isna().sum().to_dict()
    for column, null_count in key_nulls.items():
        if null_count:
            hard_failures.append(f"Null values found in {column}: {null_count}")

    duplicate_ids = int(df["batting_id"].duplicated().sum())
    if duplicate_ids:
        hard_failures.append(f"Duplicate batting_id values found: {duplicate_ids}")

    duplicate_business_keys = int(
        df.duplicated(subset=["match_id", "innings_number", "player_name", "team"]).sum()
    )
    if duplicate_business_keys:
        hard_failures.append(
            "Duplicate player innings detected on (match_id, innings_number, player_name, team): "
            f"{duplicate_business_keys}"
        )

    numeric_cols = ["runs", "balls", "fours", "sixes", "strike_rate"]
    numeric_df = df[numeric_cols].fillna(0)
    for column in numeric_cols:
        if (numeric_df[column] < 0).any():
            hard_failures.append(f"Negative values found in {column}")

    warning_rules = {
        "runs_above_250": df["runs"].fillna(0) > 250,
        "strike_rate_above_400_small_sample": (
            (df["strike_rate"].fillna(0) > 400) & (df["balls"].fillna(0) < 8)
        ),
        "strike_rate_above_400_large_sample": (
            (df["strike_rate"].fillna(0) > 400) & (df["balls"].fillna(0) >= 8)
        ),
        "boundary_runs_exceed_total_runs": (
            (df["fours"].fillna(0) * 4 + df["sixes"].fillna(0) * 6) > df["runs"].fillna(0)
        ),
        "runs_scored_from_zero_balls": (
            (df["balls"].fillna(0) == 0) & (df["runs"].fillna(0) > 0)
        ),
    }

    warning_counts: dict[str, int] = {}
    sample_columns = ["batting_id", "match_id", "player_name", "team", "runs", "balls", "fours", "sixes", "strike_rate"]
    for rule_name, mask in warning_rules.items():
        issue_count = int(mask.sum())
        warning_counts[rule_name] = issue_count
        if issue_count:
            warnings.append(f"{rule_name}: {issue_count}")
            sample_rows = df.loc[mask, sample_columns].head(5).to_dict(orient="records")
            warning_samples[rule_name] = sample_rows

    if hard_failures:
        return {
            "status": "failed",
            "rows_checked": int(len(df)),
            "hard_failures": hard_failures,
            "warnings": warnings,
            "warning_counts": warning_counts,
            "warning_samples": warning_samples,
        }

    if warnings:
        return {
            "status": "warning",
            "rows_checked": int(len(df)),
            "hard_failures": [],
            "warnings": warnings,
            "warning_counts": warning_counts,
            "warning_samples": warning_samples,
        }

    return {
        "status": "passed",
        "rows_checked": int(len(df)),
        "hard_failures": [],
        "warnings": [],
        "warning_counts": warning_counts,
        "warning_samples": {},
    }


def run_post_load_checks(
    settings: Settings | None = None,
    write_report: bool = True,
) -> dict:
    active_settings = settings or get_settings()
    connection = sqlite3.connect(active_settings.db_path)
    connection.row_factory = sqlite3.Row

    checks = [
        (
            "null_key_columns",
            """
            SELECT COUNT(*) AS issue_count
            FROM (
                SELECT match_id AS key_value
                FROM matches
                WHERE match_id IS NULL OR raw_series_name IS NULL
                UNION ALL
                SELECT batting_id
                FROM batting_scorecard
                WHERE batting_id IS NULL OR match_id IS NULL OR player_name IS NULL OR team IS NULL
            )
            """,
            0,
            "failure",
        ),
        (
            "duplicate_batting_business_keys",
            """
            SELECT COUNT(*) AS issue_count
            FROM (
                SELECT match_id, innings_number, player_name, team
                FROM batting_scorecard
                GROUP BY match_id, innings_number, player_name, team
                HAVING COUNT(*) > 1
            )
            """,
            0,
            "failure",
        ),
        (
            "suspicious_batting_values",
            """
            SELECT COUNT(*) AS issue_count
            FROM batting_scorecard
            WHERE COALESCE(runs, 0) > 250
               OR (COALESCE(fours, 0) * 4 + COALESCE(sixes, 0) * 6) > COALESCE(runs, 0)
               OR (COALESCE(balls, 0) = 0 AND COALESCE(runs, 0) > 0)
            """,
            0,
            "warning",
        ),
        (
            "extreme_strike_rates_small_sample",
            """
            SELECT COUNT(*) AS issue_count
            FROM batting_scorecard
            WHERE COALESCE(strike_rate, 0) > 400
              AND COALESCE(balls, 0) < 8
            """,
            0,
            "warning",
        ),
        (
            "extreme_strike_rates_large_sample",
            """
            SELECT COUNT(*) AS issue_count
            FROM batting_scorecard
            WHERE COALESCE(strike_rate, 0) > 400
              AND COALESCE(balls, 0) >= 8
            """,
            0,
            "warning",
        ),
        (
            "missing_league_mappings",
            """
            SELECT COUNT(*) AS issue_count
            FROM matches
            WHERE competition IS NULL OR season_label IS NULL
            """,
            0,
            "failure",
        ),
        (
            "matches_without_innings",
            """
            SELECT COUNT(*) AS issue_count
            FROM matches m
            LEFT JOIN innings i
             ON m.match_id = i.match_id
            WHERE i.match_id IS NULL
            """,
            0,
            "warning",
        ),
        (
            "innings_without_batting_rows",
            """
            SELECT COUNT(*) AS issue_count
            FROM innings i
            LEFT JOIN batting_scorecard b
              ON i.match_id = b.match_id
             AND i.innings_number = b.innings_number
            WHERE b.batting_id IS NULL
            """,
            0,
            "failure",
        ),
    ]

    check_results = []
    overall_status = "passed"
    failed_checks = 0
    warning_checks = 0

    for name, query, expected_max, severity in checks:
        issue_count = int(connection.execute(query).fetchone()["issue_count"])
        status = "passed"
        if issue_count > expected_max:
            if severity == "failure":
                status = "failed"
                overall_status = "failed"
                failed_checks += 1
            else:
                status = "warning"
                if overall_status == "passed":
                    overall_status = "warning"
                warning_checks += 1
        check_results.append(
            {
                "check": name,
                "status": status,
                "severity": severity,
                "issue_count": issue_count,
                "expected_max": expected_max,
            }
        )

    summary = {
        "status": overall_status,
        "checks_run": len(check_results),
        "failed_checks": failed_checks,
        "warning_checks": warning_checks,
        "checks": check_results,
    }

    if write_report:
        report_path = active_settings.validation_dir / "post_load_validation_report.json"
        write_json(report_path, summary)

    connection.close()
    return summary


def write_pipeline_summary(report_path: Path, summary: dict) -> None:
    write_json(report_path, summary)


def main() -> dict:
    settings = get_settings()
    settings.ensure_directories()
    validate_settings(settings)
    return run_post_load_checks(settings=settings, write_report=True)


if __name__ == "__main__":
    main()
