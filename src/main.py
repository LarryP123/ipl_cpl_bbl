from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.config import get_settings
from src.extract import build_client, filter_target_series
from src.load import create_tables, get_engine, load_matches, replace_scorecards
from src.logging_utils import configure_logging
from src.transform import get_competition_metadata, parse_scorecard
from src.validate import (
    run_post_load_checks,
    validate_batting_scorecard,
    validate_settings,
    write_pipeline_summary,
)


logger = logging.getLogger(__name__)


def collect_target_series(client, settings) -> tuple[list[dict], list[dict[str, str]]]:
    all_series: list[dict] = []
    failures: list[dict[str, str]] = []

    for search_term in settings.search_terms:
        try:
            all_series.extend(client.get_series(search_term))
        except Exception as exc:
            failures.append({"search_term": search_term, "error": str(exc)})

    filtered_series = filter_target_series(all_series, settings=settings)
    return filtered_series, failures


def _build_match_row(match: dict, series_name: str, metadata: dict[str, Any]) -> dict[str, Any]:
    teams = match.get("teams", [])
    team_1 = teams[0] if len(teams) > 0 else None
    team_2 = teams[1] if len(teams) > 1 else None

    return {
        "match_id": match.get("id"),
        "raw_series_name": series_name,
        "competition": metadata["competition"],
        "season_label": metadata["season_label"],
        "match_date": match.get("date"),
        "match_type": match.get("matchType"),
        "team_1": team_1,
        "team_2": team_2,
        "venue": match.get("venue"),
        "result_text": match.get("status"),
        "status": match.get("status"),
    }


def run() -> dict:
    configure_logging()
    settings = get_settings()
    settings.ensure_directories()
    validate_settings(settings)

    client = build_client(settings)
    engine = get_engine(settings)

    logger.info("Collecting target series metadata.")
    filtered_series, search_failures = collect_target_series(client, settings)

    if not filtered_series:
        raise RuntimeError(
            "No target series found. Check the API key, upstream API availability, or series filters."
        )

    logger.info("Selected %s target series.", len(filtered_series))

    match_rows: list[dict] = []
    innings_rows: list[dict] = []
    batting_rows: list[dict] = []
    bowling_rows: list[dict] = []

    series_failures: list[dict[str, str]] = []
    scorecard_failures: list[dict[str, str]] = []
    summary: dict[str, Any] = {
        "status": "running",
        "series_selected": 0,
        "matches_collected": 0,
        "innings_collected": 0,
        "batting_rows_collected": 0,
        "bowling_rows_collected": 0,
        "series_search_failures": search_failures,
        "series_fetch_failures": series_failures,
        "scorecard_failures": scorecard_failures,
    }

    try:
        for series in filtered_series:
            series_id = series.get("id")
            series_name = series.get("name", "")
            metadata = get_competition_metadata(series_name, settings=settings)

            try:
                matches = client.get_matches_from_series_info(series_id)
            except Exception as exc:
                series_failures.append({"series_name": series_name, "error": str(exc)})
                logger.warning("Failed to fetch matches for %s: %s", series_name, exc)
                continue

            logger.info("Fetched %s matches for %s.", len(matches), series_name)

            for match in matches:
                match_row = _build_match_row(match, series_name, metadata)
                if not match_row["match_id"]:
                    logger.warning("Skipping match with no match_id in %s.", series_name)
                    continue

                match_rows.append(match_row)
                teams = match.get("teams", [])
                team_1 = teams[0] if len(teams) > 0 else None
                team_2 = teams[1] if len(teams) > 1 else None

                try:
                    scorecard_data = client.get_match_scorecard(match_row["match_id"])
                    parsed_innings, parsed_batting, parsed_bowling = parse_scorecard(
                        match_row["match_id"],
                        scorecard_data,
                        team_1,
                        team_2,
                    )
                    innings_rows.extend(parsed_innings)
                    batting_rows.extend(parsed_batting)
                    bowling_rows.extend(parsed_bowling)
                except Exception as exc:
                    scorecard_failures.append(
                        {
                            "series_name": series_name,
                            "match_id": str(match_row["match_id"]),
                            "error": str(exc),
                        }
                    )
                    logger.warning(
                        "Failed to parse scorecard for match %s (%s): %s",
                        match_row["match_id"],
                        series_name,
                        exc,
                    )

        if not match_rows:
            raise RuntimeError("No matches were collected, so the pipeline stopped before loading.")

        logger.info(
            "Collection summary: %s matches, %s innings, %s batting rows, %s bowling rows.",
            len(match_rows),
            len(innings_rows),
            len(batting_rows),
            len(bowling_rows),
        )

        summary.update(
            {
                "series_selected": len(filtered_series),
                "matches_collected": len(match_rows),
                "innings_collected": len(innings_rows),
                "batting_rows_collected": len(batting_rows),
                "bowling_rows_collected": len(bowling_rows),
            }
        )

        batting_validation = validate_batting_scorecard(pd.DataFrame(batting_rows))
        summary["batting_validation"] = batting_validation
        if batting_validation["status"] == "failed":
            raise RuntimeError("; ".join(batting_validation["hard_failures"]))
        if batting_validation["status"] == "warning":
            logger.warning(
                "Batting validation completed with warnings: %s",
                "; ".join(batting_validation["warnings"]),
            )

        create_tables(engine=engine)
        load_matches(match_rows, engine=engine)
        replace_scorecards(
            innings_rows=innings_rows,
            batting_rows=batting_rows,
            bowling_rows=bowling_rows,
            engine=engine,
        )

        post_load_validation = run_post_load_checks(settings=settings, write_report=True)
        summary["post_load_validation"] = post_load_validation
        has_warnings = any(
            [
                bool(search_failures),
                bool(series_failures),
                bool(scorecard_failures),
                batting_validation["status"] == "warning",
                post_load_validation["status"] != "passed",
            ]
        )
        summary["status"] = "completed_with_warnings" if has_warnings else "completed"
        write_pipeline_summary(settings.reports_dir / "pipeline_refresh_summary.json", summary)
        if has_warnings:
            logger.warning("Pipeline completed with warnings.")
        else:
            logger.info("Pipeline completed successfully.")
        return summary
    except Exception as exc:
        summary.update(
            {
                "status": "failed",
                "error": str(exc),
            }
        )
        write_pipeline_summary(settings.reports_dir / "pipeline_refresh_summary.json", summary)
        logger.exception("Pipeline failed before completion.")
        raise


if __name__ == "__main__":
    run()
