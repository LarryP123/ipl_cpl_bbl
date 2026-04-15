from __future__ import annotations

import logging
import sqlite3

import pandas as pd

from src.config import Settings, get_settings
from src.logging_utils import configure_logging
from src.utils import load_sql, write_json
from src.validate import validate_settings


logger = logging.getLogger(__name__)


def _drop_legacy_object(connection: sqlite3.Connection, name: str) -> None:
    row = connection.execute(
        "SELECT type FROM sqlite_master WHERE name = ?",
        (name,),
    ).fetchone()
    if row is None:
        return

    object_type = row[0]
    if object_type == "table":
        connection.execute(f"DROP TABLE IF EXISTS {name}")
    elif object_type == "view":
        connection.execute(f"DROP VIEW IF EXISTS {name}")


def run_exports(settings: Settings | None = None) -> dict:
    configure_logging()
    active_settings = settings or get_settings()
    active_settings.ensure_directories()
    validate_settings(active_settings)

    connection = sqlite3.connect(active_settings.db_path)
    exported_files: list[dict[str, str | int]] = []

    try:
        _drop_legacy_object(connection, "player_metrics")
        connection.executescript(load_sql(active_settings.analytics_layer_sql))

        for export_name, sql_path in active_settings.export_queries.items():
            query = load_sql(sql_path)
            dataframe = pd.read_sql_query(query, connection)
            output_path = active_settings.exports_dir / f"{export_name}.csv"
            dataframe.to_csv(output_path, index=False)
            exported_files.append(
                {
                    "name": export_name,
                    "path": str(output_path),
                    "rows": int(len(dataframe)),
                }
            )
            logger.info("Exported %s (%s rows).", output_path.name, len(dataframe))
    finally:
        connection.close()

    summary = {"exports": exported_files, "total_exports": len(exported_files)}
    write_json(active_settings.reports_dir / "export_manifest.json", summary)
    return summary


if __name__ == "__main__":
    run_exports()
