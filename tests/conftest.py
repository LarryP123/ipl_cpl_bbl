from __future__ import annotations

from pathlib import Path

import pytest

from src.config import Settings, get_settings


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    base_settings = get_settings()
    data_dir = tmp_path / "data"
    exports_dir = tmp_path / "exports"
    outputs_dir = tmp_path / "outputs"
    data_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        api_key="test-key",
        base_url=base_settings.base_url,
        request_timeout=base_settings.request_timeout,
        request_retries=base_settings.request_retries,
        request_backoff_seconds=base_settings.request_backoff_seconds,
        root_dir=tmp_path,
        data_dir=data_dir,
        sql_dir=base_settings.sql_dir,
        exports_dir=exports_dir,
        outputs_dir=outputs_dir,
        validation_dir=outputs_dir / "validation",
        reports_dir=outputs_dir / "reports",
        db_path=data_dir / "test_cricket.db",
        analytics_layer_sql=base_settings.analytics_layer_sql,
        export_queries=base_settings.export_queries,
        dashboard_files=base_settings.dashboard_files,
        target_competitions=base_settings.target_competitions,
    )
