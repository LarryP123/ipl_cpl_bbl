from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class CompetitionConfig:
    search_term: str
    keywords: tuple[str, ...]
    season_patterns: tuple[str, ...]
    season_label: str


TARGET_COMPETITIONS: dict[str, CompetitionConfig] = {
    "IPL": CompetitionConfig(
        search_term="Indian Premier League",
        keywords=("indian premier league",),
        season_patterns=("2025",),
        season_label="2025",
    ),
    "CPL": CompetitionConfig(
        search_term="Caribbean Premier League",
        keywords=("caribbean premier league",),
        season_patterns=("2025",),
        season_label="2025",
    ),
    "BBL": CompetitionConfig(
        search_term="Big Bash League",
        keywords=("big bash league",),
        season_patterns=("2024-25",),
        season_label="2024-25",
    ),
    "BPL": CompetitionConfig(
        search_term="Bangladesh Premier League",
        keywords=("bangladesh premier league",),
        season_patterns=("2024-25",),
        season_label="2024-25",
    ),
    "LPL": CompetitionConfig(
        search_term="Lanka Premier League",
        keywords=("lanka premier league",),
        season_patterns=("2024",),
        season_label="2024",
    ),
    "PSL": CompetitionConfig(
        search_term="Pakistan Super League",
        keywords=("pakistan super league",),
        season_patterns=("2024",),
        season_label="2024",
    ),
    "SA20": CompetitionConfig(
        search_term="SA20",
        keywords=("sa20",),
        season_patterns=("2025",),
        season_label="2025",
    ),
}


ROOT_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    api_key: str | None
    base_url: str
    request_timeout: tuple[int, int]
    request_retries: int
    request_backoff_seconds: float
    root_dir: Path
    data_dir: Path
    sql_dir: Path
    exports_dir: Path
    outputs_dir: Path
    validation_dir: Path
    reports_dir: Path
    db_path: Path
    analytics_layer_sql: Path
    export_queries: dict[str, Path]
    dashboard_files: dict[str, str]
    target_competitions: dict[str, CompetitionConfig]

    @property
    def series_endpoint(self) -> str:
        return f"{self.base_url}/series"

    @property
    def series_info_endpoint(self) -> str:
        return f"{self.base_url}/series_info"

    @property
    def match_scorecard_endpoint(self) -> str:
        return f"{self.base_url}/match_scorecard"

    @property
    def database_url(self) -> str:
        return f"sqlite:///{self.db_path}"

    @property
    def search_terms(self) -> list[str]:
        return [config.search_term for config in self.target_competitions.values()]

    def ensure_directories(self) -> None:
        for directory in (
            self.data_dir,
            self.exports_dir,
            self.outputs_dir,
            self.validation_dir,
            self.reports_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    sql_dir = ROOT_DIR / "sql"
    exports_dir = ROOT_DIR / "exports"
    outputs_dir = ROOT_DIR / "outputs"

    export_queries = {
        "01_best_overall_batters": sql_dir / "01_best_overall_batters.sql",
        "02_best_by_league": sql_dir / "02_best_by_league.sql",
        "03_high_volume_batters": sql_dir / "03_high_volume_batters.sql",
        "04_efficiency_plus_aggression": sql_dir / "04_efficiency_plus_aggression.sql",
        "05_boundary_hitters": sql_dir / "05_boundary_hitters.sql",
        "06_explosive_hitters": sql_dir / "06_explosive_hitters.sql",
        "07_reliable_anchors": sql_dir / "07_reliable_anchors.sql",
        "08_underrated_players": sql_dir / "08_underrated_players.sql",
        "09_multi_league_performers": sql_dir / "09_multi_league_performers.sql",
        "10_league_environment": sql_dir / "10_league_environment.sql",
        "11_role_breakdown": sql_dir / "11_role_breakdown.sql",
        "12_most_consistent_players": sql_dir / "12_most_consistent_players.sql",
        "13_recent_form_last_5": sql_dir / "13_recent_form_last_5.sql",
        "14_form_vs_season": sql_dir / "14_form_vs_season.sql",
    }

    dashboard_files = {
        "overall": "01_best_overall_batters.csv",
        "by_league": "02_best_by_league.csv",
        "high_volume": "03_high_volume_batters.csv",
        "efficiency": "04_efficiency_plus_aggression.csv",
        "boundary": "05_boundary_hitters.csv",
        "explosive": "06_explosive_hitters.csv",
        "anchors": "07_reliable_anchors.csv",
        "underrated": "08_underrated_players.csv",
        "multi_league": "09_multi_league_performers.csv",
        "league_environment": "10_league_environment.csv",
        "roles": "11_role_breakdown.csv",
        "consistent": "12_most_consistent_players.csv",
        "recent_form": "13_recent_form_last_5.csv",
        "form_vs_season": "14_form_vs_season.csv",
    }

    return Settings(
        api_key=os.getenv("CRICKETDATA_API_KEY"),
        base_url="https://api.cricapi.com/v1",
        request_timeout=(5, 20),
        request_retries=3,
        request_backoff_seconds=1.0,
        root_dir=ROOT_DIR,
        data_dir=ROOT_DIR / "data",
        sql_dir=sql_dir,
        exports_dir=exports_dir,
        outputs_dir=outputs_dir,
        validation_dir=outputs_dir / "validation",
        reports_dir=outputs_dir / "reports",
        db_path=ROOT_DIR / "data" / "cricket.db",
        analytics_layer_sql=sql_dir / "00_create_analytics_layer.sql",
        export_queries=export_queries,
        dashboard_files=dashboard_files,
        target_competitions=TARGET_COMPETITIONS,
    )
