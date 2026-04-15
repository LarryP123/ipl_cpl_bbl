from __future__ import annotations

from src.validate import validate_settings


def test_validate_settings_accepts_complete_configuration(test_settings):
    test_settings.ensure_directories()
    validate_settings(test_settings)
