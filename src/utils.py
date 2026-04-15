from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def normalise_text(text: str | None) -> str:
    if text is None:
        return ""
    cleaned = text.strip().lower()
    return re.sub(r"\s+", " ", cleaned)


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent_dir(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")
