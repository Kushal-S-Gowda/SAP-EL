"""Write ingestion status for the dashboard without coupling to Spark."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def write_status(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    path.write_text(json.dumps(body, indent=2), encoding="utf-8")


def read_status(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
