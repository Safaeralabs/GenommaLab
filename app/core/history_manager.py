"""Persist execution history to a JSON file."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from app.config import settings


HISTORY_FILE = settings.RUNTIME_ROOT / "execution_history.json"
MAX_ENTRIES = 200


def save_execution(
    year: int,
    week: int,
    total: int,
    success_count: int,
    failure_count: int,
    duration_seconds: float,
    failed_providers: list[str],
    homologation_rows: int,
    logger: logging.Logger | None = None,
) -> None:
    """Append an execution record to the history file."""
    entry = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "year": year,
        "week": week,
        "total": total,
        "success": success_count,
        "failed": failure_count,
        "duration_s": round(duration_seconds, 1),
        "failed_providers": failed_providers,
        "homologation_rows": homologation_rows,
    }
    try:
        history = _load_raw()
        history.append(entry)
        # Keep only the most recent MAX_ENTRIES
        if len(history) > MAX_ENTRIES:
            history = history[-MAX_ENTRIES:]
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        if logger:
            logger.warning("No se pudo guardar historial: %s", exc)


def load_history() -> list[dict]:
    """Return all history entries, newest first."""
    return list(reversed(_load_raw()))


def _load_raw() -> list[dict]:
    if not HISTORY_FILE.exists():
        return []
    try:
        return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
