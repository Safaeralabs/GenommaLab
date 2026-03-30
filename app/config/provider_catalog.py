from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence


CATALOG_PATH = Path(__file__).resolve().parents[2] / "data" / "providers.json"


def _load_default_catalog() -> Sequence[dict[str, object]]:
    if not CATALOG_PATH.exists():
        return []

    for encoding in ("utf-8", "latin-1"):
        try:
            return json.loads(CATALOG_PATH.read_text(encoding=encoding))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue

    return []


DEFAULT_PROVIDERS: Sequence[dict[str, object]] = _load_default_catalog()
