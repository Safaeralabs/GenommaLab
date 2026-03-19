"""Helpers for filesystem operations."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from app.config import settings


def ensure_directories() -> None:
    """Create runtime directories if they do not exist."""
    for directory in (
        settings.RUNTIME_ROOT,
        settings.DATA_DIR,
        settings.DOWNLOADS_DIR,
        settings.POSTPROCESSED_DIR,
        settings.LOGS_DIR,
        settings.SCREENSHOTS_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def ensure_default_provider_catalog() -> Path:
    """Copy the bundled providers JSON catalog if missing."""
    ensure_directories()

    if settings.PROVIDER_CATALOG_PATH.exists():
        return settings.PROVIDER_CATALOG_PATH

    if settings.PROVIDER_CATALOG_TEMPLATE.exists():
        shutil.copy2(settings.PROVIDER_CATALOG_TEMPLATE, settings.PROVIDER_CATALOG_PATH)

    return settings.PROVIDER_CATALOG_PATH


def configure_playwright_runtime() -> None:
    """Point Playwright to bundled browsers when available."""
    if settings.BUNDLED_BROWSERS_DIR.exists():
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(settings.BUNDLED_BROWSERS_DIR)


def open_directory(path: Path) -> None:
    """Open a directory with the platform file explorer."""
    resolved_path = path.resolve()

    if sys.platform.startswith("win"):
        os.startfile(resolved_path)  # type: ignore[attr-defined]
        return

    if sys.platform == "darwin":
        subprocess.Popen(["open", str(resolved_path)])
        return

    subprocess.Popen(["xdg-open", str(resolved_path)])
