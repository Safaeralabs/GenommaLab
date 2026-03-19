"""Run a compatibility validation across all Abako providers."""

from __future__ import annotations

import json
import os
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from app.config import settings
from app.core.excel_reader import ExcelReader
from app.core.logger_manager import configure_logging
from app.portals.portal_a import PortalA


@dataclass(slots=True)
class ValidationRow:
    """One provider validation outcome."""

    proveedor: str
    login_url: str
    success: bool
    duration_seconds: float
    message: str


def _snapshot_files(root: Path) -> set[Path]:
    if not root.exists():
        return set()
    return {path for path in root.rglob("*") if path.is_file()}


def _cleanup_artifacts(created_files: set[Path], created_dirs: set[Path]) -> None:
    for file_path in sorted(created_files, reverse=True):
        if file_path.exists():
            file_path.unlink()

    for dir_path in sorted(created_dirs, key=lambda item: len(item.parts), reverse=True):
        if dir_path.exists():
            shutil.rmtree(dir_path, ignore_errors=True)


def main() -> int:
    os.environ.setdefault("RPA_HEADLESS", "1")

    settings.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    settings.DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    settings.SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    logger = configure_logging()
    excel_path = PROJECT_DIR / "Accesob2b.xlsx"
    proveedores = [
        proveedor
        for proveedor in ExcelReader(excel_path).read_proveedores()
        if proveedor.portal_tipo == "portal_a"
    ]

    results: list[ValidationRow] = []
    total = len(proveedores)
    started_at = datetime.now()
    logger.info("Validacion masiva Abako iniciada. Proveedores: %s", total)

    for index, proveedor in enumerate(proveedores, start=1):
        logger.info("[%s/%s] Validando %s", index, total, proveedor.display_name)

        before_downloads = _snapshot_files(settings.DOWNLOADS_DIR)
        before_screenshots = _snapshot_files(settings.SCREENSHOTS_DIR)
        provider_dir = proveedor.target_download_dir(settings.DOWNLOADS_DIR)
        start_time = datetime.now()

        portal = PortalA(
            proveedor=proveedor,
            download_dir=provider_dir,
            screenshot_dir=settings.SCREENSHOTS_DIR,
            logger=logger,
        )
        result = portal.ejecutar()
        duration_seconds = round((datetime.now() - start_time).total_seconds(), 2)

        after_downloads = _snapshot_files(settings.DOWNLOADS_DIR)
        after_screenshots = _snapshot_files(settings.SCREENSHOTS_DIR)
        created_files = (after_downloads - before_downloads) | (after_screenshots - before_screenshots)
        created_dirs = {provider_dir} if provider_dir.exists() else set()
        _cleanup_artifacts(created_files, created_dirs)

        results.append(
            ValidationRow(
                proveedor=proveedor.display_name,
                login_url=proveedor.login_url,
                success=result.success,
                duration_seconds=duration_seconds,
                message=result.message,
            )
        )

    finished_at = datetime.now()
    success_count = sum(1 for item in results if item.success)
    failure_count = total - success_count

    report = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "total": total,
        "success_count": success_count,
        "failure_count": failure_count,
        "results": [asdict(item) for item in results],
    }

    report_path = settings.LOGS_DIR / f"abako_validation_{finished_at.strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Validacion masiva Abako finalizada. OK: %s | Fallidos: %s | Reporte: %s",
        success_count,
        failure_count,
        report_path,
    )

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
