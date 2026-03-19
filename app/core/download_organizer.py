"""Post-processing helpers for downloaded provider files."""

from __future__ import annotations

import json
import logging
import shutil
import unicodedata
from datetime import datetime
from pathlib import Path

from app.config import settings
from app.core.models import OrganizedFile, Proveedor


class DownloadOrganizer:
    """Classify and copy downloaded files to an organized output tree."""

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    def organize(
        self,
        proveedor: Proveedor,
        downloaded_files: list[Path],
        execution_dir: Path | None = None,
    ) -> list[OrganizedFile]:
        """Copy downloaded files to the post-processed folder and return their paths."""
        existing_files = [path for path in downloaded_files if path.exists()]
        if not existing_files:
            self.logger.warning(
                "[%s] No hay archivos descargados para postprocesar.",
                proveedor.display_name,
            )
            return []

        sanitized_name = self._sanitize_name(proveedor.carpeta or proveedor.display_name)

        if execution_dir is not None:
            provider_root = execution_dir / sanitized_name
        else:
            batch_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            provider_root = settings.POSTPROCESSED_DIR / sanitized_name / batch_stamp

        organized_items: list[OrganizedFile] = []
        manifest_rows: list[dict[str, str]] = []

        for source_path in existing_files:
            category = self._classify_file(source_path)
            destination_dir = provider_root / category
            destination_dir.mkdir(parents=True, exist_ok=True)

            destination_path = self._ensure_unique_path(destination_dir / source_path.name)
            shutil.copy2(source_path, destination_path)
            organized_items.append(OrganizedFile(path=destination_path, category=category))

            manifest_rows.append(
                {
                    "source": str(source_path),
                    "destination": str(destination_path),
                    "category": category,
                }
            )

        manifest_data: dict = {"proveedor": proveedor.display_name, "files": manifest_rows}
        if execution_dir is None:
            manifest_data["batch_stamp"] = provider_root.name

        manifest_path = provider_root / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        self.logger.info(
            "[%s] Postprocesado completado. Archivos organizados: %s | Carpeta: %s",
            proveedor.display_name,
            len(organized_items),
            provider_root,
        )
        return organized_items

    @staticmethod
    def _classify_file(source_path: Path) -> str:
        normalized_name = DownloadOrganizer._sanitize_name(source_path.stem)
        if "inventario" in normalized_name or "saldo" in normalized_name or "stock" in normalized_name:
            return "inventario"
        return "ventas"

    @staticmethod
    def _ensure_unique_path(base_path: Path) -> Path:
        if not base_path.exists():
            return base_path

        counter = 1
        while True:
            candidate = base_path.with_name(f"{base_path.stem}_{counter}{base_path.suffix}")
            if not candidate.exists():
                return candidate
            counter += 1

    @staticmethod
    def _sanitize_name(value: str) -> str:
        normalized = "".join(
            char
            for char in unicodedata.normalize("NFKD", value.strip())
            if not unicodedata.combining(char)
        )
        cleaned = []
        for char in normalized:
            if char.isalnum():
                cleaned.append(char.lower())
            elif char in {" ", "-", "_"}:
                cleaned.append("_")
        safe_value = "".join(cleaned).strip("_")
        while "__" in safe_value:
            safe_value = safe_value.replace("__", "_")
        return safe_value or "sin_nombre"
