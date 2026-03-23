"""Helpers to mirror output into the user's OneDrive directory."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from app.config import settings
from app.core.models import OrganizedFile


def sync_paths_to_onedrive(
    file_paths: list[Path],
    subfolder: str,
    logger: logging.Logger | None = None,
) -> None:
    """Copy the given files inside the configured OneDrive sync folder."""
    root = settings.ONEDRIVE_SYNC_DIR
    if root is None:
        return

    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        if logger:
            logger.warning("No se pudo crear la carpeta OneDrive: %s", exc)
        return

    base_target = root / subfolder
    base_target.mkdir(parents=True, exist_ok=True)

    for file_path in file_paths:
        if not file_path.exists():
            continue

        try:
            relative = file_path.relative_to(settings.POSTPROCESSED_DIR)
        except ValueError:
            relative = Path(file_path.name)

        target = base_target / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, target)
        if logger:
            logger.info("[OneDrive] Copiado %s -> %s", file_path.name, target)


def sync_to_client_onedrive(
    organized_files: list[OrganizedFile],
    onedrive_path: str,
    year: int,
    week: int,
    logger: logging.Logger | None = None,
) -> None:
    """Copy organized files to OneDrive/Data Clientes/{onedrive_path}/{year}/S{week}_{year}/."""
    base = settings.ONEDRIVE_DATA_CLIENTES_BASE
    if base is None:
        if logger:
            logger.debug("[OneDrive] ONEDRIVE_DATA_CLIENTES_BASE no detectado, sync omitido.")
        return

    week_folder = f"S{week:02d}_{year}"
    target_dir = base / onedrive_path / str(year) / week_folder

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        if logger:
            logger.warning("[OneDrive] No se pudo crear carpeta %s: %s", target_dir, exc)
        return

    for item in organized_files:
        if not item.path.exists():
            continue
        target = target_dir / item.path.name
        if target.exists():
            if logger:
                logger.debug("[OneDrive] Ya existe, omitido: %s", target.name)
            continue
        try:
            shutil.copy2(item.path, target)
            if logger:
                logger.info(
                    "[OneDrive] %s -> .../%s/%s/%s/%s",
                    item.path.name, onedrive_path, year, week_folder, item.path.name,
                )
        except OSError as exc:
            if logger:
                logger.warning("[OneDrive] Error copiando %s: %s", item.path.name, exc)


def sync_downloads_to_hb(
    organized_files: list[OrganizedFile],
    client_folder: str,
    week: int,
    year: int,
    logger: logging.Logger | None = None,
) -> None:
    """Copy organized downloads to OneDrive/HB/{client}/{S{week}_{year}}/.

    The folder name inside HB uses the provider's 'carpeta' field (or display
    name) as-is, since those folders are pre-configured by the client.
    """
    root = settings.ONEDRIVE_HB_DIR
    if root is None:
        if logger:
            logger.debug("[OneDrive/HB] ONEDRIVE_HB_DIR no configurado, sincronizacion omitida.")
        return

    week_folder = f"S{week:02d}_{year}"
    target_dir = root / client_folder / week_folder

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        if logger:
            logger.warning("[OneDrive/HB] No se pudo crear carpeta %s: %s", target_dir, exc)
        return

    for item in organized_files:
        if not item.path.exists():
            continue
        target = target_dir / item.path.name
        if target.exists():
            if logger:
                logger.debug("[OneDrive/HB] Ya existe, omitido: %s", target.name)
            continue
        try:
            shutil.copy2(item.path, target)
            if logger:
                logger.info(
                    "[OneDrive/HB] %s -> HB/%s/%s/%s",
                    item.path.name,
                    client_folder,
                    week_folder,
                    item.path.name,
                )
        except OSError as exc:
            if logger:
                logger.warning("[OneDrive/HB] Error copiando %s: %s", item.path.name, exc)
