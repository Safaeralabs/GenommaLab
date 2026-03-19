"""Central project settings and packaging-aware paths."""

from __future__ import annotations

import os
import sys
from pathlib import Path


APP_NAME = "RPA Panel Cliente"
APP_SLUG = "rpa_panel_cliente"
APP_VERSION = "1.0.0"
WINDOW_SIZE = "980x720"
LOG_FILE_NAME = "rpa_panel.log"
EXCEL_SHEET_NAME = "proveedores"
EXCEL_FILE_NAME = "Accesob2b.xlsx"

PACKAGE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = PACKAGE_DIR.parent
RESOURCE_DIR = Path(getattr(sys, "_MEIPASS", PROJECT_DIR))
APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else PROJECT_DIR


def _resolve_runtime_root() -> Path:
    """Return a writable directory for logs, downloads and templates."""
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / APP_NAME
    return Path.home() / f".{APP_SLUG}"


RUNTIME_ROOT = _resolve_runtime_root()
DATA_DIR = RUNTIME_ROOT / "data"
DOWNLOADS_DIR = RUNTIME_ROOT / "downloads"
POSTPROCESSED_DIR = RUNTIME_ROOT / "postprocesado"
LOGS_DIR = RUNTIME_ROOT / "logs"
SCREENSHOTS_DIR = RUNTIME_ROOT / "screenshots"
PROVIDER_CATALOG_NAME = "providers.json"
PROVIDER_CATALOG_PATH = DATA_DIR / PROVIDER_CATALOG_NAME
PROVIDER_CATALOG_TEMPLATE = RESOURCE_DIR / "data" / PROVIDER_CATALOG_NAME
PROJECT_ROOT = PROJECT_DIR
BUNDLED_BROWSERS_DIR = RESOURCE_DIR / "ms-playwright"
TEMPLATE_HOMOLOGACION = PROJECT_ROOT / "Homologacion.xlsx"
HOMOLOGATION_TEMPLATE_NAME = "homologacion.xlsx"
HOMOLOGATION_TEMPLATE_PATH = DATA_DIR / HOMOLOGATION_TEMPLATE_NAME

PROVIDERS_SOURCE = os.getenv("PROVIDERS_SOURCE", "catalog").lower()


def _resolve_onedrive_root() -> Path | None:
    for key in ("ONEDRIVE", "OneDriveCommercial", "OneDriveConsumer"):
        value = os.getenv(key)
        if value:
            return Path(value)
    return None


_ONEDRIVE_ROOT = _resolve_onedrive_root()
ONEDRIVE_SYNC_DIR = _ONEDRIVE_ROOT / APP_NAME if _ONEDRIVE_ROOT else None

# Carpeta HB en OneDrive donde se depositan los archivos por cliente.
# Se puede sobreescribir con la variable de entorno ONEDRIVE_HB_PATH.
ONEDRIVE_HB_DIR: Path | None = (
    Path(os.getenv("ONEDRIVE_HB_PATH"))
    if os.getenv("ONEDRIVE_HB_PATH")
    else (_ONEDRIVE_ROOT / "HB" if _ONEDRIVE_ROOT else None)
)

# Ruta base en OneDrive para Megatiendas (EOS Consultores)
ONEDRIVE_BI_MEGATIENDAS_BASE: Path | None = (
    _ONEDRIVE_ROOT / "BI" / "Data Clientes" / "TT" / "Nuevo" / "1. B2B" / "Megatiendas"
    if _ONEDRIVE_ROOT else None
)

# Ruta base en OneDrive para Provecol (Soluciones Prácticas)
ONEDRIVE_BI_PROVECOL_BASE: Path | None = (
    _ONEDRIVE_ROOT / "BI" / "Data Clientes" / "TT" / "Nuevo" / "1. B2B" / "Provecol"
    if _ONEDRIVE_ROOT else None
)
