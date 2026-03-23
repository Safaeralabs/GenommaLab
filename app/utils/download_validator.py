"""Validate downloaded files before postprocessing."""
from __future__ import annotations
from pathlib import Path

# Magic bytes
_XLSX_MAGIC = b"PK\x03\x04"
_XLS_MAGIC  = b"\xd0\xcf\x11\xe0"
_HTML_SIGS  = (b"<!doctype", b"<html", b"<?xml")
MIN_SIZE_BYTES = 512


def validate_download(path: Path) -> tuple[bool, str]:
    """Return (ok, reason). reason is empty string when ok=True."""
    if not path.exists():
        return False, "Archivo no encontrado tras la descarga."
    size = path.stat().st_size
    if size < MIN_SIZE_BYTES:
        return False, f"Archivo demasiado pequeño ({size} bytes); posible descarga vacía."
    try:
        header = path.read_bytes()[:8]
    except OSError as exc:
        return False, f"No se pudo leer el archivo: {exc}"

    suffix = path.suffix.lower()
    if suffix == ".xlsx" and not header.startswith(_XLSX_MAGIC):
        # Could still be HTML disguised as xlsx
        try:
            snippet = path.read_bytes()[:256].lower()
            if any(sig in snippet for sig in _HTML_SIGS):
                return False, "El archivo descargado es una página HTML, no un Excel."
        except OSError:
            pass
    if suffix == ".xls" and not header.startswith(_XLS_MAGIC):
        try:
            snippet = path.read_bytes()[:256].lower()
            if any(sig in snippet for sig in _HTML_SIGS):
                return False, "El archivo descargado es una página HTML, no un Excel."
        except OSError:
            pass
    if suffix == ".csv":
        try:
            snippet = path.read_bytes()[:256].lower()
            if any(sig in snippet for sig in _HTML_SIGS):
                return False, "El archivo descargado es una página HTML, no un CSV."
        except OSError:
            pass
    return True, ""
