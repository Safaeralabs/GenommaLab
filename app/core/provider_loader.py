"""Provider loader that switches between Excel and a JSON catalog."""

from __future__ import annotations

import json
import logging
from pathlib import Path
import unicodedata
from typing import Sequence

from app.config import settings
from app.config.provider_catalog import DEFAULT_PROVIDERS
from app.core.excel_reader import ExcelReader
from app.core.models import Proveedor

LOGGER = logging.getLogger(__name__)


class ProviderLoader:
    """Load providers from Excel or from a structured catalog."""

    def load(self, source: str | None = None, excel_path: Path | None = None) -> list[Proveedor]:
        normalized = (source or settings.PROVIDERS_SOURCE).strip().lower()
        if normalized == "catalog":
            return self._load_from_catalog()

        if normalized != "excel":
            raise ValueError(f"Fuente de proveedores desconocida: {source}")

        if not excel_path:
            raise FileNotFoundError("No se proporcionó ruta de Excel para cargar proveedores.")

        return ExcelReader(excel_path).read_proveedores()

    def _load_from_catalog(self) -> list[Proveedor]:
        entries = self._catalog_entries()
        providers: list[Proveedor] = []
        for entry in entries:
            normalized = {
                self._normalize_key(key): value for key, value in entry.items() if key is not None
            }
            activo = ExcelReader._as_bool(
                self._find_value(normalized, ("activo", "activo_rpa", "estado")) or True
            )
            if not activo:
                continue
            portal_tipo = ExcelReader._normalize_portal_type(
                self._find_value(normalized, ("portal_tipo", "portal")) or ""
            )
            portal_origen = (
                self._find_value(normalized, ("portal_origen", "portal_tipo", "portal"))
                or portal_tipo
            )
            provider = Proveedor(
                proveedor=ExcelReader._as_text(self._find_value(normalized, ("proveedor", "cliente"))),
                activo=True,
                portal_tipo=portal_tipo,
                portal_origen=portal_origen,
                login_url=ExcelReader._as_text(self._find_value(normalized, ("url_principal", "login_url", "url"))),
                url_alternativa=ExcelReader._as_text(self._find_value(normalized, ("url_alternativa", "url_alt", "alternativa"))),
                usuario=ExcelReader._as_text(self._find_value(normalized, ("usuario", "user"))),
                password=ExcelReader._as_text(self._find_value(normalized, ("password", "pass"))),
                fecha_desde=ExcelReader._as_text(self._find_value(normalized, ("fecha_desde", "fecha"))),
                fecha_hasta=ExcelReader._as_text(self._find_value(normalized, ("fecha_hasta", "hasta"))),
                carpeta=ExcelReader._as_text(self._find_value(normalized, ("carpeta", "folder"))),
                onedrive_path=ExcelReader._as_text(self._find_value(normalized, ("onedrive_carpeta", "onedrive_path"))),
                sede_subportal=ExcelReader._as_text(self._find_value(normalized, ("sede_subportal", "sede"))),
                requiere_revision=ExcelReader._as_bool(self._find_value(normalized, ("requiere_revision", "requiere_rev", "revision"))),
                notas_operativas=ExcelReader._as_text(self._find_value(normalized, ("notas_operativas", "notas"))),
                conflictos_detectados=ExcelReader._as_text(self._find_value(normalized, ("conflictos_detectados", "conflictos"))),
                fuente=ExcelReader._as_text(self._find_value(normalized, ("fuente", "origen"))),
                tipo_acceso=ExcelReader._as_text(self._find_value(normalized, ("tipo_acceso", "acceso"))),
            )
            if provider.proveedor:
                providers.append(provider)

        if not providers:
            LOGGER.warning("No se encontraron proveedores activos en el catálogo.")
        return providers

    def _catalog_entries(self) -> Sequence[dict[str, object]]:
        path = settings.PROVIDER_CATALOG_PATH
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                LOGGER.exception("Error leyendo el catálogo de proveedores JSON: %s", exc)
            except UnicodeDecodeError:
                LOGGER.warning("Reintentando lectura del catálogo con latin-1.")
                return json.loads(path.read_text(encoding="latin-1"))

        LOGGER.debug("Usando catálogo por defecto embebido.")
        return DEFAULT_PROVIDERS

    @staticmethod
    def _normalize_key(raw: str) -> str:
        text = str(raw).strip().lower()
        normalized = unicodedata.normalize("NFKD", text)
        return "".join(ch for ch in normalized if ch.isalnum())

    def _find_value(self, normalized: dict[str, object], candidates: tuple[str, ...]) -> object | None:
        for candidate in candidates:
            key = self._normalize_key(candidate)
            if key in normalized:
                return normalized[key]
        return None
