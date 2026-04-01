from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(slots=True)
class Proveedor:
    proveedor: str
    activo: bool
    portal_tipo: str
    login_url: str
    usuario: str
    password: str
    fecha_desde: str
    fecha_hasta: str
    carpeta: str
    onedrive_path: str = ""
    url_alternativa: str = ""
    sede_subportal: str = ""
    cadena: str = ""
    requiere_revision: bool = False
    notas_operativas: str = ""
    conflictos_detectados: str = ""
    fuente: str = ""
    tipo_acceso: str = ""
    portal_origen: str = ""

    @property
    def display_name(self) -> str:
        if self.sede_subportal:
            return f"{self.proveedor} - {self.sede_subportal}"
        return self.proveedor

    def target_download_dir(self, downloads_root: Path) -> Path:
        folder_name = self.carpeta.strip() or self.display_name.strip() or "sin_nombre"
        safe_folder = folder_name.replace("/", "_").replace("\\", "_")
        return downloads_root / safe_folder


@dataclass(slots=True)
class ExecutionResult:
    proveedor: str
    portal_tipo: str
    success: bool
    message: str
    error_type: str = ""
    needs_retry: bool = False
    screenshot_path: Path | None = None
    downloaded_file: Path | None = None
    downloaded_files: list[Path] = field(default_factory=list)
    organized_files: list["OrganizedFile"] = field(default_factory=list)
    portal_handled_sync: bool = False


@dataclass(slots=True)
class ExecutionErrorDetail:
    proveedor: str
    message: str
    screenshot: Path | None
    proveedor_obj: Proveedor


@dataclass(slots=True)
class ExecutionSummary:
    total: int
    success_count: int
    failure_count: int
    started_at: datetime
    finished_at: datetime


@dataclass(slots=True)
class OrganizedFile:
    path: Path
    category: str


@dataclass(slots=True)
class ProviderRunDetail:
    """Resultado de ejecución de un proveedor, para el Resumen de homologación."""
    display_name: str
    cadena: str
    success: bool
    message: str
    portal_tipo: str = ""


@dataclass
class HomologationSummary:
    path: Path
    included_providers: int
    total_providers: int
    missing_providers: list[str]
