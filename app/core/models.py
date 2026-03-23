"""Domain models used by the application."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(slots=True)
class Proveedor:
    """Represents one provider row from Excel."""

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
    requiere_revision: bool = False
    notas_operativas: str = ""
    conflictos_detectados: str = ""
    fuente: str = ""
    tipo_acceso: str = ""
    portal_origen: str = ""

    @property
    def display_name(self) -> str:
        """Return a user-facing name including the subportal when present."""
        if self.sede_subportal:
            return f"{self.proveedor} - {self.sede_subportal}"
        return self.proveedor

    def target_download_dir(self, downloads_root: Path) -> Path:
        """Return the provider-specific download directory."""
        folder_name = self.carpeta.strip() or self.display_name.strip() or "sin_nombre"
        safe_folder = folder_name.replace("/", "_").replace("\\", "_")
        return downloads_root / safe_folder


@dataclass(slots=True)
class ExecutionResult:
    """Outcome of a provider execution."""

    proveedor: str
    portal_tipo: str
    success: bool
    message: str
    error_type: str = ""
    needs_retry: bool = False          # True si la descarga fue parcial y vale la pena reintentar
    screenshot_path: Path | None = None
    downloaded_file: Path | None = None
    downloaded_files: list[Path] = field(default_factory=list)
    organized_files: list["OrganizedFile"] = field(default_factory=list)
    portal_handled_sync: bool = False


@dataclass(slots=True)
class ExecutionErrorDetail:
    """Captures provider failures for UI summary."""

    proveedor: str
    message: str
    screenshot: Path | None
    proveedor_obj: Proveedor


@dataclass(slots=True)
class ExecutionSummary:
    """Final execution summary."""

    total: int
    success_count: int
    failure_count: int
    started_at: datetime
    finished_at: datetime


@dataclass(slots=True)
class OrganizedFile:
    """Metadata for each file produced during postprocesado."""

    path: Path
    category: str


@dataclass
class HomologationSummary:
    """Summary of a homologation file write operation."""

    path: Path
    included_providers: int   # unique providers that have data in the file
    total_providers: int       # total providers that ran this execution
    missing_providers: list[str]  # display names of providers with no rows
