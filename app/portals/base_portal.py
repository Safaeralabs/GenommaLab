"""Base abstraction for provider portals."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from app.core.models import ExecutionResult, Proveedor


class BasePortal(ABC):
    """Abstract portal contract."""

    def __init__(
        self,
        proveedor: Proveedor,
        download_dir: Path,
        screenshot_dir: Path,
    ) -> None:
        self.proveedor = proveedor
        self.download_dir = download_dir
        self.screenshot_dir = screenshot_dir

    @abstractmethod
    def ejecutar(self) -> ExecutionResult:
        """Execute the portal workflow."""
