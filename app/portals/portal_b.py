"""Placeholder implementation for a second provider portal."""

from __future__ import annotations

import logging
from pathlib import Path

from app.core.models import ExecutionResult, Proveedor
from app.portals.base_portal import BasePortal


class PortalB(BasePortal):
    """Portal scaffold pending real business rules."""

    def __init__(
        self,
        proveedor: Proveedor,
        download_dir: Path,
        screenshot_dir: Path,
        logger: logging.Logger,
    ) -> None:
        super().__init__(proveedor, download_dir, screenshot_dir)
        self.logger = logger

    def ejecutar(self) -> ExecutionResult:
        """Return a controlled placeholder result until implemented."""
        self.logger.warning(
            "[%s] PortalB aún no está implementado. Se omite ejecución real.",
            self.proveedor.proveedor,
        )
        return ExecutionResult(
            proveedor=self.proveedor.proveedor,
            portal_tipo=self.proveedor.portal_tipo,
            success=False,
            message="PortalB pendiente de implementación.",
        )
