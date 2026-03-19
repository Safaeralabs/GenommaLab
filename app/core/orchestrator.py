"""Execution orchestration for the RPA workflow."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
import threading
from typing import Callable, Sequence

from app.config import settings
from app.core.download_organizer import DownloadOrganizer
from app.core.homologation_writer import HomologationWriter, HomologationRow
from app.core.models import ExecutionErrorDetail, ExecutionResult, ExecutionSummary, HomologationSummary, Proveedor
from app.portals.base_portal import BasePortal
from app.core.provider_loader import ProviderLoader
from app.portals.portal_a import PortalA
from app.portals.portal_b import PortalB
from app.portals.portal_eos import PortalEOS
from app.portals.portal_provecol import PortalProvecol
from app.portals.portal_xeon import PortalXeon
from app.utils.onedrive_sync import sync_downloads_to_hb, sync_paths_to_onedrive


StatusCallback = Callable[[str], None]
ProgressCallback = Callable[[int, int], None]
SummaryCallback = Callable[[ExecutionSummary], None]
ErrorsCallback = Callable[[list[ExecutionErrorDetail]], None]
HomologationCallback = Callable[[HomologationSummary | None], None]
ResultCallback = Callable[[ExecutionResult], None]


@dataclass(slots=True)
class UiCallbacks:
    """Callbacks used by the orchestrator to update the UI."""

    on_status: StatusCallback
    on_progress: ProgressCallback
    on_summary: SummaryCallback
    on_errors: ErrorsCallback
    on_last_homologation: HomologationCallback
    on_result: ResultCallback


class Orchestrator:
    """Coordinates the full provider execution cycle."""

    def __init__(self, logger: logging.Logger, callbacks: UiCallbacks, stop_event: threading.Event) -> None:
        self.logger = logger
        self.callbacks = callbacks
        self.download_organizer = DownloadOrganizer(logger)
        self.homologation_writer = HomologationWriter(logger)
        self.stop_event = stop_event
        self.homologation_rows: list[HomologationRow] = []
        self.portal_registry: dict[str, type[BasePortal]] = {
            "abako": PortalA,
            "portal_a": PortalA,
            "eos_consultores": PortalEOS,
            "soluciones_practicas": PortalProvecol,
            "xeon": PortalXeon,
            "portal_b": PortalB,
        }
        self.last_failed_providers: list[Proveedor] = []
        self.last_error_details: list[ExecutionErrorDetail] = []
        self.last_homologation_path: Path | None = None
        self.execution_dir: Path | None = None

    def run(
        self,
        excel_path: Path | None,
        year: int,
        week: int,
        portal_origins: Sequence[str] | None = None,
        providers_override: Sequence[Proveedor] | None = None,
        provider_source: str | None = None,
    ) -> ExecutionSummary:
        """Run all active providers from the given Excel."""
        started_at = datetime.now()
        exec_stamp = f"{started_at.strftime('%Y%m%d_%H%M%S')}_S{week:02d}_{year}"
        self.execution_dir = settings.POSTPROCESSED_DIR / exec_stamp
        self.execution_dir.mkdir(parents=True, exist_ok=True)
        self.homologation_rows.clear()
        self.last_failed_providers.clear()
        self.last_error_details.clear()
        self.last_homologation_path = None
        self.callbacks.on_status("Leyendo proveedores...")
        provider_source = provider_source or settings.PROVIDERS_SOURCE
        if providers_override is None:
            loader = ProviderLoader()
            load_path = excel_path if provider_source == "excel" else None
            providers = loader.load(provider_source, load_path)
            if portal_origins:
                providers = [
                    proveedor for proveedor in providers if proveedor.portal_origen in portal_origins
                ]
                if not providers:
                    self.logger.warning(
                        "Filtro de portales %s no devolvio proveedores activos.", portal_origins
                    )
            if provider_source == "catalog":
                self.logger.info("Usando catálogo de proveedores en lugar de Excel.")
        else:
            providers = list(providers_override)

        total = len(providers)
        if total == 0:
            summary = ExecutionSummary(
                total=0,
                success_count=0,
                failure_count=0,
                started_at=started_at,
                finished_at=datetime.now(),
            )
            self.logger.warning("No se encontraron proveedores activos para procesar.")
            self.callbacks.on_status("Sin proveedores activos.")
            self.callbacks.on_summary(summary)
            return summary

        success_count = 0
        failure_count = 0
        retry_flag = " (retry)" if providers_override else ""
        self.logger.info(
            "Se encontraron %s proveedores activos (Semana %s, %s)%s.",
            total,
            week,
            year,
            retry_flag,
        )

        start_date, end_date = self._week_to_iso_range(year, week)
        for index, proveedor in enumerate(providers, start=1):
            if self.stop_event.is_set():
                self.logger.warning("Ejecucion cancelada antes de procesar %s.", proveedor.display_name)
                break
            self.callbacks.on_status(f"Procesando {proveedor.display_name} ({index}/{total})")
            self.logger.info(
                "[%s/%s] Iniciando proveedor '%s' con portal '%s'.",
                index,
                total,
                proveedor.display_name,
                proveedor.portal_tipo,
            )

            if proveedor.requiere_revision:
                self.logger.warning(
                    "[%s] Registro marcado para revision. Conflictos: %s",
                    proveedor.display_name,
                    proveedor.conflictos_detectados or "Sin detalle informado.",
                )

            execution_proveedor = replace(
                proveedor,
                fecha_desde=start_date,
                fecha_hasta=end_date,
            )
            result: ExecutionResult | None = None

            try:
                result = self._run_provider(execution_proveedor)
                if result.success:
                    result = self._run_postprocess(execution_proveedor, result, year, week)
                if result.success:
                    success_count += 1
                    self.logger.info("[%s] OK - %s", proveedor.display_name, result.message)
                else:
                    failure_count += 1
                    self.logger.error("[%s] ERROR - %s", proveedor.display_name, result.message)
                    if result.screenshot_path is not None:
                        self.logger.error(
                            "[%s] Screenshot guardado en %s",
                            proveedor.display_name,
                            result.screenshot_path,
                        )
                    self.last_failed_providers.append(execution_proveedor)
                    self.last_error_details.append(
                        ExecutionErrorDetail(
                            proveedor=proveedor.display_name,
                            message=result.message,
                            screenshot=result.screenshot_path,
                            proveedor_obj=execution_proveedor,
                        )
                    )
                    self._publish_errors()
            except Exception as exc:
                failure_count += 1
                self.logger.exception(
                    "[%s] Error no controlado durante la ejecucion: %s",
                    proveedor.display_name,
                    exc,
                )
                self.last_failed_providers.append(execution_proveedor)
                self.last_error_details.append(
                    ExecutionErrorDetail(
                        proveedor=execution_proveedor.display_name,
                        message=str(exc),
                        screenshot=None,
                        proveedor_obj=execution_proveedor,
                    )
                )
                self._publish_errors()
                result = ExecutionResult(
                    proveedor=execution_proveedor.display_name,
                    portal_tipo=execution_proveedor.portal_tipo,
                    success=False,
                    message=str(exc),
                )
            finally:
                if result is not None:
                    self.callbacks.on_result(result)
                self.callbacks.on_progress(index, total)

        summary = ExecutionSummary(
            total=total,
            success_count=success_count,
            failure_count=failure_count,
            started_at=started_at,
            finished_at=datetime.now(),
        )
        self.callbacks.on_status(
            f"Finalizado. OK: {success_count} | Fallidos: {failure_count}"
        )
        self.callbacks.on_summary(summary)
        self.logger.info(
            "Resumen final -> Total: %s | OK: %s | Fallidos: %s",
            total,
            success_count,
            failure_count,
        )
        if self.homologation_rows:
            failed_names = [p.display_name for p in self.last_failed_providers]
            hom_summary = self.homologation_writer.write(
                self.homologation_rows,
                year,
                week,
                start_date,
                total_providers=total,
                missing_providers=failed_names,
            )
            self.last_homologation_path = hom_summary.path
            self.logger.info(
                "Homologacion S%s/%s: %s/%s proveedores incluidos.",
                week, year, hom_summary.included_providers, hom_summary.total_providers,
            )
            sync_paths_to_onedrive([hom_summary.path], "Homologaciones", self.logger)
            self.callbacks.on_errors(self.last_error_details)
            self.callbacks.on_last_homologation(hom_summary)
        else:
            self.callbacks.on_errors(self.last_error_details)
            self.callbacks.on_last_homologation(None)
        return summary

    def _publish_errors(self) -> None:
        if self.last_error_details:
            self.callbacks.on_errors(list(self.last_error_details))

    def _run_provider(
        self,
        proveedor: Proveedor,
    ) -> ExecutionResult:
        portal_class = self.portal_registry.get(proveedor.portal_tipo)
        if portal_class is None:
            raise ValueError(
                f"portal_tipo '{proveedor.portal_tipo}' no soportado para {proveedor.display_name}."
            )

        download_dir = proveedor.target_download_dir(settings.DOWNLOADS_DIR)
        portal = portal_class(
            proveedor=proveedor,
            download_dir=download_dir,
            screenshot_dir=settings.SCREENSHOTS_DIR,
            logger=self.logger,
        )
        return portal.ejecutar()

    def _run_postprocess(
        self,
        proveedor: Proveedor,
        result: ExecutionResult,
        year: int,
        week: int,
    ) -> ExecutionResult:
        downloaded_files = result.downloaded_files[:]
        if not downloaded_files and result.downloaded_file is not None:
            downloaded_files = [result.downloaded_file]
        if not downloaded_files:
            self.logger.warning(
                "[%s] El portal no devolvio archivos para postprocesar.",
                proveedor.display_name,
            )
            return result

        self.logger.info("[%s] Iniciando postprocesado de descargas.", proveedor.display_name)

        try:
            organized_files = self.download_organizer.organize(
                proveedor, downloaded_files, execution_dir=self.execution_dir
            )
        except Exception as exc:
            self.logger.exception(
                "[%s] Error durante el postprocesado: %s",
                proveedor.display_name,
                exc,
            )
            result.success = False
            result.message = f"{result.message} | Postprocesado fallido: {exc}"
            return result

        result.organized_files = organized_files
        if not result.portal_handled_sync:
            sync_downloads_to_hb(
                organized_files,
                proveedor.proveedor,
                year,
                week,
                self.logger,
            )
        try:
            rows = self.homologation_writer.collect_rows(proveedor, organized_files)
            self.homologation_rows.extend(rows)
            if rows:
                result.message = f"{result.message} | Homologacion filas: {len(rows)}"
        except Exception as exc:
            self.logger.exception(
                "[%s] Error en homologacion: %s",
                proveedor.display_name,
                exc,
            )
            result.success = False
            result.message = f"{result.message} | Homologacion fallida: {exc}"

        if organized_files:
            result.message = (
                f"{result.message} | Postprocesados: {len(organized_files)} archivo(s)"
            )
        return result

    @staticmethod
    def _week_to_iso_range(year: int, week: int) -> tuple[str, str]:
        start = datetime.fromisocalendar(year, week, 1)
        end = start + timedelta(days=6)
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
