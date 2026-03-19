"""Playwright implementation for Soluciones Prácticas (Provecol) downloads."""

from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from app.config import settings
from app.core.models import ExecutionResult, Proveedor
from app.portals.base_portal import BasePortal


# IDs de reporte en la plataforma
_REP_VENTAS = 79
_REP_INVENTARIO = 28


class PortalProvecol(BasePortal):
    """Soluciones Prácticas workflow: login, ventas por fecha, inventario por mes."""

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
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        error_screenshot_path = self._build_screenshot_path("error")

        start_date, end_date = self.proveedor.fecha_desde, self.proveedor.fecha_hasta
        week, year, month = self._resolve_week_year_month()

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self._headless_mode())
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            try:
                self._login(page)
                self.logger.info("[%s] Login completado.", self.proveedor.display_name)

                self._open_reportes(page)
                ventas_path = self._download_ventas(page, start_date, end_date)
                inventario_path = self._download_inventario(page, year, month)

            except (PlaywrightTimeoutError, PlaywrightError, Exception) as exc:
                self._take_screenshot(page, error_screenshot_path)
                self.logger.exception(
                    "[%s] Error durante descarga Provecol: %s",
                    self.proveedor.display_name, exc,
                )
                return ExecutionResult(
                    proveedor=self.proveedor.display_name,
                    portal_tipo=self.proveedor.portal_tipo,
                    success=False,
                    message=f"Error Provecol: {exc}",
                    screenshot_path=error_screenshot_path if error_screenshot_path.exists() else None,
                )
            finally:
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass

        # Renombrar a nombres estándar (preservar extensión original .xls/.xlsx)
        today = datetime.now().strftime("%d%m%Y")
        ventas_final = self._rename_file(ventas_path, f"Provecol_Ventas_{today}{ventas_path.suffix}")
        inventario_final = self._rename_file(inventario_path, f"Provecol_Inventario_{today}{inventario_path.suffix}")

        # Sincronizar ambos a OneDrive BI
        self._sync_to_bi_onedrive(ventas_final, week, year)
        self._sync_to_bi_onedrive(inventario_final, week, year)

        self.logger.info(
            "[%s] Descarga completada: %s | %s",
            self.proveedor.display_name, ventas_final.name, inventario_final.name,
        )
        return ExecutionResult(
            proveedor=self.proveedor.display_name,
            portal_tipo=self.proveedor.portal_tipo,
            success=True,
            message=f"Descargados: {ventas_final.name} | {inventario_final.name}",
            downloaded_file=ventas_final,
            downloaded_files=[ventas_final, inventario_final],
            portal_handled_sync=True,
        )

    # ── Login ─────────────────────────────────────────────────────────────────

    def _login(self, page: Page) -> None:
        login_url = self.proveedor.login_url.strip()
        self.logger.info("[%s] Abriendo %s", self.proveedor.display_name, login_url)
        page.goto(login_url, wait_until="domcontentloaded", timeout=60000)

        # Seleccionar empresa en el dropdown (búsqueda case-insensitive)
        empresa = self.proveedor.carpeta or self.proveedor.proveedor
        self.logger.info("[%s] Seleccionando empresa: %s", self.proveedor.display_name, empresa)
        matched = page.evaluate(f"""() => {{
            const sel = document.querySelector('select');
            if (!sel) return false;
            for (const opt of sel.options) {{
                if (opt.text.trim().toLowerCase() === '{empresa.lower()}') {{
                    sel.value = opt.value;
                    sel.dispatchEvent(new Event('change'));
                    return true;
                }}
            }}
            return false;
        }}""")
        if not matched:
            self.logger.warning(
                "[%s] No se encontró empresa '%s' en el dropdown, usando primera opción.",
                self.proveedor.display_name, empresa,
            )
        page.wait_for_timeout(500)

        # Usuario y contraseña
        page.locator("input[type='text'], input:not([type='password']):not([type='hidden']):not([type='submit'])").first.fill(
            self.proveedor.usuario
        )
        page.locator("input[type='password']").first.fill(self.proveedor.password)
        page.wait_for_timeout(300)

        page.get_by_text("Iniciar Sesion", exact=False).click()
        page.wait_for_url("**/menu.aspx**", timeout=30000)

    # ── Descargas ─────────────────────────────────────────────────────────────

    def _open_reportes(self, page: Page) -> None:
        """Navega a la grilla de reportes desde donde sea que esté la página."""
        # Si ya estamos en la grilla de reportes, no hacer nada
        if page.locator(f"input[type='image'][onclick*='rep={_REP_VENTAS}']").count():
            return
        # Desde el menú principal: #btt5
        if page.locator("#btt5").is_visible():
            page.locator("#btt5").click()
        else:
            # Desde una página de criterios: #btt10 vuelve a la grilla
            page.locator("#btt10").click()
        page.locator(f"input[type='image'][onclick*='rep={_REP_VENTAS}']").wait_for(
            state="visible", timeout=15000
        )
        self.logger.info("[%s] Menú de reportes cargado.", self.proveedor.display_name)

    def _download_ventas(self, page: Page, fecha_desde: str, fecha_hasta: str) -> Path:
        """Descarga el reporte de ventas para el rango de fechas dado."""
        self.logger.info(
            "[%s] Descargando ventas: %s → %s",
            self.proveedor.display_name, fecha_desde, fecha_hasta,
        )
        # Hacer click en el botón de ventas por fecha desde la grilla
        page.locator(f"input[type='image'][onclick*='rep={_REP_VENTAS}']").click()
        page.wait_for_url(f"**/reportes.aspx?rep={_REP_VENTAS}**", timeout=15000)

        # Fechas en formato dd-mm-yyyy
        fecha_inf = self._to_portal_date(fecha_desde)
        fecha_sup = self._to_portal_date(fecha_hasta)

        self._fill_criteria(page, fecha_inf, fecha_sup)
        return self._click_exporta_excel(page, "ventas")

    def _download_inventario(self, page: Page, year: int, month: int) -> Path:
        """Descarga el reporte de inventario para el año/mes dado."""
        self.logger.info(
            "[%s] Descargando inventario: %d/%02d",
            self.proveedor.display_name, year, month,
        )
        # Volver al menú de reportes y hacer click en inventarios
        self._open_reportes(page)
        page.locator(f"input[type='image'][onclick*='rep={_REP_INVENTARIO}']").click()
        page.wait_for_url(f"**/reportes.aspx?rep={_REP_INVENTARIO}**", timeout=15000)

        self._fill_criteria(page, str(year), f"{month:02d}")
        return self._click_exporta_excel(page, "inventario")

    # ── Helpers de formulario ─────────────────────────────────────────────────

    def _fill_criteria(self, page: Page, value1: str, value2: str) -> None:
        """Rellena TextBox_1 y TextBox_2 del formulario de criterios."""
        tb1 = page.locator("#TextBox_1")
        tb1.wait_for(state="visible", timeout=10000)
        tb1.fill(value1)
        tb2 = page.locator("#TextBox_2")
        tb2.wait_for(state="visible", timeout=10000)
        tb2.fill(value2)

    def _click_exporta_excel(self, page: Page, tipo: str) -> Path:
        """Hace clic en el botón 'Exporta A Excel' (#btt_exp) y guarda la descarga."""
        self.logger.info("[%s] Exportando %s a Excel.", self.proveedor.display_name, tipo)
        btn = page.locator("#btt_exp")
        btn.wait_for(state="visible", timeout=10000)

        with page.expect_download(timeout=120000) as dl_info:
            btn.click()

        download = dl_info.value
        dest = self.download_dir / (download.suggested_filename or f"provecol_{tipo}.xlsx")
        download.save_as(dest)
        self.logger.info("[%s] Archivo guardado: %s", self.proveedor.display_name, dest)
        return dest

    # ── Post-proceso ──────────────────────────────────────────────────────────

    def _rename_file(self, src: Path, new_name: str) -> Path:
        dest = src.parent / new_name
        shutil.move(str(src), str(dest))
        return dest

    def _sync_to_bi_onedrive(self, file_path: Path, week: int, year: int) -> None:
        """Copia el archivo a OneDrive/BI/Data Clientes/TT/Nuevo/1. B2B/Provecol/{year}/S{week:02d}/"""
        base = settings.ONEDRIVE_BI_PROVECOL_BASE
        if base is None:
            self.logger.debug(
                "[%s] ONEDRIVE_BI_PROVECOL_BASE no configurado, sync omitido.",
                self.proveedor.display_name,
            )
            return

        target_dir = base / str(year) / f"S{week:02d}"
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            self.logger.warning(
                "[%s] No se pudo crear carpeta OneDrive BI Provecol: %s",
                self.proveedor.display_name, exc,
            )
            return

        try:
            shutil.copy2(file_path, target_dir / file_path.name)
            self.logger.info(
                "[%s] [OneDrive/BI] %s -> .../Provecol/%s/S%02d/%s",
                self.proveedor.display_name, file_path.name, year, week, file_path.name,
            )
        except OSError as exc:
            self.logger.warning(
                "[%s] Error copiando a OneDrive BI Provecol: %s",
                self.proveedor.display_name, exc,
            )

    # ── Utilidades ────────────────────────────────────────────────────────────

    def _base_url(self) -> str:
        """Retorna la URL base para construir rutas de reportes (ej: .../next)."""
        return self.proveedor.login_url.rstrip("/")

    def _resolve_week_year_month(self) -> tuple[int, int, int]:
        from datetime import date
        try:
            d = date.fromisoformat(self.proveedor.fecha_desde)
        except (ValueError, TypeError):
            d = date.today()
        iso = d.isocalendar()
        return iso[1], iso[0], d.month

    @staticmethod
    def _to_portal_date(iso_date: str) -> str:
        """Convierte 'yyyy-mm-dd' a 'dd-mm-yyyy' (formato del portal)."""
        try:
            d = datetime.strptime(iso_date, "%Y-%m-%d")
            return d.strftime("%d-%m-%Y")
        except (ValueError, TypeError):
            return iso_date

    @staticmethod
    def _headless_mode() -> bool:
        raw_value = os.getenv("RPA_HEADLESS", "0").strip().lower()
        return raw_value in {"1", "true", "yes", "si"}

    def _build_screenshot_path(self, suffix: str) -> Path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = self.proveedor.display_name.replace(" ", "_").replace("/", "_")
        return self.screenshot_dir / f"{safe_name}_{suffix}_{ts}.png"

    def _take_screenshot(self, page: Page, path: Path) -> None:
        try:
            page.screenshot(path=str(path), full_page=False)
        except Exception:
            pass
