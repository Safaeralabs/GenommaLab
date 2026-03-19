"""Playwright implementation for EOS Consultores / Megatiendas sales downloads."""

from __future__ import annotations

import csv
import logging
import os
import shutil
from datetime import date, datetime
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from app.config import settings
from app.core.models import ExecutionResult, Proveedor
from app.portals.base_portal import BasePortal


MONTH_NAMES_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}

PROVIDER_FILTER = "genom"


class PortalEOS(BasePortal):
    """EOS Consultores workflow: login, filter, download monthly CSV for Megatiendas."""

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

        week, year, month = self._resolve_week_year_month()

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self._headless_mode(), channel=settings.BROWSER_CHANNEL)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            try:
                login_url = self.proveedor.login_url.strip()
                self.logger.info("[%s] Abriendo %s", self.proveedor.display_name, login_url)
                page.goto(login_url, wait_until="domcontentloaded", timeout=60000)

                self._login(page)
                self.logger.info("[%s] Login completado.", self.proveedor.display_name)

                page.goto(
                    "https://eos-consultores.com/index.php/reportes/",
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                self._open_descargas_ventas(page)
                self._apply_filters(page, month, year)
                raw_csv = self._download_csv(page)

            except (PlaywrightTimeoutError, PlaywrightError, Exception) as exc:
                self._take_screenshot(page, error_screenshot_path)
                self.logger.exception("[%s] Error durante descarga EOS: %s", self.proveedor.display_name, exc)
                return ExecutionResult(
                    proveedor=self.proveedor.display_name,
                    portal_tipo=self.proveedor.portal_tipo,
                    success=False,
                    message=f"Error EOS Consultores: {exc}",
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

        # 1. Renombrar y llevar el archivo virgen a OneDrive
        today = datetime.now()
        renamed = self._rename_file(raw_csv, today)
        self._sync_to_bi_onedrive(renamed, week, year)

        # 2. Calcular delta vs semana anterior → solo filas nuevas para homologación
        delta_csv = self._compute_week_delta(renamed, week, year)
        hom_file = delta_csv if delta_csv is not None else renamed

        self.logger.info("[%s] Descarga completada: %s", self.proveedor.display_name, renamed.name)
        return ExecutionResult(
            proveedor=self.proveedor.display_name,
            portal_tipo=self.proveedor.portal_tipo,
            success=True,
            message=f"Descargado: {renamed.name}",
            downloaded_file=hom_file,
            downloaded_files=[hom_file],
            portal_handled_sync=True,
        )

    # ── Helpers de navegación ─────────────────────────────────────────────────

    def _login(self, page: Page) -> None:
        usuario_field = page.locator("#user_login")
        usuario_field.wait_for(state="visible", timeout=15000)
        usuario_field.click()
        page.wait_for_timeout(500)
        usuario_field.fill(self.proveedor.usuario)

        password_field = page.locator("#user_pass")
        password_field.wait_for(state="visible", timeout=10000)
        password_field.click()
        page.wait_for_timeout(500)
        password_field.fill(self.proveedor.password)

        page.wait_for_timeout(500)
        page.click("#wp-submit")
        page.wait_for_url("**/reportes/**", timeout=30000)

    def _open_descargas_ventas(self, page: Page) -> None:
        self.logger.info("[%s] Abriendo DESCARGAS VENTAS.", self.proveedor.display_name)
        tab = page.get_by_text("DESCARGAS VENTAS", exact=True)
        tab.wait_for(state="visible", timeout=15000)
        tab.click()
        # La tabla puede tardar varios minutos en cargar (datos pesados).
        # Esperamos hasta que aparezca "Showing X to Y of Z entries" o el boton CSV.
        self.logger.info("[%s] Esperando carga de tabla DESCARGAS VENTAS...", self.proveedor.display_name)
        page.locator(".dataTables_info, .dataTables_wrapper, button:has-text('CSV')").first.wait_for(
            state="visible", timeout=300000  # hasta 5 minutos
        )
        self.logger.info("[%s] Tabla cargada.", self.proveedor.display_name)

    def _apply_filters(self, page: Page, month: int, year: int) -> None:
        """Apply proveedor, mes and año filters using tfoot input placeholders."""
        self.logger.info(
            "[%s] Aplicando filtros: proveedor=%s mes=%s anio=%s",
            self.proveedor.display_name, PROVIDER_FILTER, month, year,
        )
        # Los filtros son inputs en <tfoot> con placeholders exactos: MES, ANIO, PROVEEDOR
        self._fill_tfoot_filter(page, "MES", str(month))
        self._fill_tfoot_filter(page, "ANIO", str(year))
        self._fill_tfoot_filter(page, "PROVEEDOR", PROVIDER_FILTER)

        # Esperar a que la tabla se refresque con los filtros aplicados
        page.wait_for_timeout(5000)
        self.logger.info("[%s] Filtros aplicados.", self.proveedor.display_name)

    def _fill_tfoot_filter(self, page: Page, placeholder: str, value: str) -> None:
        """Fill a tfoot filter input by its exact placeholder attribute."""
        inp = page.locator(f"tfoot input[placeholder='{placeholder}']")
        inp.wait_for(state="visible", timeout=10000)
        inp.click()
        inp.fill(value)
        inp.press("Enter")
        page.wait_for_timeout(500)

    def _download_csv(self, page: Page) -> Path:
        """Click the CSV button and save the download."""
        self.logger.info("[%s] Descargando CSV.", self.proveedor.display_name)
        csv_button = page.get_by_role("button", name="CSV")
        if not csv_button.count():
            csv_button = page.get_by_text("CSV", exact=True).first

        with page.expect_download(timeout=60000) as dl_info:
            csv_button.click()

        download = dl_info.value
        dest = self.download_dir / (download.suggested_filename or "eos_ventas.csv")
        download.save_as(dest)
        self.logger.info("[%s] CSV guardado en: %s", self.proveedor.display_name, dest)
        return dest

    # ── Post-proceso ──────────────────────────────────────────────────────────

    def _rename_file(self, csv_path: Path, download_date: datetime) -> Path:
        """Rename to Megatiendas_DDMMYYYY.csv"""
        new_name = f"Megatiendas_{download_date.strftime('%d%m%Y')}.csv"
        new_path = csv_path.parent / new_name
        shutil.move(str(csv_path), str(new_path))
        return new_path


    def _compute_week_delta(self, current_csv: Path, week: int, year: int) -> Path | None:
        """Devuelve un CSV con solo las filas nuevas respecto a la semana anterior.

        Busca el archivo de S{week-1} en la misma ruta de OneDrive. Si no existe
        (primera semana del mes o primer uso), retorna None (se usa el mes completo).
        """
        base = settings.ONEDRIVE_BI_MEGATIENDAS_BASE
        if base is None:
            return None

        # Calcular semana anterior
        prev_week = week - 1
        prev_year = year
        if prev_week == 0:
            prev_year -= 1
            prev_week = date(prev_year, 12, 28).isocalendar()[1]  # última semana del año anterior

        prev_dir = base / str(prev_year) / f"S{prev_week:02d}"
        prev_files = sorted(prev_dir.glob("Megatiendas_*.csv")) if prev_dir.exists() else []

        if not prev_files:
            self.logger.info(
                "[%s] No hay archivo previo en S%02d — se usará el mes completo para homologación.",
                self.proveedor.display_name, prev_week,
            )
            return None

        prev_csv = prev_files[-1]  # el más reciente si hubiera varios
        self.logger.info(
            "[%s] Comparando con semana anterior: %s", self.proveedor.display_name, prev_csv.name
        )

        current_rows = self._read_csv_rows(current_csv)
        prev_rows = self._read_csv_rows(prev_csv)

        if not current_rows:
            return None

        header = current_rows[0]
        prev_set = {tuple(row) for row in prev_rows[1:]}
        delta_rows = [row for row in current_rows[1:] if tuple(row) not in prev_set]

        self.logger.info(
            "[%s] Delta S%02d: %d filas nuevas (total mes: %d | semana anterior: %d).",
            self.proveedor.display_name, week,
            len(delta_rows), len(current_rows) - 1, len(prev_rows) - 1,
        )

        if not delta_rows:
            self.logger.warning(
                "[%s] Sin filas nuevas respecto a S%02d. Se enviará mes completo a homologación.",
                self.proveedor.display_name, prev_week,
            )
            return None

        delta_path = current_csv.parent / f"Megatiendas_delta_S{week:02d}.csv"
        with open(delta_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(delta_rows)

        return delta_path

    def _read_csv_rows(self, csv_path: Path) -> list[list[str]]:
        """Lee todas las filas de un CSV, probando distintos encodings y delimitadores."""
        for encoding in ("utf-8-sig", "latin-1", "utf-8"):
            try:
                with open(csv_path, newline="", encoding=encoding) as f:
                    sample = f.read(4096)
                    f.seek(0)
                    try:
                        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                    except csv.Error:
                        dialect = csv.excel  # type: ignore[assignment]
                    return list(csv.reader(f, dialect))
            except UnicodeDecodeError:
                continue
        return []

    def _sync_to_bi_onedrive(self, file_path: Path, week: int, year: int) -> None:
        """Copy the file to OneDrive/BI/Data Clientes/TT/Nuevo/1. B2B/Megatiendas/{year}/S{week:02d}/"""
        base = settings.ONEDRIVE_BI_MEGATIENDAS_BASE
        if base is None:
            self.logger.debug("[%s] ONEDRIVE_BI_MEGATIENDAS_BASE no configurado, sync omitido.", self.proveedor.display_name)
            return

        target_dir = base / str(year) / f"S{week:02d}"
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            self.logger.warning("[%s] No se pudo crear carpeta OneDrive BI: %s", self.proveedor.display_name, exc)
            return

        target = target_dir / file_path.name
        try:
            shutil.copy2(file_path, target)
            self.logger.info(
                "[%s] [OneDrive/BI] %s -> .../%s/S%02d/%s",
                self.proveedor.display_name, file_path.name, year, week, file_path.name,
            )
        except OSError as exc:
            self.logger.warning("[%s] Error copiando a OneDrive BI: %s", self.proveedor.display_name, exc)

    # ── Utilidades ────────────────────────────────────────────────────────────

    def _resolve_week_year_month(self) -> tuple[int, int, int]:
        """Derive ISO week, year and month from proveedor.fecha_desde."""
        try:
            d = date.fromisoformat(self.proveedor.fecha_desde)
        except (ValueError, TypeError):
            d = date.today()
        iso = d.isocalendar()
        return iso[1], iso[0], d.month

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
