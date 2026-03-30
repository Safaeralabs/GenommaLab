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

        today = datetime.now()
        renamed = self._rename_file(raw_csv, today)
        self._sync_to_bi_onedrive(renamed, week, year)

        delta_csv = self._compute_week_delta(renamed, week, year)
        ventas_file = delta_csv if delta_csv is not None else renamed

        inv_file = self._build_inventario_csv(ventas_file, week)

        files_for_homologation = [ventas_file]
        if inv_file is not None:
            files_for_homologation.append(inv_file)

        self.logger.info("[%s] Descarga completada: %s", self.proveedor.display_name, renamed.name)
        return ExecutionResult(
            proveedor=self.proveedor.display_name,
            portal_tipo=self.proveedor.portal_tipo,
            success=True,
            message=f"Descargado: {renamed.name}",
            downloaded_file=ventas_file,
            downloaded_files=files_for_homologation,
            portal_handled_sync=True,
        )

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
        self.logger.info("[%s] Esperando carga de tabla DESCARGAS VENTAS...", self.proveedor.display_name)
        page.locator(".dataTables_info, .dataTables_wrapper, button:has-text('CSV')").first.wait_for(
            state="visible", timeout=300000
        )
        self.logger.info("[%s] Tabla cargada.", self.proveedor.display_name)

    def _apply_filters(self, page: Page, month: int, year: int) -> None:
        self.logger.info(
            "[%s] Aplicando filtros: proveedor=%s mes=%s anio=%s",
            self.proveedor.display_name, PROVIDER_FILTER, month, year,
        )
        self._fill_tfoot_filter(page, "MES", str(month))
        self._fill_tfoot_filter(page, "ANIO", str(year))
        self._fill_tfoot_filter(page, "PROVEEDOR", PROVIDER_FILTER)
        page.wait_for_timeout(5000)
        self.logger.info("[%s] Filtros aplicados.", self.proveedor.display_name)

    def _fill_tfoot_filter(self, page: Page, placeholder: str, value: str) -> None:
        inp = page.locator(f"tfoot input[placeholder='{placeholder}']")
        inp.wait_for(state="visible", timeout=10000)
        inp.click()
        inp.fill(value)
        inp.press("Enter")
        page.wait_for_timeout(500)

    def _download_csv(self, page: Page) -> Path:
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

    def _rename_file(self, csv_path: Path, download_date: datetime) -> Path:
        new_name = f"Megatiendas_{download_date.strftime('%d%m%Y')}.csv"
        new_path = csv_path.parent / new_name
        shutil.move(str(csv_path), str(new_path))
        return new_path

    def _compute_week_delta(self, current_csv: Path, week: int, year: int) -> Path | None:
        base = settings.ONEDRIVE_BI_MEGATIENDAS_BASE
        if base is None:
            return None

        prev_week = week - 1
        prev_year = year
        if prev_week == 0:
            prev_year -= 1
            prev_week = date(prev_year, 12, 28).isocalendar()[1]

        prev_dir = base / str(prev_year) / f"S{prev_week:02d}"
        prev_files = sorted(prev_dir.glob("Megatiendas_*.csv")) if prev_dir.exists() else []

        if not prev_files:
            self.logger.info(
                "[%s] No hay archivo previo en S%02d — se usará el mes completo para homologación.",
                self.proveedor.display_name, prev_week,
            )
            return None

        prev_csv = prev_files[-1]
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

    def _build_inventario_csv(self, ventas_csv: Path, week: int) -> Path | None:
        rows = self._read_csv_rows(ventas_csv)
        if len(rows) < 2:
            return None

        header = rows[0]

        units_col: int | None = None
        for idx, col in enumerate(header):
            norm = col.strip().upper().replace("_", "").replace(" ", "")
            if "VENTAUNIDAD" in norm or norm == "UNIDADES" or norm == "CANTIDAD":
                units_col = idx
                break

        if units_col is None:
            self.logger.warning(
                "[%s] No se encontró columna de unidades en el delta; inventario omitido.",
                self.proveedor.display_name,
            )
            return None

        inv_rows: list[list[str]] = [header]
        for row in rows[1:]:
            new_row = list(row)
            try:
                raw = new_row[units_col].strip().replace(",", ".").replace(" ", "")
                val = float(raw) * 4
                new_row[units_col] = str(int(val)) if val == int(val) else f"{val:.2f}"
            except (ValueError, IndexError):
                pass
            inv_rows.append(new_row)

        inv_path = ventas_csv.parent / f"Megatiendas_inventario_S{week:02d}.csv"
        with open(inv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerows(inv_rows)

        self.logger.info(
            "[%s] Inventario derivado generado (%d filas, unidades ×4): %s",
            self.proveedor.display_name, len(inv_rows) - 1, inv_path.name,
        )
        return inv_path

    def _read_csv_rows(self, csv_path: Path) -> list[list[str]]:
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

    def _resolve_week_year_month(self) -> tuple[int, int, int]:
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
