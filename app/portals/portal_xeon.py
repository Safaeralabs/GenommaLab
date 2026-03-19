"""Playwright implementation for Xeon TAT portal (Pastor Julio Delgado, canal tradicional)."""

from __future__ import annotations

import logging
import os
import re
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


class PortalXeon(BasePortal):
    """Xeon TAT workflow: login, Paretto de Ventas, Inventario Neto."""

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

        start_date = self.proveedor.fecha_desde
        end_date = self.proveedor.fecha_hasta
        week, year = self._resolve_week_year()
        zona = self._extract_zona()

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self._headless_mode(), channel=settings.BROWSER_CHANNEL)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            try:
                self._login(page)
                self.logger.info("[%s] Login completado.", self.proveedor.display_name)

                ventas_path = self._download_ventas(page, start_date, end_date)
                inventario_path = self._download_inventario(page)

            except (PlaywrightTimeoutError, PlaywrightError, Exception) as exc:
                self._take_screenshot(page, error_screenshot_path)
                self.logger.exception(
                    "[%s] Error durante descarga Xeon: %s",
                    self.proveedor.display_name, exc,
                )
                return ExecutionResult(
                    proveedor=self.proveedor.display_name,
                    portal_tipo=self.proveedor.portal_tipo,
                    success=False,
                    message=f"Error Xeon: {exc}",
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

        today = datetime.now().strftime("%d%m%Y")
        ventas_final = self._rename_file(
            ventas_path, f"Xeon_{zona}_Ventas_{today}{ventas_path.suffix}"
        )
        inventario_final = self._rename_file(
            inventario_path, f"Xeon_{zona}_Inventario_{today}{inventario_path.suffix}"
        )

        self._sync_to_hb(ventas_final, week, year)
        self._sync_to_hb(inventario_final, week, year)

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
        login_url = self._base_url()
        self.logger.info("[%s] Abriendo %s", self.proveedor.display_name, login_url)
        page.goto(login_url, wait_until="domcontentloaded", timeout=60000)

        # Usuario
        page.locator(
            "input[name='usuario'], input[name='user'], "
            "input[placeholder*='suario' i], input[placeholder*='user' i]"
        ).first.fill(self.proveedor.usuario)

        # Contraseña
        page.locator("input[type='password']").first.fill(self.proveedor.password)

        # Cód. de Seguridad – extraído de los dígitos iniciales del usuario (ej: "225BUC" → "225")
        security_code = self._extract_security_code()
        if security_code:
            cod_loc = page.locator(
                "input[name*='cod' i][name*='seg' i], "
                "input[name='cod_seg'], input[name='codseg'], "
                "input[name='seguridad'], input[placeholder*='eguridad' i], "
                "input[placeholder*='ódigo' i], input[placeholder*='odigo' i]"
            )
            if cod_loc.count():
                cod_loc.first.fill(security_code)
            else:
                # Fallback: tercer input de texto visible
                inputs = page.locator(
                    "input:not([type='password']):not([type='hidden']):not([type='submit'])"
                )
                if inputs.count() >= 3:
                    inputs.nth(2).fill(security_code)

        page.get_by_text("Ingresar", exact=False).click()
        page.wait_for_url("**/home.php**", timeout=30000)

    # ── Ventas (Paretto) ──────────────────────────────────────────────────────

    def _download_ventas(self, page: Page, fecha_desde: str, fecha_hasta: str) -> Path:
        """Navega a Paretto de Ventas, aplica filtros y descarga el Excel."""
        self.logger.info(
            "[%s] Descargando ventas (Paretto): %s → %s",
            self.proveedor.display_name, fecha_desde, fecha_hasta,
        )
        self._navigate_reportes_submenu(page, "Paretto")
        page.wait_for_url("**/home.php?view=paretto**", timeout=15000)

        # Seleccionar mes en el dropdown
        self._select_mes(page, fecha_desde)

        # Rellenar fechas en formato dd/mm/yyyy
        fecha_ini = self._to_portal_date(fecha_desde)
        fecha_fin = self._to_portal_date(fecha_hasta)

        date_inputs = page.locator(
            "input[name*='fecha_ini' i], input[placeholder*='dd/mm' i], "
            "input[name*='inicio' i], input[name*='desde' i]"
        )
        date_inputs.first.fill(fecha_ini)

        date_inputs_fin = page.locator(
            "input[name*='fecha_fin' i], input[name*='fin' i], "
            "input[name*='hasta' i], input[name*='final' i]"
        )
        date_inputs_fin.last.fill(fecha_fin)

        page.get_by_text("Buscar", exact=False).first.click()
        page.wait_for_load_state("networkidle", timeout=60000)

        return self._click_export_excel(page, "ventas")

    def _select_mes(self, page: Page, fecha_desde: str) -> None:
        """Selecciona en el dropdown 'Mes' el período que contiene fecha_desde."""
        try:
            from datetime import date as _date
            d = _date.fromisoformat(fecha_desde)
            year_str = str(d.year)
            month_str = f"{d.month:02d}"

            select = page.locator("select").first
            select.wait_for(state="visible", timeout=5000)

            options = page.evaluate("""() => {
                const sel = document.querySelector('select');
                if (!sel) return [];
                return Array.from(sel.options).map(o => ({value: o.value, text: o.text.trim()}));
            }""")

            target_val = None
            for opt in options:
                text = opt["text"]
                if year_str in text and month_str in text:
                    target_val = opt["value"]
                    break

            if target_val is None:
                # Fallback: buscar por nombre de mes en español
                _MONTHS_ES = {
                    1: "ene", 2: "feb", 3: "mar", 4: "abr",
                    5: "may", 6: "jun", 7: "jul", 8: "ago",
                    9: "sep", 10: "oct", 11: "nov", 12: "dic",
                }
                month_es = _MONTHS_ES.get(d.month, "")
                for opt in options:
                    text = opt["text"].lower()
                    if year_str in text and month_es in text:
                        target_val = opt["value"]
                        break

            if target_val:
                select.select_option(value=target_val)
                self.logger.info(
                    "[%s] Mes seleccionado: %s", self.proveedor.display_name, target_val
                )
            else:
                self.logger.warning(
                    "[%s] No se encontró opción de mes para %s-%s; se deja el valor por defecto.",
                    self.proveedor.display_name, year_str, month_str,
                )
        except Exception as exc:
            self.logger.warning(
                "[%s] Error seleccionando mes: %s", self.proveedor.display_name, exc
            )

    # ── Inventario ────────────────────────────────────────────────────────────

    def _download_inventario(self, page: Page) -> Path:
        """Navega a Inventario Neto / Lista de Precios y descarga el Excel."""
        self.logger.info("[%s] Descargando inventario.", self.proveedor.display_name)
        self._navigate_reportes_submenu(page, "Inventario")
        page.wait_for_load_state("domcontentloaded", timeout=15000)

        # Si hay un botón BUSCAR, hacer click para cargar los resultados
        buscar = page.locator(
            "button:has-text('BUSCAR'), button:has-text('Buscar'), "
            "input[type='submit'][value*='BUSCAR' i], input[type='button'][value*='BUSCAR' i]"
        )
        if buscar.count():
            buscar.first.click()
            page.wait_for_load_state("networkidle", timeout=60000)

        return self._click_export_excel(page, "inventario")

    # ── Navigation ────────────────────────────────────────────────────────────

    def _navigate_reportes_submenu(self, page: Page, submenu_text: str) -> None:
        """Hover sobre 'Reportes' en la barra de navegación y clic en el ítem del submenú."""
        reportes = page.get_by_role("link", name=re.compile(r"^Reportes$", re.IGNORECASE))
        reportes.hover()
        page.wait_for_timeout(500)
        # El submenú debería aparecer; clic en el ítem que contiene submenu_text
        page.locator(
            f"a:has-text('{submenu_text}'), li:has-text('{submenu_text}') > a"
        ).first.click()

    # ── Export ────────────────────────────────────────────────────────────────

    def _click_export_excel(self, page: Page, tipo: str) -> Path:
        """Localiza y hace clic en el botón de exportar a Excel; retorna la ruta guardada."""
        self.logger.info("[%s] Exportando %s a Excel.", self.proveedor.display_name, tipo)

        export_loc = page.locator(
            "a:has-text('Excel'), button:has-text('Excel'), "
            "a:has-text('export'), button:has-text('export'), "
            "a:has-text('Export'), a:has-text('EXCEL'), "
            "input[value*='Excel' i], input[value*='Export' i], "
            "a[href*='excel'], a[href*='export']"
        ).first
        export_loc.wait_for(state="visible", timeout=15000)

        with page.expect_download(timeout=120000) as dl_info:
            export_loc.click()

        download = dl_info.value
        dest = self.download_dir / (download.suggested_filename or f"xeon_{tipo}.xlsx")
        download.save_as(dest)
        self.logger.info("[%s] Archivo guardado: %s", self.proveedor.display_name, dest)
        return dest

    # ── OneDrive HB ───────────────────────────────────────────────────────────

    def _rename_file(self, src: Path, new_name: str) -> Path:
        dest = src.parent / new_name
        shutil.move(str(src), str(dest))
        return dest

    def _sync_to_hb(self, file_path: Path, week: int, year: int) -> None:
        """Copia el archivo a OneDrive/HB/{cliente}/S{week:02d}_{year}/"""
        hb_dir = settings.ONEDRIVE_HB_DIR
        if hb_dir is None:
            self.logger.debug(
                "[%s] ONEDRIVE_HB_DIR no configurado, sync omitido.",
                self.proveedor.display_name,
            )
            return

        target_dir = hb_dir / self.proveedor.proveedor / f"S{week:02d}_{year}"
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            self.logger.warning(
                "[%s] No se pudo crear carpeta OneDrive HB: %s",
                self.proveedor.display_name, exc,
            )
            return

        try:
            shutil.copy2(file_path, target_dir / file_path.name)
            self.logger.info(
                "[%s] [OneDrive/HB] %s → HB/%s/S%02d_%s/%s",
                self.proveedor.display_name,
                file_path.name,
                self.proveedor.proveedor,
                week,
                year,
                file_path.name,
            )
        except OSError as exc:
            self.logger.warning(
                "[%s] Error copiando a OneDrive HB: %s",
                self.proveedor.display_name, exc,
            )

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _base_url(self) -> str:
        """Normaliza la URL del portal asegurando protocolo y ruta /tat_nuevo/."""
        url = self.proveedor.login_url.strip()
        if not url.startswith(("http://", "https://")):
            url = "http://" + url
        if not url.endswith("/"):
            url += "/"
        if "tat_nuevo" not in url:
            url += "tat_nuevo/"
        return url

    def _extract_zona(self) -> str:
        """Extrae el sufijo de zona del usuario (ej: '225BUC' → 'BUC')."""
        match = re.search(r"([A-Z]{2,4})$", self.proveedor.usuario.upper())
        if match:
            return match.group(1)
        # Fallback: usar sede_subportal sin espacios
        return self.proveedor.sede_subportal.replace(" ", "_") or "X"

    def _extract_security_code(self) -> str:
        """Extrae el prefijo numérico del usuario como Cód. de Seguridad (ej: '225BUC' → '225')."""
        match = re.match(r"^(\d+)", self.proveedor.usuario)
        return match.group(1) if match else ""

    def _resolve_week_year(self) -> tuple[int, int]:
        from datetime import date
        try:
            d = date.fromisoformat(self.proveedor.fecha_desde)
        except (ValueError, TypeError):
            d = date.today()
        iso = d.isocalendar()
        return iso[1], iso[0]

    @staticmethod
    def _to_portal_date(iso_date: str) -> str:
        """Convierte 'yyyy-mm-dd' a 'dd/mm/yyyy' (formato del portal)."""
        try:
            d = datetime.strptime(iso_date, "%Y-%m-%d")
            return d.strftime("%d/%m/%Y")
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
