from __future__ import annotations

import logging
import os
import re
import shutil
import socket
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import Download, Page, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from app.config import settings
from app.core.models import ExecutionResult, Proveedor
from app.portals.base_portal import BasePortal


class PortalXeon(BasePortal):

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

        host, port = self._parse_host_port()
        if not self._is_reachable(host, port):
            msg = f"VPN no activa o portal inaccesible ({host}:{port})."
            self.logger.warning("[%s] %s", self.proveedor.display_name, msg)
            return ExecutionResult(
                proveedor=self.proveedor.display_name,
                portal_tipo=self.proveedor.portal_tipo,
                success=False,
                message=msg,
            )

        week, year = self._resolve_week_year()
        zona = self._extract_zona()

        try:
            ventas_path, inventario_path = self._run_playwright_session()
        except Exception as exc:
            self.logger.exception(
                "[%s] Error durante descarga Xeon: %s", self.proveedor.display_name, exc
            )
            return ExecutionResult(
                proveedor=self.proveedor.display_name,
                portal_tipo=self.proveedor.portal_tipo,
                success=False,
                message=f"Error Xeon: {exc}",
            )

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
            "[%s] Completado: %s | %s",
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

    def _run_playwright_session(self) -> tuple[Path, Path]:
        headless = os.getenv("RPA_HEADLESS", "0").strip().lower() in {"1", "true", "yes", "si"}

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=headless, channel=settings.BROWSER_CHANNEL)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            try:
                self._login(page)
                ventas_path = self._download_paretto(page)
                inventario_path = self._download_listaprecios(page)
            finally:
                context.close()
                browser.close()

        return ventas_path, inventario_path

    def _login(self, page: Page) -> None:
        login_url = self._base_url()
        self.logger.info("[%s] Login en %s", self.proveedor.display_name, login_url)

        page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
        page.screenshot(path=str(self.screenshot_dir / "xeon_01_login_page.png"))

        page.locator(
            "input[name='usuario'], input[name='username'], input[name='user'], "
            "input[placeholder*='suario' i], input[placeholder*='sername' i]"
        ).first.fill(self.proveedor.usuario)

        page.locator("input[type='password']").first.fill(self.proveedor.password)
        page.screenshot(path=str(self.screenshot_dir / "xeon_02_filled.png"))

        page.get_by_text("Ingresar", exact=False).click()
        page.wait_for_timeout(3000)
        page.screenshot(path=str(self.screenshot_dir / "xeon_03_after_click.png"))
        self.logger.info("[%s] URL tras click: %s", self.proveedor.display_name, page.url)

        page.wait_for_url("**/home.php**", timeout=30000)
        self.logger.info("[%s] Login OK.", self.proveedor.display_name)

    def _download_paretto(self, page: Page) -> Path:
        # Interceptar la respuesta de Reportes_Paretto.php a nivel de red (bypasea CORS)
        paretto_html: list[str] = []

        def _capture_paretto_response(resp) -> None:
            if "Reportes_Paretto" in resp.url or ("8080" in resp.url and "Paretto" in resp.url):
                try:
                    body = resp.body().decode("latin-1", errors="replace")
                    paretto_html.append(body)
                    self.logger.info(
                        "[%s] Respuesta Reportes_Paretto capturada: status=%s len=%d preview=%s",
                        self.proveedor.display_name, resp.status, len(body), body[:200],
                    )
                except Exception as exc:
                    self.logger.warning("[%s] Error capturando respuesta paretto: %s", self.proveedor.display_name, exc)

        page.on("response", _capture_paretto_response)

        self.logger.info("[%s] Buscando menu paretto en home...", self.proveedor.display_name)

        # Hover sobre Reportes para desplegar submenu CSS hover
        page.locator("a:text('Reportes')").first.hover()
        page.wait_for_timeout(600)

        # Hacer click en el link de paretto del menu
        paretto_link = page.locator("a[href*='paretto' i]").first
        try:
            paretto_link.wait_for(state="visible", timeout=10000)
        except PlaywrightTimeoutError:
            page.screenshot(path=str(self.screenshot_dir / "xeon_home_menu.png"))
            raise RuntimeError("No se encontro el link de Paretto en el menu de home.php.")

        paretto_link.click()
        page.wait_for_load_state("networkidle", timeout=30000)
        self.logger.info("[%s] URL tras click paretto: %s", self.proveedor.display_name, page.url)
        page.screenshot(path=str(self.screenshot_dir / "xeon_paretto_loaded.png"))

        # Si #BOX vacio (CORS bloqueo) pero tenemos la respuesta capturada, inyectarla
        box_empty = page.evaluate("!document.getElementById('BOX')?.innerHTML?.trim()")
        if box_empty and paretto_html:
            self.logger.info("[%s] Inyectando HTML de Reportes_Paretto en #BOX (fix CORS)...", self.proveedor.display_name)
            page.evaluate("(html) => { document.getElementById('BOX').innerHTML = html; }", paretto_html[0])
            page.wait_for_timeout(500)
        elif box_empty:
            page.screenshot(path=str(self.screenshot_dir / "xeon_paretto_empty.png"))
            raise RuntimeError("#BOX vacio y no se capturo respuesta de Reportes_Paretto.php.")

        page.wait_for_selector("#LstMes, select[name='LstMes']", state="attached", timeout=20000)

        self._select_mes(page)
        page.wait_for_timeout(1000)

        self.logger.info("[%s] Buscando ventas...", self.proveedor.display_name)
        # El boton existe en el HTML inyectado pero puede no ser visible (CSS de otro servidor).
        # Usar JS click directo para no depender de visibilidad.
        clicked = page.evaluate(
            "() => { const b = document.getElementById('BtoBuscar'); if(b){ b.click(); return true; } return false; }"
        )
        if not clicked:
            raise RuntimeError("No se encontro #BtoBuscar para hacer click.")
        self.logger.info("[%s] Click en BtoBuscar OK, esperando resultados...", self.proveedor.display_name)

        # Esperar navegacion o carga de resultados (puede ir a :8080 o quedarse en la misma pagina)
        page.wait_for_load_state("networkidle", timeout=60000)
        self.logger.info("[%s] URL tras buscar: %s", self.proveedor.display_name, page.url)
        page.screenshot(path=str(self.screenshot_dir / "xeon_paretto_results.png"))

        try:
            page.locator("a[href*='ParettoExportar']").wait_for(state="attached", timeout=30000)
        except PlaywrightTimeoutError:
            page.screenshot(path=str(self.screenshot_dir / "xeon_paretto_timeout.png"))
            raise RuntimeError("Timeout esperando el link de exportacion de ventas (ParettoExportar).")

        self.logger.info("[%s] Exportando ventas...", self.proveedor.display_name)
        with page.expect_download(timeout=60000) as dl:
            page.evaluate("() => { const a = document.querySelector('a[href*=\"ParettoExportar\"]'); if(a) a.click(); }")

        return self._save_download(dl.value, "ventas")

    def _download_listaprecios(self, page: Page) -> Path:
        lista_url = self._base_url() + "home.php?view=listaprecios"
        self.logger.info("[%s] Navegando a listaprecios: %s", self.proveedor.display_name, lista_url)

        page.goto(lista_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        self.logger.info("[%s] URL tras carga listaprecios: %s", self.proveedor.display_name, page.url)

        page.wait_for_selector("input[name='BtoBuscar'], #BtoBuscar", timeout=20000)
        self.logger.info("[%s] Buscando inventario...", self.proveedor.display_name)
        page.locator("input[name='BtoBuscar'], #BtoBuscar, input[value*='uscar' i]").first.click()

        try:
            page.locator("a[href*='Exportar']").wait_for(state="visible", timeout=60000)
        except PlaywrightTimeoutError:
            page.screenshot(path=str(self.screenshot_dir / "xeon_listaprecios_timeout.png"))
            raise RuntimeError("Timeout esperando el link de exportacion de inventario.")

        self.logger.info("[%s] Exportando inventario...", self.proveedor.display_name)
        with page.expect_download(timeout=60000) as dl:
            page.locator("a[href*='Exportar']").first.click()

        return self._save_download(dl.value, "inventario")

    def _select_mes(self, frame: Page) -> None:
        # Las opciones tienen formato "M333 => 2026-03-01 - 2026-03-31", buscar por año-mes
        try:
            d = date.fromisoformat(self.proveedor.fecha_desde)
            year_month = f"{d.year}-{d.month:02d}"
        except (ValueError, TypeError):
            year_month = self.proveedor.fecha_desde[:7]

        selected = frame.evaluate(
            """(yearMonth) => {
                const sel = document.getElementById('LstMes') || document.querySelector('select[name="LstMes"]');
                if (!sel) return false;
                // Loguear opciones disponibles para depuracion
                const opts = Array.from(sel.options).map(o => o.text).join(' | ');
                console.log('LstMes options:', opts);
                for (const opt of sel.options) {
                    if (opt.text.includes(yearMonth)) {
                        sel.value = opt.value;
                        sel.dispatchEvent(new Event('change'));
                        return opt.text;
                    }
                }
                return 'OPTIONS: ' + opts;
            }""",
            year_month,
        )
        if not selected or selected.startswith("OPTIONS:"):
            raise RuntimeError(
                f"No se encontro opcion en LstMes para {year_month}. Disponibles: {selected}"
            )
        self.logger.info("[%s] Mes seleccionado: %s", self.proveedor.display_name, selected)

    def _save_download(self, download: Download, tipo: str) -> Path:
        filename = download.suggested_filename or f"xeon_{tipo}_{self._extract_zona()}.xlsx"
        dest = self.download_dir / filename
        download.save_as(dest)
        self.logger.info("[%s] Guardado: %s", self.proveedor.display_name, dest)
        return dest

    def _rename_file(self, src: Path, new_name: str) -> Path:
        dest = src.parent / new_name
        shutil.move(str(src), str(dest))
        return dest

    def _sync_to_hb(self, file_path: Path, week: int, year: int) -> None:
        hb_dir = settings.ONEDRIVE_HB_DIR
        if hb_dir is None:
            self.logger.debug("[%s] ONEDRIVE_HB_DIR no configurado.", self.proveedor.display_name)
            return
        target_dir = hb_dir / self.proveedor.proveedor / f"S{week:02d}_{year}"
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            dest = target_dir / file_path.name
            if not dest.exists():
                shutil.copy2(file_path, dest)
                self.logger.info("[%s] Copiado a HB: %s", self.proveedor.display_name, dest)
        except OSError as exc:
            self.logger.warning("[%s] Error sync HB: %s", self.proveedor.display_name, exc)

    def _base_url(self) -> str:
        url = self.proveedor.login_url.strip()
        if not url.startswith(("http://", "https://")):
            url = "http://" + url
        if not url.endswith("/"):
            url += "/"
        parsed = urlparse(url)
        host = parsed.hostname or ""
        path = parsed.path.strip("/")
        if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", host) and not path:
            url += "tat_nuevo/"
        return url

    def _extract_zona(self) -> str:
        match = re.search(r"([A-Z]{2,4})$", self.proveedor.usuario.upper())
        if match:
            return match.group(1)
        return self.proveedor.sede_subportal.replace(" ", "_") or "X"

    def _resolve_week_year(self) -> tuple[int, int]:
        try:
            d = date.fromisoformat(self.proveedor.fecha_desde)
        except (ValueError, TypeError):
            d = date.today()
        iso = d.isocalendar()
        return iso[1], iso[0]

    def _parse_host_port(self) -> tuple[str, int]:
        url = self.proveedor.login_url.strip()
        url = re.sub(r"^https?://", "", url).split("/")[0]
        if ":" in url:
            host, port_str = url.rsplit(":", 1)
            try:
                return host, int(port_str)
            except ValueError:
                pass
        return url, 80

    @staticmethod
    def _is_reachable(host: str, port: int, timeout: int = 5) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False
