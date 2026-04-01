from __future__ import annotations

import logging
import os
import re
import shutil
import socket
from datetime import date, datetime
from pathlib import Path
from calendar import monthrange
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

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
        self._sync_to_hb(ventas_final, week, year)

        downloaded_files = [ventas_final]
        msg_parts = [ventas_final.name]

        if inventario_path is not None:
            inventario_final = self._rename_file(
                inventario_path, f"Xeon_{zona}_Inventario_{today}{inventario_path.suffix}"
            )
            self._sync_to_hb(inventario_final, week, year)
            downloaded_files.append(inventario_final)
            msg_parts.append(inventario_final.name)
        else:
            msg_parts.append("Inventario: no descargado")

        self.logger.info(
            "[%s] Completado: %s",
            self.proveedor.display_name, " | ".join(msg_parts),
        )
        return ExecutionResult(
            proveedor=self.proveedor.display_name,
            portal_tipo=self.proveedor.portal_tipo,
            success=True,
            message="Descargados: " + " | ".join(msg_parts),
            downloaded_file=ventas_final,
            downloaded_files=downloaded_files,
            portal_handled_sync=True,
        )

    def _run_playwright_session(self) -> tuple[Path, Path | None]:
        headless = os.getenv("RPA_HEADLESS", "0").strip().lower() in {"1", "true", "yes", "si"}

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=headless, channel=settings.BROWSER_CHANNEL)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            try:
                self._login(page)
                ventas_path = self._download_paretto(page)
                try:
                    inventario_path: Path | None = self._download_listaprecios(page)
                except Exception as exc:
                    self.logger.warning(
                        "[%s] Inventario no descargado (no fatal): %s",
                        self.proveedor.display_name, exc,
                    )
                    inventario_path = None
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
        # Capturar URL base y HTML del form desde la respuesta de Reportes_Paretto.php
        paretto_base_url: list[str] = []
        paretto_html: list[str] = []

        def _capture(resp) -> None:
            if "Reportes_Paretto" in resp.url:
                if not paretto_base_url:
                    paretto_base_url.append(resp.url)
                try:
                    paretto_html.append(resp.body().decode("latin-1", errors="replace"))
                except Exception:
                    pass

        page.on("response", _capture)

        self.logger.info("[%s] Buscando menu paretto en home...", self.proveedor.display_name)

        # Hover sobre Reportes para desplegar submenu CSS hover
        page.locator("a:text('Reportes')").first.hover()
        page.wait_for_timeout(600)

        paretto_link = page.locator("a[href*='paretto' i]").first
        try:
            paretto_link.wait_for(state="visible", timeout=10000)
        except PlaywrightTimeoutError:
            page.screenshot(path=str(self.screenshot_dir / "xeon_home_menu.png"))
            raise RuntimeError("No se encontro el link de Paretto en el menu de home.php.")

        paretto_link.click()
        page.wait_for_load_state("networkidle", timeout=30000)

        if not paretto_base_url:
            raise RuntimeError("No se capturo URL de Reportes_Paretto.php del servidor :8080.")

        # Navegar directamente al form en :8080 — sin CORS, CSS y JS cargan del mismo servidor
        self.logger.info("[%s] Navegando al form en :8080: %s", self.proveedor.display_name, paretto_base_url[0])
        page.goto(paretto_base_url[0], wait_until="networkidle", timeout=60000)
        page.screenshot(path=str(self.screenshot_dir / "xeon_paretto_form8080.png"))

        # Seleccionar mes — ahora estamos en :8080, sin CORS, todo funciona normalmente
        page.wait_for_selector("#LstMes, select[name='LstMes']", state="visible", timeout=20000)
        self._select_mes(page)
        page.wait_for_timeout(1500)

        # Click en Buscar
        self.logger.info("[%s] Buscando ventas...", self.proveedor.display_name)
        page.locator("#BtoBuscar, input[name='BtoBuscar'], button[name='BtoBuscar']").first.click()
        page.wait_for_load_state("networkidle", timeout=60000)
        self.logger.info("[%s] URL tras buscar: %s", self.proveedor.display_name, page.url)
        page.screenshot(path=str(self.screenshot_dir / "xeon_paretto_results.png"))

        try:
            page.locator("a[href*='ParettoExportar'], a[href*='Exportar']").wait_for(state="attached", timeout=30000)
        except PlaywrightTimeoutError:
            body_preview = page.evaluate("document.body?.innerText?.substring(0, 400) || 'sin body'")
            self.logger.warning("[%s] Sin link exportar. Pagina: %s", self.proveedor.display_name, body_preview)
            page.screenshot(path=str(self.screenshot_dir / "xeon_paretto_timeout.png"))
            raise RuntimeError("Timeout esperando el link de exportacion de ventas.")

        self.logger.info("[%s] Exportando ventas...", self.proveedor.display_name)
        with page.expect_download(timeout=60000) as dl:
            page.locator("a[href*='ParettoExportar'], a[href*='Exportar']").first.click()

        return self._save_download(dl.value, "ventas")

    def _download_listaprecios(self, page: Page) -> Path:
        # Capturar URL base del form desde la respuesta cross-origin (mismo patron que paretto)
        parsed_main = urlparse(self._base_url())
        main_netloc = parsed_main.netloc

        lista_base_url: list[str] = []

        def _capture_lista(resp) -> None:
            if lista_base_url:
                return
            resp_netloc = urlparse(resp.url).netloc
            if resp_netloc and resp_netloc != main_netloc:
                self.logger.debug(
                    "[%s] Cross-origin listaprecios: %s", self.proveedor.display_name, resp.url
                )
                lista_base_url.append(resp.url)

        page.on("response", _capture_lista)

        # Volver a home y desplegar submenu Reportes → Inv. y Lista de Precios
        home_url = self._base_url() + "home.php"
        self.logger.info("[%s] Navegando a home para listaprecios: %s", self.proveedor.display_name, home_url)
        page.goto(home_url, wait_until="domcontentloaded", timeout=30000)

        page.locator("a:text('Reportes')").first.hover()
        page.wait_for_timeout(600)

        inv_link = page.locator(
            "a[href*='listaprecios' i], a[href*='lista_precio' i], "
            "a:text-is('Inv. y Lista de Precios'), a:text('Inv.'), a:text('Lista de Precios')"
        ).first
        try:
            inv_link.wait_for(state="visible", timeout=10000)
            inv_link.click()
            page.wait_for_load_state("networkidle", timeout=30000)
        except PlaywrightTimeoutError:
            self.logger.info(
                "[%s] Link 'Inv. y Lista de Precios' no visible en menu, navegando directo.",
                self.proveedor.display_name,
            )
            page.goto(self._base_url() + "home.php?view=listaprecios", wait_until="networkidle", timeout=30000)

        self.logger.info("[%s] URL tras carga listaprecios: %s", self.proveedor.display_name, page.url)
        self.logger.info("[%s] Cross-origin URLs capturadas: %s", self.proveedor.display_name, lista_base_url)

        # Si el form está en :8080, navegar directamente (sin CORS)
        if lista_base_url:
            self.logger.info("[%s] Navegando al form en :8080: %s", self.proveedor.display_name, lista_base_url[0])
            page.goto(lista_base_url[0], wait_until="networkidle", timeout=30000)

        page.screenshot(path=str(self.screenshot_dir / "xeon_listaprecios_form.png"))

        # Seleccionar proveedor en el select del form
        self._select_proveedor(page)

        # Activar "Ver todas mis lineas": poner viewall=1
        self._select_todas_lineas(page)

        page.wait_for_timeout(500)
        page.screenshot(path=str(self.screenshot_dir / "xeon_listaprecios_filled.png"))

        # Click Buscar — el boton es <button class="azul" onclick="Search();">
        self.logger.info("[%s] Buscando inventario...", self.proveedor.display_name)
        buscar = page.locator("button.azul, button[onclick*='Search'], button:text('BUSCAR'), button:text('Buscar')").first
        buscar.wait_for(state="visible", timeout=15000)
        buscar.click()
        page.wait_for_load_state("networkidle", timeout=60000)
        page.screenshot(path=str(self.screenshot_dir / "xeon_listaprecios_results.png"))

        try:
            page.locator("a[href*='Exportar']").wait_for(state="attached", timeout=60000)
        except PlaywrightTimeoutError:
            body_preview = page.evaluate("document.body?.innerText?.substring(0, 400) || 'sin body'")
            self.logger.warning("[%s] Sin link exportar inventario. Pagina: %s", self.proveedor.display_name, body_preview)
            page.screenshot(path=str(self.screenshot_dir / "xeon_listaprecios_timeout.png"))
            raise RuntimeError("Timeout esperando el link de exportacion de inventario.")

        self.logger.info("[%s] Exportando inventario...", self.proveedor.display_name)
        with page.expect_download(timeout=60000) as dl:
            page.locator("a[href*='Exportar']").first.click()

        return self._save_download(dl.value, "inventario")

    def _select_proveedor(self, page: Page) -> None:
        """Selecciona el proveedor en el primer select visible del form frmsearch."""
        result = page.evaluate(
            """(nombre) => {
                // Buscar dentro del form frmsearch, o en toda la pagina
                const root = document.querySelector('form[name="frmsearch"]') || document;
                const selects = Array.from(root.querySelectorAll('select'));
                if (!selects.length) return 'NO_SELECTS';
                // Loguear todos para diagnostico
                const info = selects.map(s => (s.name || s.id || '?') + ':' + Array.from(s.options).map(o => o.text).join(',')).join(' || ');
                console.log('Selects:', info);
                // Buscar en cada select la opcion que coincida con el proveedor
                for (const sel of selects) {
                    for (const opt of sel.options) {
                        if (opt.text.trim().toLowerCase() === nombre.toLowerCase()) {
                            sel.value = opt.value;
                            sel.dispatchEvent(new Event('change'));
                            return 'EXACT[' + (sel.name||sel.id) + ']:' + opt.text;
                        }
                    }
                }
                for (const sel of selects) {
                    for (const opt of sel.options) {
                        if (opt.text.toLowerCase().includes(nombre.toLowerCase())) {
                            sel.value = opt.value;
                            sel.dispatchEvent(new Event('change'));
                            return 'PARTIAL[' + (sel.name||sel.id) + ']:' + opt.text;
                        }
                    }
                }
                return 'NOT_FOUND || ' + info;
            }""",
            self.proveedor.proveedor,
        )
        self.logger.info("[%s] Proveedor seleccionado: %s", self.proveedor.display_name, result)

    def _select_todas_lineas(self, page: Page) -> None:
        """Activa 'Ver todas mis lineas' poniendo el input hidden viewall=1."""
        result = page.evaluate(
            """() => {
                // El form tiene <input type="hidden" name="viewall" id="viewall" value="0">
                const viewall = document.getElementById('viewall') ||
                                document.querySelector('input[name="viewall"]');
                if (viewall) {
                    viewall.value = '1';
                    return 'viewall=1';
                }
                // Fallback: buscar radio/checkbox/label con texto 'todas'
                const labels = Array.from(document.querySelectorAll('label, span, a'));
                for (const el of labels) {
                    const txt = el.textContent.toLowerCase();
                    if (txt.includes('todas') && txt.includes('l')) {
                        return 'LABEL_FOUND:' + el.textContent.trim();
                    }
                }
                return 'NOT_FOUND';
            }"""
        )
        self.logger.info("[%s] Todas las lineas: %s", self.proveedor.display_name, result)
        # Si encontramos el label pero no el hidden, intentar click real
        if result.startswith("LABEL_FOUND"):
            label_text = result.split(":", 1)[1]
            try:
                page.locator(f"label:text('{label_text}'), span:text('{label_text}')").first.click()
                page.wait_for_timeout(300)
            except Exception:
                pass

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
