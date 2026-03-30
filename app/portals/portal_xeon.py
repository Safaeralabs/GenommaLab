from __future__ import annotations

import logging
import os
import re
import shutil
import socket
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import requests
from playwright.sync_api import sync_playwright

from app.config import settings
from app.core.models import ExecutionResult, Proveedor
from app.portals.base_portal import BasePortal

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


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
            cookies, page_htmls = self._login_and_get_cookies()

            session = requests.Session()
            session.headers.update({"User-Agent": _UA})
            for c in cookies:
                session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

            backend_url, qs = self._resolve_backend_url(session, "paretto", page_htmls.get("paretto"))
            ventas_path = self._download_ventas(session, backend_url, qs)

            backend_inv_url, qs_inv = self._resolve_backend_url(session, "listaprecios", page_htmls.get("listaprecios"))
            inventario_path = self._download_inventario(session, backend_inv_url, qs_inv)

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

    def _login_and_get_cookies(self) -> tuple[list[dict], dict[str, str]]:
        login_url = self._base_url()
        self.logger.info("[%s] Login (Playwright) en %s", self.proveedor.display_name, login_url)

        headless = os.getenv("RPA_HEADLESS", "0").strip().lower() in {"1", "true", "yes", "si"}

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=headless, channel=settings.BROWSER_CHANNEL)
            context = browser.new_context()
            page = context.new_page()

            page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
            page.screenshot(path=str(self.screenshot_dir / "xeon_01_login_page.png"))

            user_loc = page.locator(
                "input[name='usuario'], input[name='username'], input[name='user'], "
                "input[placeholder*='suario' i], input[placeholder*='sername' i]"
            ).first
            user_loc.fill(self.proveedor.usuario)

            page.locator("input[type='password']").first.fill(self.proveedor.password)
            page.screenshot(path=str(self.screenshot_dir / "xeon_02_filled.png"))

            page.get_by_text("Ingresar", exact=False).click()
            page.wait_for_timeout(3000)
            page.screenshot(path=str(self.screenshot_dir / "xeon_03_after_click.png"))
            self.logger.info("[%s] URL tras click: %s", self.proveedor.display_name, page.url)

            page.wait_for_url("**/home.php**", timeout=30000)

            # Navegar directamente a cada vista dentro de la misma sesion del browser
            # para capturar el HTML con el iframe ya autenticado
            page_htmls: dict[str, str] = {}
            for view in ("paretto", "listaprecios"):
                view_url = self._base_url() + f"home.php?view={view}"
                self.logger.info("[%s] Capturando HTML de %s", self.proveedor.display_name, view_url)
                page.goto(view_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(1500)
                page_htmls[view] = page.content()

            cookies = context.cookies()
            context.close()
            browser.close()

        self.logger.info("[%s] Login OK — %d cookies obtenidas.", self.proveedor.display_name, len(cookies))
        return cookies, page_htmls

    def _resolve_backend_url(
        self, session: requests.Session, view: str, cached_html: str | None = None
    ) -> tuple[str, dict[str, str]]:
        frontend = self._base_url() + f"home.php?view={view}"

        if cached_html:
            html = cached_html
            self.logger.debug("[%s] Usando HTML capturado por Playwright para '%s'.", self.proveedor.display_name, view)
        else:
            resp = session.get(frontend, timeout=30)
            resp.raise_for_status()
            html = resp.text

        match = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if not match:
            raise RuntimeError(
                f"No se encontro iframe en home.php?view={view}. "
                "El portal puede haber cambiado su estructura."
            )

        iframe_src = match.group(1)
        full_url = urljoin(frontend, iframe_src)
        parsed = urlparse(full_url)
        base = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
        qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}

        self.logger.debug(
            "[%s] Backend URL para '%s': %s (params: %s)",
            self.proveedor.display_name, view, base, qs,
        )
        return base, qs

    def _download_ventas(
        self,
        session: requests.Session,
        backend_url: str,
        qs: dict[str, str],
    ) -> Path:
        self.logger.info(
            "[%s] Descargando ventas (Paretto) %s → %s",
            self.proveedor.display_name,
            self.proveedor.fecha_desde,
            self.proveedor.fecha_hasta,
        )

        resp = session.get(backend_url, params=qs, timeout=30)
        resp.raise_for_status()
        mes_value = self._find_mes_value(resp.text)
        self.logger.info("[%s] Mes seleccionado: %s", self.proveedor.display_name, mes_value)

        form = {
            "LstMes":      mes_value,
            "TxtFecIni":   self.proveedor.fecha_desde,
            "TxtFecFin":   self.proveedor.fecha_hasta,
            "TxtSistema":  qs.get("S", "B"),
            "TxtSucursal": qs.get("Sucursal", self._extract_zona()),
            "TxtLstMes":   "",
            "TxtZona":     qs.get("Zona", "0"),
            "TxtLinea":    qs.get("Linea", "224,225"),
            "Mobile":      "0",
            "BtoBuscar":   "",
        }
        resp = session.post(backend_url, params=qs, data=form, timeout=60)
        resp.raise_for_status()

        export_url = self._find_export_href(resp.text, backend_url, "ParettoExportar")
        self.logger.info("[%s] Export URL ventas: %s", self.proveedor.display_name, export_url)

        return self._download_file(session, export_url, "ventas")

    def _download_inventario(
        self,
        session: requests.Session,
        backend_url: str,
        qs: dict[str, str],
    ) -> Path:
        self.logger.info("[%s] Descargando inventario (Lista de Precios).", self.proveedor.display_name)

        resp = session.get(backend_url, params=qs, timeout=30)
        resp.raise_for_status()

        form = {
            "TxtSucursal": qs.get("Sucursal", self._extract_zona()),
            "TxtSistema":  qs.get("S", "B"),
            "TxtLinea":    qs.get("Linea", "224,225"),
            "TxtZona":     qs.get("Zona", "0"),
            "Mobile":      "0",
            "BtoBuscar":   "",
        }
        resp = session.post(backend_url, params=qs, data=form, timeout=60)
        resp.raise_for_status()

        export_url = self._find_export_href(resp.text, backend_url, "Exportar")
        self.logger.info("[%s] Export URL inventario: %s", self.proveedor.display_name, export_url)

        return self._download_file(session, export_url, "inventario")

    def _find_mes_value(self, html: str) -> str:
        d = date.fromisoformat(self.proveedor.fecha_desde)
        year_str = str(d.year)
        month_str = f"{d.month:02d}"

        options = re.findall(
            r'<option[^>]+value=["\']([^"\']*)["\'][^>]*>([^<]+)<', html, re.IGNORECASE
        )
        for value, text in options:
            text = text.strip()
            if year_str in text and month_str in text:
                return value

        raise RuntimeError(
            f"Mes {year_str}-{month_str} no encontrado en el dropdown del portal."
        )

    def _find_export_href(self, html: str, backend_url: str, keyword: str) -> str:
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE)
        for href in hrefs:
            if keyword.lower() in href.lower():
                return urljoin(backend_url, href)
        for href in hrefs:
            if any(w in href.lower() for w in ("exportar", "excel", "export")):
                return urljoin(backend_url, href)
        raise RuntimeError(
            f"No se encontro link de exportacion (keyword='{keyword}') en la respuesta del portal."
        )

    def _download_file(self, session: requests.Session, url: str, tipo: str) -> Path:
        resp = session.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        cd = resp.headers.get("Content-Disposition", "")
        name_match = re.search(r'filename[^;=\n]*=(["\'"]?)([^"\';\\n]+)\1', cd)
        if name_match:
            filename = name_match.group(2).strip()
        else:
            ext = ".xlsx"
            filename = f"xeon_{tipo}_{self._extract_zona()}{ext}"

        dest = self.download_dir / filename
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)

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
                self.logger.info(
                    "[%s] Copiado a HB: %s", self.proveedor.display_name, dest
                )
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
