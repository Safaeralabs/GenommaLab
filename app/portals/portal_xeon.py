"""Xeon TAT/Mensuli portal — requests-based (no browser needed)."""

from __future__ import annotations

import logging
import re
import shutil
import socket
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import requests

from app.config import settings
from app.core.models import ExecutionResult, Proveedor
from app.portals.base_portal import BasePortal

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


class PortalXeon(BasePortal):
    """Xeon workflow: login → Paretto de Ventas → Lista de Precios."""

    def __init__(
        self,
        proveedor: Proveedor,
        download_dir: Path,
        screenshot_dir: Path,
        logger: logging.Logger,
    ) -> None:
        super().__init__(proveedor, download_dir, screenshot_dir)
        self.logger = logger

    # ── Punto de entrada ──────────────────────────────────────────────────────

    def ejecutar(self) -> ExecutionResult:
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Verificar VPN antes de cualquier llamada
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

        session = requests.Session()
        session.headers.update({"User-Agent": _UA})

        try:
            self._login(session)

            backend_url, qs = self._resolve_backend_url(session, "paretto")
            ventas_path = self._download_ventas(session, backend_url, qs)

            backend_inv_url, qs_inv = self._resolve_backend_url(session, "listaprecios")
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

    # ── Login ─────────────────────────────────────────────────────────────────

    def _login(self, session: requests.Session) -> None:
        url = self._base_url()
        self.logger.info("[%s] Login en %s", self.proveedor.display_name, url)

        # GET primero para obtener cookies de sesión y campos ocultos del form
        resp = session.get(url, timeout=30)
        resp.raise_for_status()

        # Extraer campos hidden del formulario de login (si los hay)
        hidden = dict(re.findall(
            r'<input[^>]+type=["\']hidden["\'][^>]+name=["\']([^"\']+)["\'][^>]+value=["\']([^"\']*)["\']',
            resp.text, re.IGNORECASE,
        ))

        post_data = {
            **hidden,
            "username": self.proveedor.usuario,
            "password": self.proveedor.password,
        }

        resp = session.post(url, data=post_data, allow_redirects=True, timeout=30)
        resp.raise_for_status()

        # Detectar login fallido: si el campo "username" sigue presente, no entramos
        if re.search(r'<input[^>]+name=["\']username["\']', resp.text, re.IGNORECASE):
            raise RuntimeError("Login fallido: credenciales incorrectas o el portal no aceptó la sesión.")

        self.logger.info("[%s] Login OK.", self.proveedor.display_name)

    # ── Resolución de URL de backend ──────────────────────────────────────────

    def _resolve_backend_url(
        self, session: requests.Session, view: str
    ) -> tuple[str, dict[str, str]]:
        """GET frontend home.php?view=<view> y extrae el iframe src con los parámetros de usuario."""
        frontend = self._base_url() + f"home.php?view={view}"
        resp = session.get(frontend, timeout=30)
        resp.raise_for_status()

        match = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\']', resp.text, re.IGNORECASE)
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

    # ── Ventas (Paretto) ──────────────────────────────────────────────────────

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

        # 1. GET página para obtener opciones de mes
        resp = session.get(backend_url, params=qs, timeout=30)
        resp.raise_for_status()
        mes_value = self._find_mes_value(resp.text)
        self.logger.info("[%s] Mes seleccionado: %s", self.proveedor.display_name, mes_value)

        # 2. POST formulario con fechas y mes
        form = {
            "LstMes":      mes_value,
            "TxtFecIni":   self.proveedor.fecha_desde,   # YYYY-MM-DD
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

        # 3. Encontrar link de exportar Excel en la respuesta
        export_url = self._find_export_href(resp.text, backend_url, "ParettoExportar")
        self.logger.info("[%s] Export URL ventas: %s", self.proveedor.display_name, export_url)

        return self._download_file(session, export_url, "ventas")

    # ── Inventario (Lista de Precios) ─────────────────────────────────────────

    def _download_inventario(
        self,
        session: requests.Session,
        backend_url: str,
        qs: dict[str, str],
    ) -> Path:
        self.logger.info("[%s] Descargando inventario (Lista de Precios).", self.proveedor.display_name)

        # 1. GET página
        resp = session.get(backend_url, params=qs, timeout=30)
        resp.raise_for_status()

        # 2. POST formulario (buscar todos los productos)
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

        # 3. Encontrar link de exportar Excel
        export_url = self._find_export_href(resp.text, backend_url, "Exportar")
        self.logger.info("[%s] Export URL inventario: %s", self.proveedor.display_name, export_url)

        return self._download_file(session, export_url, "inventario")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _find_mes_value(self, html: str) -> str:
        """Busca en el HTML el value del <option> que corresponde al mes de fecha_desde."""
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
        """Localiza el href del link de exportar Excel en el HTML de resultados."""
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE)
        # Primero buscar por keyword exacto
        for href in hrefs:
            if keyword.lower() in href.lower():
                return urljoin(backend_url, href)
        # Fallback: cualquier link que mencione exportar/excel
        for href in hrefs:
            if any(w in href.lower() for w in ("exportar", "excel", "export")):
                return urljoin(backend_url, href)
        raise RuntimeError(
            f"No se encontro link de exportacion (keyword='{keyword}') en la respuesta del portal."
        )

    def _download_file(
        self, session: requests.Session, url: str, tipo: str
    ) -> Path:
        """Descarga el archivo desde url y lo guarda en download_dir."""
        resp = session.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        # Intentar nombre desde Content-Disposition
        cd = resp.headers.get("Content-Disposition", "")
        name_match = re.search(r'filename[^;=\n]*=(["\']?)([^"\';\n]+)\1', cd)
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

    # ── Utilidades ────────────────────────────────────────────────────────────

    def _base_url(self) -> str:
        url = self.proveedor.login_url.strip()
        if not url.startswith(("http://", "https://")):
            url = "http://" + url
        if not url.endswith("/"):
            url += "/"
        # Solo añadir /tat_nuevo/ si es IP sin path
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
