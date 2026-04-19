from __future__ import annotations

import logging
import os
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Sequence

from playwright.sync_api import Download
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Locator
from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from app.config import settings
from app.core.models import ExecutionResult, Proveedor
from app.portals.base_portal import BasePortal


DEFAULT_START_DATE = "2026-02-23"
DEFAULT_END_DATE = "2026-03-09"
DEFAULT_PROVIDER_FILTER_OPTIONS = ["genom", "genomma"]
SALES_ROW_FIELDS = [
    "Proveedor",
    "Codigo Articulo",
    "Descripcion Articulo",
    "Fecha",
]
INVENTORY_ROW_FIELDS = [
    "Proveedor",
    "Codigo Articulo",
    "Articulo",
]


class PortalA(BasePortal):

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

        ventas_cached = self._find_recent_download("ventas")
        inventario_cached = self._find_recent_download("inventario")

        if ventas_cached:
            self.logger.info(
                "[%s] Ventas ya descargadas en intento anterior, reutilizando: %s",
                self.proveedor.display_name, ventas_cached.name,
            )
        if inventario_cached:
            self.logger.info(
                "[%s] Inventario ya descargado en intento anterior, reutilizando: %s",
                self.proveedor.display_name, inventario_cached.name,
            )

        if ventas_cached and inventario_cached:
            names = f"{ventas_cached.name} | {inventario_cached.name}"
            self.logger.info("[%s] Ambos archivos en caché, sin necesidad de acceder al portal.", self.proveedor.display_name)
            return ExecutionResult(
                proveedor=self.proveedor.display_name,
                portal_tipo=self.proveedor.portal_tipo,
                success=True,
                needs_retry=False,
                message=f"Descargados: {names}",
                downloaded_file=ventas_cached,
                downloaded_files=[ventas_cached, inventario_cached],
            )

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self._headless_mode(), channel=settings.BROWSER_CHANNEL)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            downloaded_files: list[Path] = []
            if ventas_cached:
                downloaded_files.append(ventas_cached)
            if inventario_cached:
                downloaded_files.append(inventario_cached)
            partial_errors: list[str] = []
            login_ok = False

            try:
                login_url = self._resolve_login_url()
                self.logger.info("[%s] Abriendo URL %s", self.proveedor.display_name, login_url)
                page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
                self._login(page)
                login_ok = True

                if not ventas_cached:
                    try:
                        sales_file = self._run_sales_export(page)
                        downloaded_files.append(sales_file)
                        self.logger.info(
                            "[%s] Ventas descargadas: %s",
                            self.proveedor.display_name, sales_file.name,
                        )
                    except (PlaywrightTimeoutError, PlaywrightError, Exception) as exc:
                        partial_errors.append(f"Ventas: {exc}")
                        self.logger.error(
                            "[%s] Error al exportar ventas: %s",
                            self.proveedor.display_name, exc,
                        )
                        self._recover_to_home(page)

                if not inventario_cached:
                    try:
                        inventory_file = self._run_inventory_export(page)
                        downloaded_files.append(inventory_file)
                        self.logger.info(
                            "[%s] Inventario descargado: %s",
                            self.proveedor.display_name, inventory_file.name,
                        )
                    except (PlaywrightTimeoutError, PlaywrightError, Exception) as exc:
                        partial_errors.append(f"Inventario: {exc}")
                        self.logger.error(
                            "[%s] Error al exportar inventario: %s",
                            self.proveedor.display_name, exc,
                        )

            except (PlaywrightTimeoutError, PlaywrightError, Exception) as exc:
                if not login_ok:
                    self._take_screenshot(page, error_screenshot_path)
                    self.logger.exception("[%s] Login fallido", self.proveedor.display_name)
                    partial_errors.append(f"Login fallido: {exc}")
                else:
                    self.logger.exception(
                        "[%s] Error inesperado durante exportaciones", self.proveedor.display_name
                    )
                    partial_errors.append(str(exc))
            finally:
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass

            if not downloaded_files:
                return ExecutionResult(
                    proveedor=self.proveedor.display_name,
                    portal_tipo=self.proveedor.portal_tipo,
                    success=False,
                    message=" | ".join(partial_errors) or "Sin archivos descargados.",
                    screenshot_path=error_screenshot_path if error_screenshot_path.exists() else None,
                )

            names = " | ".join(f.name for f in downloaded_files)
            message = f"Descargados: {names}"
            if partial_errors:
                message += " | Parcial: " + " | ".join(partial_errors)
                self.logger.warning(
                    "[%s] Descarga parcial (%s/2 archivos).",
                    self.proveedor.display_name, len(downloaded_files),
                )
            else:
                self.logger.info(
                    "[%s] Exportaciones completas: %s",
                    self.proveedor.display_name, names,
                )

            return ExecutionResult(
                proveedor=self.proveedor.display_name,
                portal_tipo=self.proveedor.portal_tipo,
                success=True,
                needs_retry=bool(partial_errors),
                message=message,
                downloaded_file=downloaded_files[0],
                downloaded_files=downloaded_files,
            )

    def _find_recent_download(self, tipo: str, max_age_minutes: int = 240) -> Path | None:
        cutoff = time.time() - max_age_minutes * 60
        try:
            candidates = [
                f for f in self.download_dir.iterdir()
                if f.is_file()
                and tipo in f.name.lower()
                and f.stat().st_mtime > cutoff
            ]
        except OSError:
            return None
        return max(candidates, key=lambda f: f.stat().st_mtime) if candidates else None

    def _run_sales_export(self, page: Page) -> Path:
        self._open_ventas_netas_bi(page)
        self._open_sales_filter_modal(page)
        self._configure_sales_search(page)
        self._apply_provider_filter(page, DEFAULT_PROVIDER_FILTER_OPTIONS)
        return self._export_excel(page, tipo="ventas")

    def _run_inventory_export(self, page: Page) -> Path:
        self._open_inventory_report(page)
        self._open_inventory_filter_modal(page)
        self._configure_inventory_search(page)
        try:
            self._apply_provider_filter(page, DEFAULT_PROVIDER_FILTER_OPTIONS)
        except (RuntimeError, PlaywrightTimeoutError, PlaywrightError) as exc:
            self.logger.warning(
                "[%s] No se pudo aplicar filtro de proveedor en inventario (%s); exportando sin filtro.",
                self.proveedor.display_name, exc,
            )
        return self._export_excel(page, tipo="inventario")

    def _login(self, page: Page) -> None:
        # Algunos portales muestran seleccion de sede antes de pedir credenciales
        self._handle_branch_selection(page)

        self.logger.info("[%s] Intentando login en Abako.", self.proveedor.display_name)

        usuario_input = self._first_visible(
            page,
            [
                "input[placeholder='usuario']",
                "input[name='usuario']",
                "#cajas",
            ],
        )
        password_input = self._first_visible(
            page,
            [
                "input[placeholder='contraseña']",
                "input[placeholder='contrasena']",
                "input[name='pass']",
                "#pass",
            ],
        )
        ingresar_button = self._first_visible(
            page,
            [
                "input[type='submit']",
                "#btnfor",
                "button:has-text('INGRESAR')",
            ],
        )

        self.logger.info(
            "[%s] Ingresando credenciales (usuario='%s').",
            self.proveedor.display_name, self.proveedor.usuario,
        )
        usuario_input.fill(self.proveedor.usuario)
        password_input.fill(self.proveedor.password)
        ingresar_button.click()

        # Algunos portales muestran seleccion de sede despues de ingresar credenciales
        page.wait_for_timeout(2000)
        self._handle_branch_selection(page)

        try:
            page.locator("text=Portal Web").first.wait_for(timeout=60000)
            page.locator("text=Inicio").first.wait_for(timeout=60000)
        except PlaywrightTimeoutError:
            self.logger.error(
                "[%s] Login: timeout esperando 'Portal Web'/'Inicio'. URL actual: %s",
                self.proveedor.display_name, page.url,
            )
            raise
        self.logger.info("[%s] Login completado.", self.proveedor.display_name)

    def _handle_branch_selection(self, page: Page) -> None:
        try:
            if page.locator("text=Portal Web").first.is_visible():
                return
        except Exception:
            pass

        known_options = ["Bg Duitama", "Bg Soraca"]
        for option in known_options:
            try:
                button = page.get_by_role("button", name=option)
                if button.count() and button.first.is_visible():
                    self.logger.info(
                        "[%s] Seleccionando sede del portal: %s",
                        self.proveedor.display_name, option,
                    )
                    button.first.click()
                    page.wait_for_timeout(1500)
                    return
            except Exception:
                continue

        login_visible = False
        for sel in ("input[placeholder='usuario']", "input[name='usuario']", "#cajas"):
            try:
                if page.locator(sel).first.is_visible():
                    login_visible = True
                    break
            except Exception:
                continue
        if login_visible:
            return

        broad = page.locator(
            "button, [role='button'], input[type='submit'], input[type='button']"
        )
        try:
            count = broad.count()
        except Exception:
            return
        for i in range(count):
            try:
                el = broad.nth(i)
                if not el.is_visible():
                    continue
                box = el.bounding_box()
                if box and box["width"] < 60:
                    continue
                label = el.inner_text().strip() or el.get_attribute("value") or f"elemento #{i}"
                self.logger.info(
                    "[%s] Seleccionando sede del portal (fallback): %s",
                    self.proveedor.display_name, label,
                )
                el.click()
                page.wait_for_timeout(1500)
                return
            except Exception:
                continue

    def _open_ventas_netas_bi(self, page: Page) -> None:
        self.logger.info("[%s] Abriendo modulo Ventas Netas BI.", self.proveedor.display_name)

        page.get_by_text("Portal Web", exact=True).first.click()
        ventas_link = self._find_visible_option(page, "Ventas")
        if ventas_link is None:
            raise RuntimeError("No se encontro el enlace visible 'Ventas' en el sidebar.")
        ventas_link.hover()
        page.get_by_text("Ventas Netas BI", exact=True).first.wait_for(state="visible", timeout=15000)
        page.get_by_text("Ventas Netas BI", exact=True).first.click()

        page.locator("text=Ventas Netas Bi").first.wait_for(timeout=30000)
        page.locator("text=Filtrar").first.wait_for(timeout=30000)

    def _open_inventory_report(self, page: Page) -> None:
        self.logger.info("[%s] Abriendo reporte de inventario.", self.proveedor.display_name)

        self._retry_inventory_side_click(page)
        page.wait_for_timeout(2000)

        inventory_option = self._find_inventory_in_dropdown(page)
        if inventory_option is None:
            inventory_option = self._find_visible_option(page, "Inventario Proveedor", min_x=250)
        if inventory_option is None:
            inventory_option = self._find_visible_option(page, "Inventario", min_x=250)
        if inventory_option is None:
            raise RuntimeError("No se encontro la opcion 'Inventario' dentro del mega menu.")

        inventory_option.click(force=True)
        page.wait_for_timeout(1200)

        self._ensure_dropdown_closed(page)

        page.locator("text=Portal Web").first.wait_for(timeout=30000)
        page.locator("text=Saldos").first.wait_for(timeout=30000)
        page.locator("text=Filtrar").first.wait_for(timeout=30000)

    def _open_sales_filter_modal(self, page: Page) -> None:
        self.logger.info("[%s] Abriendo modal de filtros de ventas.", self.proveedor.display_name)
        page.get_by_text("Filtrar", exact=True).first.click()
        page.locator("text=Filtros").first.wait_for(timeout=30000)
        page.locator("text=Fecha Inicial*").first.wait_for(timeout=30000)
        page.locator("text=Fecha Final*").first.wait_for(timeout=30000)
        page.locator("button.btn.btn-success").last.wait_for(timeout=30000)

    def _open_inventory_filter_modal(self, page: Page) -> None:
        self.logger.info("[%s] Abriendo modal de filtros de inventario.", self.proveedor.display_name)
        page.get_by_text("Filtrar", exact=True).first.click()
        page.locator("text=Filtros").first.wait_for(timeout=30000)
        page.locator("li.tab").filter(has_text="Campos").wait_for(timeout=30000)
        page.locator("button.btn.btn-success").last.wait_for(timeout=30000)

    def _retry_inventory_side_click(self, page: Page) -> Locator:
        attempts = 0
        while attempts < 3:
            option = self._find_visible_option(page, "Inventario", max_x=250)
            if option:
                option.click()
                return option

            self.logger.debug(
                "[%s] No se encontro el enlace lateral 'Inventario', reintentando (%s/3).",
                self.proveedor.display_name, attempts + 1,
            )
            self._dismiss_portal_menu(page)
            page.get_by_text("Portal Web", exact=True).first.click()
            page.locator("text=Inventario").first.wait_for(state="visible", timeout=10000)
            attempts += 1

        raise RuntimeError("No se encontro la opcion lateral 'Inventario' tras varios intentos.")

    def _dismiss_portal_menu(self, page: Page) -> None:
        page.keyboard.press("Escape")
        page.mouse.click(10, 10)
        page.wait_for_timeout(500)

    def _configure_sales_search(self, page: Page) -> None:
        start_date, end_date = self._resolve_date_range()
        self.logger.info(
            "[%s] Configurando filtros de ventas: %s a %s",
            self.proveedor.display_name, start_date, end_date,
        )

        page.get_by_label("Fecha Inicial").fill(start_date)
        page.get_by_label("Fecha Final").fill(end_date)
        page.locator("li.tab").filter(has_text="Campos").click()
        page.locator("#fields").wait_for(state="visible", timeout=15000)
        page.wait_for_timeout(500)

        rows = page.locator("#rows")
        fields = page.locator("#fields")
        self._clear_drop_zone(page, rows, fields)

        for label in SALES_ROW_FIELDS:
            self._move_field_to_zone(page, label, rows)

        page.locator("button.btn.btn-success").last.click()
        self._wait_for_grid_load(page, timeout_ms=90000)
        page.locator("text=Exportar").first.wait_for(timeout=30000)

    def _configure_inventory_search(self, page: Page) -> None:
        self.logger.info("[%s] Configurando campos de inventario.", self.proveedor.display_name)

        # Intentar aplicar fecha de corte en el tab Filtros (si el portal lo soporta).
        # Algunos portales Abako muestran "Fecha Inicial"/"Fecha Final" también en inventario;
        # otros solo "Fecha de Corte". Se intenta de forma no-fatal para no romper portales
        # que no tengan el campo.
        _, end_date = self._resolve_date_range()
        try:
            fecha_corte = page.get_by_label("Fecha de Corte")
            if fecha_corte.count() and fecha_corte.first.bounding_box():
                fecha_corte.first.fill(end_date)
                self.logger.info("[%s] Fecha de corte inventario: %s", self.proveedor.display_name, end_date)
            else:
                fecha_ini = page.get_by_label("Fecha Inicial")
                fecha_fin = page.get_by_label("Fecha Final")
                if fecha_ini.count() and fecha_ini.first.bounding_box():
                    fecha_ini.first.fill(end_date)
                    fecha_fin.first.fill(end_date)
                    self.logger.info("[%s] Fecha inventario (ini=fin): %s", self.proveedor.display_name, end_date)
        except Exception as exc:
            self.logger.debug("[%s] Sin filtro de fecha en inventario: %s", self.proveedor.display_name, exc)

        page.locator("li.tab").filter(has_text="Campos").click()
        page.locator("#fields").wait_for(state="visible", timeout=15000)
        page.wait_for_timeout(500)

        rows = page.locator("#rows")
        fields = page.locator("#fields")
        columns = page.locator("#columns")
        self._clear_drop_zone(page, rows, fields)
        self._clear_drop_zone(page, columns, fields)

        for label in INVENTORY_ROW_FIELDS:
            self._move_field_to_zone(page, label, rows)

        page.locator("button.btn.btn-success").last.click()
        self._wait_for_grid_load(page, timeout_ms=90000)
        page.locator("text=Exportar").first.wait_for(timeout=30000)

    def _apply_provider_filter(self, page: Page, provider_candidates: Sequence[str]) -> None:
        candidates = [candidate.strip() for candidate in provider_candidates if candidate.strip()]
        if not candidates:
            raise ValueError("Se requiere al menos un nombre de proveedor para aplicar el filtro.")
        self.logger.info(
            "[%s] Aplicando filtro de proveedor: %s",
            self.proveedor.display_name, ", ".join(candidates),
        )

        try:
            page.get_by_text("Proveedor", exact=True).first.wait_for(state="visible", timeout=60000)
        except PlaywrightTimeoutError:
            pass

        provider_headers = self._visible_text_locators(page, "Proveedor")
        if not provider_headers:
            raise RuntimeError("No se encontro la cabecera 'Proveedor' en el grid.")

        provider_header = provider_headers[-1]
        filter_icon = provider_header.locator("xpath=..").locator(".dx-header-filter")
        if filter_icon.count():
            filter_icon.first.click(force=True)
        else:
            provider_box = provider_header.bounding_box()
            if provider_box is None:
                raise RuntimeError("No se pudo calcular la posicion del filtro de Proveedor.")
            page.mouse.click(
                provider_box["x"] + 75,
                provider_box["y"] + provider_box["height"] / 2,
            )
        page.wait_for_timeout(1500)

        visible_inputs = self._visible_text_inputs(page)
        if not visible_inputs:
            raise RuntimeError("No se abrio el popup del filtro de proveedor.")

        visible_input = visible_inputs[-1]
        provider_option: Locator | None = None
        last_error: RuntimeError | None = None
        for candidate in candidates:
            normalized = candidate.strip()
            if not normalized:
                continue

            visible_input.fill(normalized.lower())
            self._wait_for_filter_results(page)

            try:
                provider_option = self._select_provider_option(page, candidate)
                break
            except RuntimeError as exc:
                last_error = exc
                self.logger.debug(
                    "[%s] No se encontro '%s'; intento siguiente.",
                    self.proveedor.display_name, candidate,
                )
                visible_input.fill("")
                page.wait_for_timeout(600)

        if provider_option is None:
            raise last_error or RuntimeError(
                "No se encontro ningun proveedor con las cadenas solicitadas."
            )

        provider_option.click()
        self._click_modal_confirm_button(page)
        self._wait_for_grid_load(page, timeout_ms=60000)

    def _select_provider_option(self, page: Page, provider_name: str) -> Locator:
        search_name = provider_name

        candidate = self._find_provider_locator(page, provider_name)
        while candidate is None and len(search_name) > 3:
            search_name = search_name[:-1]
            self.logger.debug(
                "[%s] No se encontro '%s'; intentando con '%s'.",
                self.proveedor.display_name, provider_name, search_name,
            )
            candidate = self._find_provider_locator(page, search_name)

        if candidate is None:
            raise RuntimeError(f"No se encontro ningun proveedor coincidente con '{provider_name}'.")
        return candidate

    def _click_modal_confirm_button(self, page: Page) -> None:
        button = self._wait_for_confirm_button(page)
        self.logger.info(
            "[%s] Pulsando boton de confirmacion del filtro (%s).",
            self.proveedor.display_name, self._safe_button_text(button),
        )
        button.click()

    def _wait_for_confirm_button(self, page: Page, timeout_ms: int = 12000) -> Locator:
        deadline = time.time() + timeout_ms / 1000
        last_message = ""
        while time.time() < deadline:
            button = self._find_visible_confirm_button(page)
            if button is not None:
                return button
            remaining = int((deadline - time.time()) * 1000)
            last_message = f"No se encontro boton de confirmacion (restan {remaining} ms)."
            self.logger.debug("[%s] %s", self.proveedor.display_name, last_message)
            page.wait_for_timeout(500)
        raise RuntimeError("No se encontro boton de confirmacion en el modal de filtros.")

    def _find_visible_confirm_button(self, page: Page) -> Locator | None:
        keywords = {"ok", "aceptar", "confirmar"}
        buttons = page.locator("button, [role='button']")
        total = buttons.count()
        for index in range(total):
            button = buttons.nth(index)
            try:
                box = button.bounding_box()
            except PlaywrightError:
                continue
            if box is None:
                continue
            text = self._safe_button_text(button)
            if not text:
                continue
            normalized = self._normalize_text(text)
            if normalized in keywords:
                try:
                    if not button.is_enabled():
                        continue
                except PlaywrightError:
                    pass
                return button
        return None

    @staticmethod
    def _find_provider_locator(page: Page, text: str) -> Locator | None:
        matches = page.get_by_text(text, exact=False)
        if matches.count() == 0:
            return None
        locator = matches.last
        try:
            locator.wait_for(timeout=2000)
            return locator
        except PlaywrightTimeoutError:
            return None

    def _export_excel(self, page: Page, tipo: str = "") -> Path:
        self.logger.info("[%s] Exportando archivo Excel.", self.proveedor.display_name)

        with page.expect_download(timeout=30000) as download_info:
            page.get_by_text("Exportar", exact=True).first.click()
            page.get_by_text("Excel", exact=True).first.click()

        return self._save_download(download_info.value, tipo=tipo)

    def _save_download(self, download: Download, tipo: str = "") -> Path:
        original = download.suggested_filename
        if tipo:
            p = Path(original)
            original = f"{p.stem}_{tipo}{p.suffix}"
        target_path = self.download_dir / original
        download.save_as(target_path)
        return target_path

    def _clear_drop_zone(self, page: Page, zone: Locator, fallback_target: Locator) -> None:
        items = zone.locator(".example-box-custom")
        while items.count():
            self._drag_to_target(page, items.first, fallback_target)
            items = zone.locator(".example-box-custom")

    def _move_field_to_zone(self, page: Page, label: str, target_zone: Locator) -> None:
        self._drag_to_target(page, self._field_locator(page, label), target_zone)

    def _field_locator(self, page: Page, label: str) -> Locator:
        items = page.locator("#fields .box-drag")
        total = items.count()
        target = self._normalize_text(label)

        for index in range(total):
            item = items.nth(index)
            text = item.locator("span.col-lg-10").inner_text().strip()
            if self._normalize_text(text) == target:
                return item

        raise RuntimeError(f"No se encontro el campo '{label}' en la lista de CAMPOS.")

    def _drag_to_target(self, page: Page, source: Locator, target: Locator) -> None:
        source.scroll_into_view_if_needed()
        page.wait_for_timeout(400)

        source_box = source.bounding_box()
        target_box = target.bounding_box()
        if source_box is None or target_box is None:
            raise RuntimeError("No fue posible calcular coordenadas para drag and drop.")

        page.mouse.move(
            source_box["x"] + source_box["width"] / 2,
            source_box["y"] + source_box["height"] / 2,
        )
        page.mouse.down()
        page.mouse.move(
            target_box["x"] + target_box["width"] / 2,
            target_box["y"] + min(20, target_box["height"] / 2),
            steps=30,
        )
        page.mouse.up()
        page.wait_for_timeout(1800)

    def _resolve_date_range(self) -> tuple[str, str]:
        start_date = self._normalize_date_value(self.proveedor.fecha_desde) or DEFAULT_START_DATE
        end_date = self._normalize_date_value(self.proveedor.fecha_hasta) or DEFAULT_END_DATE
        return start_date, end_date

    @staticmethod
    def _normalize_date_value(value: str) -> str:
        value = value.strip()
        if not value:
            return ""

        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return value

    @staticmethod
    def _visible_text_inputs(page: Page) -> list[Locator]:
        inputs = page.locator("input[type='text']")
        return [inputs.nth(index) for index in range(inputs.count()) if inputs.nth(index).bounding_box() is not None]

    @staticmethod
    def _visible_text_locators(page: Page, text: str) -> list[Locator]:
        matches = page.get_by_text(text, exact=True)
        return [
            matches.nth(index)
            for index in range(matches.count())
            if matches.nth(index).bounding_box() is not None
        ]

    @staticmethod
    def _visible_anchor_locators(page: Page, text: str) -> list[Locator]:
        anchors = page.locator("a")
        matches: list[Locator] = []
        for index in range(anchors.count()):
            anchor = anchors.nth(index)
            try:
                if anchor.bounding_box() is None:
                    continue
                if anchor.inner_text().strip() == text:
                    matches.append(anchor)
            except Exception:
                continue
        return matches

    @classmethod
    def _find_visible_option(
        cls,
        page: Page,
        text: str,
        min_x: float | None = None,
        max_x: float | None = None,
    ) -> Locator | None:
        candidates = cls._visible_anchor_locators(page, text) + cls._visible_text_locators(page, text)
        for option in candidates:
            box = option.bounding_box()
            if box is None:
                continue
            if min_x is not None and box["x"] <= min_x:
                continue
            if max_x is not None and box["x"] >= max_x:
                continue
            return option
        return None

    @staticmethod
    def _first_visible(page: Page, selectors: list[str]) -> Locator:
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                locator.wait_for(state="visible", timeout=5000)
                return locator
            except PlaywrightTimeoutError:
                continue
        raise PlaywrightTimeoutError(f"No se encontro ningun selector visible: {selectors}")

    @staticmethod
    def _normalize_text(value: str) -> str:
        text = value.strip().lower()
        return "".join(
            char
            for char in unicodedata.normalize("NFKD", text)
            if not unicodedata.combining(char)
        )

    @staticmethod
    def _safe_button_text(button: Locator) -> str:
        try:
            text = button.inner_text().strip()
            if text:
                return text
        except PlaywrightError:
            pass
        try:
            label = button.get_attribute("aria-label")
            if label:
                return label.strip()
        except PlaywrightError:
            pass
        return ""

    @staticmethod
    def _headless_mode() -> bool:
        raw_value = os.getenv("RPA_HEADLESS", "0").strip().lower()
        return raw_value in {"1", "true", "yes", "si"}

    def _resolve_login_url(self) -> str:
        url = self.proveedor.login_url.strip()
        if not url:
            return url
        lower_url = url.lower()
        if lower_url.endswith("/home"):
            return f"{url.rsplit('/', 1)[0]}/login"
        if lower_url.endswith("/portalabakoerp"):
            return f"{url}/login"
        if lower_url.endswith("/portalabakoerp/"):
            return f"{url}login"
        return url

    def _find_inventory_in_dropdown(self, page: Page) -> Locator | None:
        try:
            saldos_loc = page.get_by_text("Saldos", exact=True).first
            saldos_box = saldos_loc.bounding_box()
            if saldos_box is None:
                return None

            candidates = (
                page.get_by_text("Inventario Proveedor", exact=True).all()
                or page.get_by_text("Inventario", exact=True).all()
            )
            best: Locator | None = None
            best_dist = float("inf")
            for candidate in candidates:
                box = candidate.bounding_box()
                if box is None:
                    continue
                if box["y"] < 100:
                    continue
                if abs(box["x"] - saldos_box["x"]) > 80:
                    continue
                dist = abs(box["y"] - saldos_box["y"])
                if dist < best_dist:
                    best_dist = dist
                    best = candidate
            return best
        except Exception:
            return None

    def _ensure_dropdown_closed(self, page: Page) -> None:
        try:
            menu_indicators = [
                page.get_by_text("Sugerido", exact=True).first,
                page.get_by_text("Inventario Proveedor", exact=True).first,
            ]
            menu_open = any(loc.is_visible() for loc in menu_indicators)
            if menu_open:
                self.logger.debug(
                    "[%s] Mega menú sigue abierto tras navegación; cerrando.",
                    self.proveedor.display_name,
                )
                page.keyboard.press("Escape")
                page.wait_for_timeout(400)
                page.mouse.click(740, 35)
                page.wait_for_timeout(600)
        except Exception:
            pass

    def _wait_for_grid_load(self, page: Page, timeout_ms: int = 90000) -> None:
        load_selector = (
            ".dx-loadpanel-wrapper, .dx-datagrid-load-panel, .dx-loadindicator-icon"
        )
        try:
            page.wait_for_selector(load_selector, state="visible", timeout=8000)
            self.logger.debug("[%s] Loading indicator detectado, esperando fin de carga.", self.proveedor.display_name)
            page.wait_for_selector(load_selector, state="hidden", timeout=timeout_ms)
            page.wait_for_timeout(1500)
        except PlaywrightTimeoutError:
            page.wait_for_timeout(6000)

    def _wait_for_filter_results(self, page: Page, timeout_ms: int = 20000) -> None:
        filter_list_selector = ".dx-list-item, .dx-checkbox-container, .dx-filterbuilder-item"
        try:
            page.wait_for_selector(filter_list_selector, state="visible", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            page.wait_for_timeout(3000)

    def _recover_to_home(self, page: Page) -> None:
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(600)
        except Exception:
            pass
        try:
            page.get_by_text("Inicio", exact=True).first.click(timeout=5000)
            page.wait_for_timeout(2000)
        except Exception:
            try:
                page.mouse.click(10, 10)
                page.wait_for_timeout(1000)
            except Exception:
                pass

    def _take_screenshot(self, page: Page, path: Path) -> None:
        try:
            page.screenshot(path=str(path), full_page=True)
        except Exception:
            self.logger.debug(
                "[%s] No fue posible guardar screenshot.", self.proveedor.display_name,
            )

    def _build_screenshot_path(self, suffix: str) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = self.proveedor.display_name.replace(" ", "_")
        filename = f"{base_name}_{suffix}_{timestamp}.png"
        return self.screenshot_dir / filename
