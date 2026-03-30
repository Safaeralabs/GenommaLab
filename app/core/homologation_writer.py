"""Consolidate sales/inventory rows into the shared Homologaciones workbook."""

from __future__ import annotations

import csv
import logging
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font

from app.config import settings
from app.core.models import HomologationSummary, OrganizedFile, Proveedor


HOMOLOGATION_COLUMNS = [
    "año",
    "Semana",
    "Tipo",
    "Fecha_Stock",
    "Cadena",
    "Cod_Prod",
    "Descripcion_prod",
    "Cod_Local",
    "Descripcion_Local",
    "Unidades",
    "Zonalocal",
]

# Column index (1-based) for the "Cadena" column in the data sheet.
_CADENA_COL_IDX = 5  # col E


@dataclass(slots=True)
class HomologationRow:
    tipo: str
    cod_prod: str
    descripcion: str
    unidades: str
    cadena: str
    cod_local: str
    descripcion_local: str
    zonalocal: str


class HomologationWriter:
    """Build a consolidated Homologaciones file from template + collected rows."""

    SALES_MAPPING = {
        "codigoarticulo": "Cod_Prod",
        "codigoproducto": "Cod_Prod",
        "codprod": "Cod_Prod",
        "descripcionarticulo": "Descripcion_prod",
        "descripcionproducto": "Descripcion_prod",
        "descripcion": "Descripcion_prod",
        "unidades": "Unidades",
        "cantidad": "Unidades",
        "ean": "Cod_Prod",
        "nombrepr": "Descripcion_prod",       # NOMBRE_PR normalized
        "nombreproducto": "Descripcion_prod", # NOMBRE_PRODUCTO (EOS)
        "ventasunidad": "Unidades",           # ventas unidad normalized
        "ventaunidad": "Unidades",            # alternate
        "ventaunidades": "Unidades",          # VENTA_UNIDADES (EOS)
    }

    INVENTORY_MAPPING = {
        "codigoarticulo": "Cod_Prod",
        "codigoproducto": "Cod_Prod",
        "articulo": "Descripcion_prod",
        "producto": "Descripcion_prod",
        "disponible": "Unidades",
        "existencia": "Unidades",
        # Aliases EOS Consultores (inventario derivado del delta de ventas)
        "nombreproducto": "Descripcion_prod",
        "ventaunidades": "Unidades",
        "ventaunidad": "Unidades",
    }

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.template_path = settings.TEMPLATE_HOMOLOGACION

    def collect_rows(
        self,
        proveedor: Proveedor,
        organized_files: list[OrganizedFile],
    ) -> list[HomologationRow]:
        rows: list[HomologationRow] = []
        for item in organized_files:
            mapping = (
                self.SALES_MAPPING if item.category == "ventas" else self.INVENTORY_MAPPING
            )
            entries = self._extract_entries(item.path, mapping)
            tipo = "SO" if item.category == "ventas" else "INV"
            for cod, desc, unidades in entries:
                rows.append(
                    HomologationRow(
                        tipo=tipo,
                        cod_prod=cod,
                        descripcion=desc,
                        unidades=unidades,
                        cadena=proveedor.cadena or proveedor.proveedor,
                        cod_local="",
                        descripcion_local=proveedor.sede_subportal or proveedor.display_name,
                        zonalocal=proveedor.sede_subportal or "",
                    )
                )
        return rows

    def write(
        self,
        rows: list[HomologationRow],
        year: int,
        week: int,
        fecha_stock: str,
        total_providers: int = 0,
        missing_providers: list[str] | None = None,
    ) -> HomologationSummary:
        if missing_providers is None:
            missing_providers = []

        filename = f"Homologaciones_S{week:02d}_{year}.xlsx"
        target_path = settings.POSTPROCESSED_DIR / filename

        # Build a set of (cadena, tipo) pairs being updated so we only replace
        # the specific types provided — partial re-runs don't erase the other type.
        updating_cadena_tipos: set[tuple[str, str]] = {(row.cadena, row.tipo) for row in rows}
        updating_cadenas = {row.cadena for row in rows}

        if target_path.exists():
            workbook = load_workbook(target_path)
            # Get the data sheet (prefer "homologacion", fall back to active)
            if "homologacion" in workbook.sheetnames:
                sheet = workbook["homologacion"]
            else:
                sheet = workbook.active

            # Keep rows whose cadena is not being updated, OR whose cadena is
            # being updated but its tipo is NOT included in the new data.
            # This allows partial re-runs (e.g. only ventas) to preserve
            # previously loaded data of the other type (e.g. inventario).
            kept_rows: list[tuple] = []
            for row_values in sheet.iter_rows(min_row=2, values_only=True):
                if not row_values or all(v is None for v in row_values):
                    continue
                existing_cadena = row_values[_CADENA_COL_IDX - 1]
                existing_tipo = row_values[2]  # Tipo is column C (index 2, 0-based)
                if (existing_cadena, existing_tipo) not in updating_cadena_tipos:
                    kept_rows.append(row_values)

            # Clear all rows after the header, then rewrite kept + new rows
            max_row = sheet.max_row
            if max_row > 1:
                sheet.delete_rows(2, max_row - 1)

            for kept in kept_rows:
                sheet.append(list(kept))
        else:
            workbook = Workbook()
            sheet = workbook.active
            sheet.title = "homologacion"
            sheet.append(HOMOLOGATION_COLUMNS)

        # Write new rows
        for row in rows:
            sheet.append(
                [
                    str(year),
                    str(week),
                    row.tipo,
                    fecha_stock,
                    row.cadena,
                    row.cod_prod,
                    row.descripcion,
                    row.cod_local,
                    row.descripcion_local,
                    row.unidades,
                    row.zonalocal,
                ]
            )

        # ── Build "Resumen" sheet ─────────────────────────────────────────────
        # Read all rows now in the data sheet to build the summary
        all_rows_in_sheet: list[tuple] = []
        for row_values in sheet.iter_rows(min_row=2, values_only=True):
            if not row_values or all(v is None for v in row_values):
                continue
            all_rows_in_sheet.append(row_values)

        # Delete existing Resumen sheet if present
        if "Resumen" in workbook.sheetnames:
            del workbook["Resumen"]

        resumen = workbook.create_sheet("Resumen")
        resumen_header = ["Proveedor", "Ventas (SO)", "Inventario (INV)", "Estado"]
        resumen.append(resumen_header)
        # Bold header
        for cell in resumen[1]:
            cell.font = Font(bold=True)

        # Gather per-cadena info from data sheet rows
        # Cadena = col index 4 (0-based), Tipo = col index 2 (0-based)
        cadena_tipos: dict[str, set[str]] = {}
        for rv in all_rows_in_sheet:
            cadena_val = rv[_CADENA_COL_IDX - 1]
            tipo_val = rv[2]  # "Tipo" is column C (index 2, 0-based)
            if cadena_val is None:
                continue
            cadena_key = str(cadena_val)
            cadena_tipos.setdefault(cadena_key, set()).add(str(tipo_val) if tipo_val else "")

        for cadena_key, tipos in sorted(cadena_tipos.items()):
            has_so = "SO" in tipos
            has_inv = "INV" in tipos
            ventas_val = "✓" if has_so else "—"
            inv_val = "✓" if has_inv else "—"
            if has_so and has_inv:
                estado = "Completo"
            elif has_so or has_inv:
                estado = "Parcial"
            else:
                estado = "Faltante"
            resumen.append([cadena_key, ventas_val, inv_val, estado])

        # Add rows for missing providers (ran but produced no data rows)
        for missing_name in missing_providers:
            if missing_name not in cadena_tipos:
                resumen.append([missing_name, "—", "—", "Faltante"])

        settings.POSTPROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        workbook.save(target_path)
        workbook.close()

        all_cadenas = set(cadena_tipos.keys())
        included = len(all_cadenas)

        self.logger.info(
            "Homologación acumulativa S%s/%s: %s (filas nuevas: %s, proveedores incluidos: %s/%s)",
            week,
            year,
            target_path,
            len(rows),
            included,
            total_providers,
        )

        return HomologationSummary(
            path=target_path,
            included_providers=included,
            total_providers=total_providers,
            missing_providers=missing_providers,
        )

    def _ensure_template(self) -> None:
        if self.template_path.exists():
            return
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "homologacion"
        sheet.append(HOMOLOGATION_COLUMNS)
        workbook.save(self.template_path)
        workbook.close()

    def _ensure_header(self, sheet) -> None:
        header = [cell.value for cell in sheet[1]]
        if header and all(h in header for h in HOMOLOGATION_COLUMNS):
            return
        sheet.insert_rows(1)
        for col_idx, column in enumerate(HOMOLOGATION_COLUMNS, start=1):
            sheet.cell(row=1, column=col_idx, value=column)

    def _extract_entries(
        self,
        file_path: Path,
        mapping: dict[str, str],
    ) -> list[tuple[str, str, str]]:
        if file_path.suffix.lower() == ".csv":
            return self._extract_entries_csv(file_path, mapping)
        if file_path.suffix.lower() == ".xls":
            return self._extract_entries_xls(file_path, mapping)
        return self._extract_entries_xlsx(file_path, mapping)

    def _extract_entries_xlsx(
        self,
        file_path: Path,
        mapping: dict[str, str],
    ) -> list[tuple[str, str, str]]:
        workbook = load_workbook(file_path, read_only=True, data_only=True)
        sheet = workbook.active
        header_row_index, headers = self._find_header_row(sheet, mapping)
        if header_row_index is None:
            workbook.close()
            return []

        lookup: dict[str, int] = {}
        for idx, raw_label in enumerate(headers):
            normalized_label = self._normalize_text(raw_label)
            target = mapping.get(normalized_label)
            if target:
                lookup[target] = idx

        required = {"Cod_Prod", "Descripcion_prod", "Unidades"}
        if not required.issubset(lookup):
            workbook.close()
            return []

        entries: list[tuple[str, str, str]] = []
        for row in sheet.iter_rows(min_row=header_row_index + 1, values_only=True):
            if not row:
                continue
            cod = row[lookup["Cod_Prod"]]
            desc = row[lookup["Descripcion_prod"]]
            unidades = row[lookup["Unidades"]]
            if not cod or not desc:
                continue
            entries.append((str(cod).strip(), str(desc).strip(), str(unidades or "").strip()))

        workbook.close()
        return entries

    def _extract_entries_csv(
        self,
        file_path: Path,
        mapping: dict[str, str],
    ) -> list[tuple[str, str, str]]:
        """Read a CSV file and extract homologation entries.

        Dots in numeric fields (decimal/thousands separators) are replaced
        with commas to match the expected homologation format.
        """
        for encoding in ("utf-8-sig", "latin-1", "utf-8"):
            try:
                with open(file_path, newline="", encoding=encoding) as f:
                    sample = f.read(4096)
                    f.seek(0)
                    try:
                        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                    except csv.Error:
                        dialect = csv.excel  # type: ignore[assignment]
                    reader = csv.reader(f, dialect)
                    all_rows = list(reader)
                break
            except UnicodeDecodeError:
                continue
        else:
            return []

        if not all_rows:
            return []

        # Find header row (same logic as xlsx: first row matching required targets)
        required = {"Cod_Prod", "Descripcion_prod", "Unidades"}
        header_idx = None
        lookup: dict[str, int] = {}
        for row_idx, row in enumerate(all_rows[:10]):
            found: set[str] = set()
            candidate_lookup: dict[str, int] = {}
            for col_idx, raw_label in enumerate(row):
                norm = self._normalize_text(str(raw_label))
                target = mapping.get(norm)
                if target:
                    found.add(target)
                    candidate_lookup[target] = col_idx
            if required.issubset(found):
                header_idx = row_idx
                lookup = candidate_lookup
                break

        if header_idx is None:
            return []

        entries: list[tuple[str, str, str]] = []
        for row in all_rows[header_idx + 1:]:
            if not row:
                continue
            try:
                cod = row[lookup["Cod_Prod"]].strip()
                desc = row[lookup["Descripcion_prod"]].strip()
                unidades_raw = row[lookup["Unidades"]].strip()
                # Reemplazar puntos por comas en el campo numérico de unidades
                unidades = unidades_raw.replace(".", ",")
            except IndexError:
                continue
            if not cod or not desc:
                continue
            entries.append((cod, desc, unidades))

        return entries

    def _find_header_row(
        self,
        sheet,
        mapping: dict[str, str],
    ) -> tuple[int | None, list[str]]:
        """Locate the first row that can serve as the header for the mapping."""
        required_targets = {"Cod_Prod", "Descripcion_prod", "Unidades"}
        for row_index in range(1, min(10, sheet.max_row) + 1):
            row = [cell.value or "" for cell in sheet[row_index]]
            found_targets: set[str] = set()
            for raw_label in row:
                normalized_label = self._normalize_text(str(raw_label))
                target = mapping.get(normalized_label)
                if target:
                    found_targets.add(target)
            if required_targets.issubset(found_targets):
                return row_index, [str(cell) for cell in row]
        return None, []

    def _extract_entries_xls(
        self,
        file_path: Path,
        mapping: dict[str, str],
    ) -> list[tuple[str, str, str]]:
        """Read a legacy .xls file using xlrd and extract homologation entries."""
        import xlrd  # type: ignore[import]

        try:
            wb = xlrd.open_workbook(str(file_path))
        except Exception:
            return []

        sheet = wb.sheet_by_index(0)
        required = {"Cod_Prod", "Descripcion_prod", "Unidades"}

        header_idx = None
        lookup: dict[str, int] = {}
        for row_idx in range(min(10, sheet.nrows)):
            row = [str(sheet.cell_value(row_idx, c)) for c in range(sheet.ncols)]
            found: set[str] = set()
            candidate: dict[str, int] = {}
            for col_idx, raw_label in enumerate(row):
                norm = self._normalize_text(raw_label)
                target = mapping.get(norm)
                if target:
                    found.add(target)
                    candidate[target] = col_idx
            if required.issubset(found):
                header_idx = row_idx
                lookup = candidate
                break

        if header_idx is None:
            # Fallback posicional para archivos sin cabecera estándar (ej. Provecol)
            return self._extract_entries_xls_positional(sheet)

        entries: list[tuple[str, str, str]] = []
        for row_idx in range(header_idx + 1, sheet.nrows):
            try:
                cod = str(sheet.cell_value(row_idx, lookup["Cod_Prod"])).strip()
                desc = str(sheet.cell_value(row_idx, lookup["Descripcion_prod"])).strip()
                raw_u = sheet.cell_value(row_idx, lookup["Unidades"])
                unidades = str(raw_u).strip().replace(".", ",")
            except IndexError:
                continue
            if not cod or not desc:
                continue
            entries.append((cod, desc, unidades))

        return entries

    @staticmethod
    def _extract_entries_xls_positional(
        sheet,
    ) -> list[tuple[str, str, str]]:
        """Extracción posicional para XLS sin cabecera estándar (formato Provecol).

        Detecta automáticamente si es ventas (código en col 3, desc en col 7)
        o inventario (código en col 1, desc en col 3) buscando la primera fila
        con un código numérico de 6 dígitos.
        """
        import re
        CODE_RE = re.compile(r"^\d{3,}[A-Za-z]{0,3}$")

        # Detectar formato buscando la columna donde aparecen los códigos
        col_code, col_desc, col_units = None, None, None
        for row_idx in range(sheet.nrows):
            row = [str(sheet.cell_value(row_idx, c)).strip() for c in range(sheet.ncols)]
            # Formato ventas: código en col 3, descripción en col 7
            if (len(row) > 14 and CODE_RE.match(row[3])
                    and row[7] and not CODE_RE.match(row[7])):
                col_code, col_desc, col_units = 3, 7, 14
                break
            # Formato inventario: código en col 1, descripción en col 3
            if (len(row) > 18 and CODE_RE.match(row[1])
                    and row[3] and not CODE_RE.match(row[3])):
                col_code, col_desc, col_units = 1, 3, 18
                break

        if col_code is None:
            return []

        entries: list[tuple[str, str, str]] = []
        for row_idx in range(sheet.nrows):
            row = [str(sheet.cell_value(row_idx, c)).strip() for c in range(sheet.ncols)]
            if len(row) <= col_units:
                continue
            cod = row[col_code]
            desc = row[col_desc]
            raw_u = row[col_units]
            if not CODE_RE.match(cod) or not desc:
                continue
            unidades = raw_u.replace(".", ",")
            entries.append((cod, desc, unidades))

        return entries

    @staticmethod
    def _normalize_text(value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value.strip().lower())
        return "".join(ch for ch in normalized if ch.isalnum())
