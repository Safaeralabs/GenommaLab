"""Tkinter main window for the client RPA panel."""

from __future__ import annotations

import os
import queue
import threading
import tkinter as tk
from datetime import date, datetime, timedelta
from pathlib import Path
from tkinter import font as tkfont
from tkinter import messagebox, ttk
from typing import Sequence

from app.config import settings
from app.core.history_manager import load_history
from app.core.logger_manager import configure_logging
from app.core.models import ExecutionErrorDetail, ExecutionResult, ExecutionSummary, HomologationSummary, Proveedor
from app.core.orchestrator import Orchestrator, UiCallbacks
from app.core.provider_loader import ProviderLoader
from app.utils.file_utils import (
    ensure_default_provider_catalog,
    ensure_directories,
    open_directory,
)

# ── Paleta de colores ─────────────────────────────────────────────────────────
CLR_ACCENT       = "#2563EB"   # azul primario
CLR_ACCENT_HOVER = "#1D4ED8"
CLR_SUCCESS      = "#16A34A"
CLR_WARNING      = "#D97706"
CLR_ERROR        = "#DC2626"
CLR_BG           = "#F1F5F9"   # fondo general
CLR_SURFACE      = "#FFFFFF"   # tarjetas / paneles
CLR_BORDER       = "#CBD5E1"
CLR_TEXT         = "#1E293B"
CLR_MUTED        = "#64748B"

CLR_SUCCESS_ROW  = "#DCFCE7"
CLR_ERROR_ROW    = "#FEE2E2"

# ── Fuentes ───────────────────────────────────────────────────────────────────
FONT_TITLE   = ("Segoe UI", 15, "bold")
FONT_HEADING = ("Segoe UI", 10, "bold")
FONT_BODY    = ("Segoe UI", 9)
FONT_SMALL   = ("Segoe UI", 8)
FONT_MONO    = ("Consolas", 9)

PORTAL_DISPLAY_NAMES = {
    "Abako": "Abako",
    "EOS Consultores": "EOS Consultores",
    "Xeon": "Xeon",
    "Soluciones Practicas": "Soluciones Practicas",
}


class MainWindow:
    """Main desktop window."""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(f"{settings.APP_NAME} v{settings.APP_VERSION}")
        self.root.geometry("980x660")
        self.root.minsize(820, 560)
        self.root.configure(bg=CLR_BG)

        self._setup_styles()

        ensure_directories()
        self.provider_catalog_path = ensure_default_provider_catalog()
        self.provider_loader = ProviderLoader()
        self.provider_source_var = tk.StringVar(
            value=f"Fuente: {settings.PROVIDERS_SOURCE.capitalize()}"
        )
        self.source_path_var = tk.StringVar(value=str(self.provider_catalog_path))

        self.log_queue: "queue.Queue[str]" = queue.Queue()
        self.logger = configure_logging(self.log_queue)

        self.iso_year, self.default_week = self._last_available_iso_year_week()
        self.week_var = tk.StringVar(value=str(self.default_week))
        self.week_description_var = tk.StringVar(value=self._describe_week(self.default_week))
        self.active_providers = self._load_active_providers()
        self.portal_filter_map = self._build_portal_filter_map(self.active_providers)
        self.portal_filter_var = tk.StringVar(value="Todos")
        self.provider_count_var = tk.StringVar(value="0 proveedores")

        # Filtro de clientes específicos
        self._selected_clients: set[str] = set()
        self._client_search_var = tk.StringVar()
        self._visible_client_names: list[str] = []
        self._client_iid_to_name: dict[str, str] = {}   # iid → display_name
        self._client_selection_label_var = tk.StringVar(value="sin selección → ejecuta todos")
        self.onedrive_path: Path | None = settings.ONEDRIVE_SYNC_DIR
        self.onedrive_status_var = tk.StringVar(value="OneDrive no detectado")

        self.worker_thread: threading.Thread | None = None
        self.status_var = tk.StringVar(value="Listo para ejecutar.")
        self.progress_var = tk.DoubleVar(value=0.0)
        self.progress_pct_var = tk.StringVar(value="0%")
        self.eta_var = tk.StringVar(value="")
        self.execution_start: datetime | None = None
        self.latest_homologation_summary: HomologationSummary | None = None
        self.error_details: list[ExecutionErrorDetail] = []
        self.error_item_map: dict[str, ExecutionErrorDetail] = {}
        self.selected_error_id: str | None = None
        self.result_entries: dict[str, str] = {}
        self.is_running = False

        self.stop_event = threading.Event()
        self._build_ui()
        self._update_provider_count()
        self._poll_log_queue()

    # ── Estilos ttk ──────────────────────────────────────────────────────────

    def _setup_styles(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure(".", background=CLR_BG, foreground=CLR_TEXT, font=FONT_BODY)
        style.configure("TFrame", background=CLR_BG)
        style.configure("Surface.TFrame", background=CLR_SURFACE, relief="flat")

        style.configure("TLabel", background=CLR_BG, foreground=CLR_TEXT, font=FONT_BODY)
        style.configure("Title.TLabel", font=FONT_TITLE, foreground=CLR_TEXT, background=CLR_BG)
        style.configure("Heading.TLabel", font=FONT_HEADING, foreground=CLR_TEXT, background=CLR_BG)
        style.configure("Muted.TLabel", font=FONT_SMALL, foreground=CLR_MUTED, background=CLR_BG)
        style.configure("MutedSurface.TLabel", font=FONT_SMALL, foreground=CLR_MUTED, background=CLR_SURFACE)
        style.configure("Success.TLabel", font=FONT_BODY, foreground=CLR_SUCCESS, background=CLR_BG)
        style.configure("Error.TLabel", font=FONT_BODY, foreground=CLR_ERROR, background=CLR_BG)

        # Botón primario
        style.configure(
            "Primary.TButton",
            font=FONT_HEADING,
            background=CLR_ACCENT,
            foreground="#FFFFFF",
            borderwidth=0,
            focusthickness=0,
            padding=(14, 7),
        )
        style.map(
            "Primary.TButton",
            background=[("active", CLR_ACCENT_HOVER), ("disabled", "#94A3B8")],
            foreground=[("disabled", "#CBD5E1")],
        )

        # Botón secundario
        style.configure(
            "TButton",
            font=FONT_BODY,
            background=CLR_SURFACE,
            foreground=CLR_TEXT,
            borderwidth=1,
            relief="flat",
            padding=(10, 6),
        )
        style.map(
            "TButton",
            background=[("active", "#E2E8F0"), ("disabled", "#F1F5F9")],
            foreground=[("disabled", CLR_MUTED)],
        )

        # Botón peligroso (detener)
        style.configure(
            "Danger.TButton",
            font=FONT_BODY,
            background="#FEE2E2",
            foreground=CLR_ERROR,
            borderwidth=1,
            relief="flat",
            padding=(10, 6),
        )
        style.map(
            "Danger.TButton",
            background=[("active", "#FECACA"), ("disabled", "#F1F5F9")],
            foreground=[("disabled", CLR_MUTED)],
        )

        # Progress bar
        style.configure(
            "Accent.Horizontal.TProgressbar",
            troughcolor=CLR_BORDER,
            background=CLR_ACCENT,
            darkcolor=CLR_ACCENT,
            lightcolor=CLR_ACCENT,
            bordercolor=CLR_BORDER,
            thickness=10,
        )

        # Combobox
        style.configure("TCombobox", padding=(6, 4), font=FONT_BODY)

        # LabelFrame
        style.configure(
            "TLabelframe",
            background=CLR_SURFACE,
            bordercolor=CLR_BORDER,
            relief="solid",
            borderwidth=1,
        )
        style.configure(
            "TLabelframe.Label",
            font=FONT_HEADING,
            foreground=CLR_MUTED,
            background=CLR_SURFACE,
        )

        # Treeview
        style.configure(
            "Treeview",
            background=CLR_SURFACE,
            foreground=CLR_TEXT,
            rowheight=24,
            fieldbackground=CLR_SURFACE,
            font=FONT_BODY,
            borderwidth=0,
        )
        style.configure(
            "Treeview.Heading",
            font=FONT_HEADING,
            background="#E2E8F0",
            foreground=CLR_MUTED,
            relief="flat",
        )
        style.map(
            "Treeview",
            background=[("selected", "#DBEAFE")],
            foreground=[("selected", CLR_ACCENT)],
        )

        # Scrollbar
        style.configure(
            "TScrollbar",
            background=CLR_BORDER,
            troughcolor=CLR_BG,
            borderwidth=0,
            arrowcolor=CLR_MUTED,
        )

    # ── Construcción de la UI ─────────────────────────────────────────────────

    def _build_ui(self) -> None:
        outer = tk.Frame(self.root, bg=CLR_BG)
        outer.pack(fill=tk.BOTH, expand=True, padx=12, pady=8)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(3, weight=1)

        self._build_header(outer)    # row 0
        self._build_toolbar(outer)   # row 1
        self._build_statusbar(outer) # row 2
        self._build_main_area(outer) # row 3 – expands

        self._update_onedrive_status()

    # ── Header ────────────────────────────────────────────────────────────────

    def _build_header(self, parent: tk.Frame) -> None:
        frame = tk.Frame(parent, bg=CLR_BG)
        frame.grid(row=0, column=0, sticky="ew", pady=(0, 4))

        # Title + badge (left side)
        ttk.Label(frame, text=settings.APP_NAME, style="Title.TLabel").pack(
            side=tk.LEFT, anchor="center"
        )
        badge = tk.Label(
            frame,
            text=f" v{settings.APP_VERSION} ",
            font=FONT_SMALL,
            bg="#DBEAFE",
            fg=CLR_ACCENT,
            padx=4,
            pady=2,
        )
        badge.pack(side=tk.LEFT, anchor="center", padx=(6, 12))

        ttk.Label(frame, textvariable=self.provider_source_var, style="Muted.TLabel").pack(
            side=tk.LEFT, anchor="center"
        )
        ttk.Label(frame, text=" | ", style="Muted.TLabel").pack(side=tk.LEFT, anchor="center")
        ttk.Label(frame, textvariable=self.source_path_var, style="Muted.TLabel").pack(
            side=tk.LEFT, anchor="center"
        )

        # OneDrive status (right side)
        ttk.Label(
            frame, textvariable=self.onedrive_status_var, style="Muted.TLabel",
        ).pack(side=tk.RIGHT, anchor="center")

    # ── Toolbar (semana + portal + acciones) ─────────────────────────────────

    def _build_toolbar(self, parent: tk.Frame) -> None:
        bar = tk.Frame(parent, bg=CLR_SURFACE, bd=0,
                       highlightbackground=CLR_BORDER, highlightthickness=1)
        bar.grid(row=1, column=0, sticky="ew", pady=(0, 4))

        # Semana combo
        ttk.Label(bar, text="Semana:", style="Heading.TLabel",
                  background=CLR_SURFACE).pack(side=tk.LEFT, padx=(8, 4))
        week_values = [str(w) for w in range(1, self.default_week + 1)]
        week_combo = ttk.Combobox(
            bar, textvariable=self.week_var,
            values=week_values, state="readonly", width=5,
        )
        week_combo.pack(side=tk.LEFT)
        week_combo.bind("<<ComboboxSelected>>", self._on_week_change)

        ttk.Label(
            bar, textvariable=self.week_description_var,
            style="MutedSurface.TLabel",
        ).pack(side=tk.LEFT, padx=(6, 0))

        # Separador
        tk.Frame(bar, width=1, bg=CLR_BORDER).pack(side=tk.LEFT, fill=tk.Y, padx=10, pady=4)

        # Portal combo
        ttk.Label(bar, text="Portal:", style="Heading.TLabel",
                  background=CLR_SURFACE).pack(side=tk.LEFT, padx=(0, 4))
        portal_combo = ttk.Combobox(
            bar, textvariable=self.portal_filter_var,
            values=list(self.portal_filter_map.keys()),
            state="readonly", width=22,
        )
        portal_combo.pack(side=tk.LEFT)
        portal_combo.bind("<<ComboboxSelected>>", self._on_portal_filter_change)

        ttk.Label(
            bar, textvariable=self.provider_count_var,
            style="MutedSurface.TLabel",
        ).pack(side=tk.LEFT, padx=(6, 0))

        # Separador
        tk.Frame(bar, width=1, bg=CLR_BORDER).pack(side=tk.LEFT, fill=tk.Y, padx=10, pady=4)

        # Botones de acción
        self.run_button = ttk.Button(
            bar, text="▶ Ejecutar", style="Primary.TButton",
            command=self._start_execution,
        )
        self.run_button.pack(side=tk.LEFT, padx=(0, 4))

        self.stop_button = ttk.Button(
            bar, text="■ Detener", style="Danger.TButton",
            command=self._request_stop, state=tk.DISABLED,
        )
        self.stop_button.pack(side=tk.LEFT, padx=(0, 4))

        ttk.Button(
            bar, text="📁 Resultados",
            command=lambda: open_directory(settings.POSTPROCESSED_DIR),
        ).pack(side=tk.LEFT, padx=(0, 4))

        self.open_last_button = ttk.Button(
            bar, text="📊 Homolog.",
            command=self._open_latest_homologation, state=tk.DISABLED,
        )
        self.open_last_button.pack(side=tk.LEFT, padx=(0, 4))

        self.open_onedrive_button = ttk.Button(
            bar, text="☁ OneDrive",
            command=self._open_onedrive_dir, state=tk.DISABLED,
        )
        self.open_onedrive_button.pack(side=tk.LEFT, padx=(0, 8))

    # ── Status bar ────────────────────────────────────────────────────────────

    def _build_statusbar(self, parent: tk.Frame) -> None:
        bar = tk.Frame(parent, bg=CLR_SURFACE, bd=0,
                       highlightbackground=CLR_BORDER, highlightthickness=1)
        bar.grid(row=2, column=0, sticky="ew", pady=(0, 4))

        self.status_dot = tk.Label(
            bar, text="●", font=("Segoe UI", 10),
            fg=CLR_MUTED, bg=CLR_SURFACE,
        )
        self.status_dot.pack(side=tk.LEFT, padx=(8, 4), pady=3)

        self.status_label = ttk.Label(
            bar, textvariable=self.status_var,
            style="MutedSurface.TLabel",
        )
        self.status_label.pack(side=tk.LEFT, padx=(0, 8))

        # ETA on right
        ttk.Label(
            bar, textvariable=self.eta_var, style="MutedSurface.TLabel",
        ).pack(side=tk.RIGHT, padx=(8, 8))

        # Progress %
        ttk.Label(
            bar, textvariable=self.progress_pct_var,
            style="MutedSurface.TLabel", width=5,
        ).pack(side=tk.RIGHT, padx=(0, 4))

        # Progress bar
        self.progress_bar = ttk.Progressbar(
            bar, variable=self.progress_var,
            maximum=100, mode="determinate",
            style="Accent.Horizontal.TProgressbar",
            length=140,
        )
        self.progress_bar.pack(side=tk.RIGHT, padx=(0, 4))

    # ── Área principal (2 columnas) ───────────────────────────────────────────

    def _build_main_area(self, parent: tk.Frame) -> None:
        main = tk.Frame(parent, bg=CLR_BG)
        main.grid(row=3, column=0, sticky="nsew")
        main.columnconfigure(0, minsize=260, weight=0)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(0, weight=1)

        self._build_client_filter(main)  # column 0
        self._build_right_panel(main)    # column 1

    # ── Filtro de clientes ────────────────────────────────────────────────────

    def _build_client_filter(self, parent: tk.Frame) -> None:
        card = self._card(parent)
        card._outer.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        card._outer.rowconfigure(0, weight=1)
        card.rowconfigure(2, weight=1)

        # ── Cabecera compacta ─────────────────────────────────────────────────
        header = tk.Frame(card, bg=CLR_SURFACE)
        header.pack(fill=tk.X, pady=(0, 4))

        ttk.Label(
            header, text="Clientes",
            style="Heading.TLabel", background=CLR_SURFACE,
        ).pack(side=tk.LEFT)

        ttk.Button(
            header, text="✓Todos",
            command=self._select_all_clients,
        ).pack(side=tk.RIGHT, padx=(4, 0))
        ttk.Button(
            header, text="✕",
            command=self._clear_client_selection,
        ).pack(side=tk.RIGHT)

        # ── Buscador ──────────────────────────────────────────────────────────
        search_outer = tk.Frame(card, bg=CLR_BORDER, padx=1, pady=1)
        search_outer.pack(fill=tk.X, pady=(0, 4))
        search_inner = tk.Frame(search_outer, bg=CLR_SURFACE)
        search_inner.pack(fill=tk.X)

        tk.Label(
            search_inner, text="🔍", bg=CLR_SURFACE, fg=CLR_MUTED, font=FONT_BODY,
        ).pack(side=tk.LEFT, padx=(6, 2))
        search_entry = tk.Entry(
            search_inner, textvariable=self._client_search_var,
            font=FONT_BODY, bg=CLR_SURFACE, fg=CLR_TEXT,
            relief="flat", bd=4, insertbackground=CLR_TEXT,
        )
        search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self._client_search_var.trace_add("write", self._on_client_search_change)

        # ── Treeview con checkboxes ───────────────────────────────────────────
        tree_frame = tk.Frame(card, bg=CLR_SURFACE)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        self._client_tree = ttk.Treeview(
            tree_frame,
            columns=("check", "name"),
            show="headings",
            selectmode="none",
        )
        self._client_tree.heading("check", text="")
        self._client_tree.heading("name", text="Cliente / Sede")
        self._client_tree.column("check", width=28, minwidth=28, anchor="center", stretch=False)
        self._client_tree.column("name", anchor="w")
        self._client_tree.tag_configure("checked",
                                        background="#EFF6FF", foreground=CLR_ACCENT)
        self._client_tree.tag_configure("unchecked",
                                        background=CLR_SURFACE, foreground=CLR_TEXT)
        self._client_tree.grid(row=0, column=0, sticky="nsew")
        self._client_tree.bind("<Button-1>", self._on_client_tree_click)

        client_scroll = ttk.Scrollbar(
            tree_frame, orient="vertical", command=self._client_tree.yview,
        )
        client_scroll.grid(row=0, column=1, sticky="ns")
        self._client_tree.configure(yscrollcommand=client_scroll.set)

        # Etiqueta de selección al fondo
        self._client_sel_label = ttk.Label(
            card, textvariable=self._client_selection_label_var,
            style="MutedSurface.TLabel",
        )
        self._client_sel_label.pack(anchor="w", pady=(4, 0))

        self._refresh_client_tree()

    # ── Lógica del filtro de clientes ─────────────────────────────────────────

    def _refresh_client_tree(self) -> None:
        """Reconstruye el Treeview de clientes según portal y búsqueda activos."""
        search = self._client_search_var.get().strip().lower()
        portal_types = self._selected_portal_types()

        candidates = [
            p for p in self.active_providers
            if (portal_types is None or p.portal_origen in portal_types)
        ]
        filtered = [
            p for p in candidates
            if not search or search in p.display_name.lower()
        ]

        self._visible_client_names = [p.display_name for p in filtered]

        self._client_tree.delete(*self._client_tree.get_children())
        self._client_iid_to_name.clear()
        for idx, provider in enumerate(filtered):
            name = provider.display_name
            iid = str(idx)
            checked = name in self._selected_clients
            tag = "checked" if checked else "unchecked"
            symbol = "☑" if checked else "☐"
            self._client_tree.insert("", "end", iid=iid, values=(symbol, name), tags=(tag,))
            self._client_iid_to_name[iid] = name

        self._refresh_client_selection_label()

    def _on_client_tree_click(self, event: tk.Event) -> None:
        """Alterna el estado ☐/☑ de la fila clickeada."""
        region = self._client_tree.identify_region(event.x, event.y)
        if region not in ("cell", "tree"):
            return
        iid = self._client_tree.identify_row(event.y)
        if not iid:
            return

        name = self._client_iid_to_name.get(iid)
        if name is None:
            return

        if name in self._selected_clients:
            self._selected_clients.discard(name)
            self._client_tree.item(iid, values=("☐", name), tags=("unchecked",))
        else:
            self._selected_clients.add(name)
            self._client_tree.item(iid, values=("☑", name), tags=("checked",))

        self._refresh_client_selection_label()
        self._update_provider_count()

    def _on_client_search_change(self, *_) -> None:
        self._refresh_client_tree()

    def _clear_client_selection(self) -> None:
        self._selected_clients.clear()
        self._refresh_client_tree()
        self._update_provider_count()

    def _select_all_clients(self) -> None:
        """Selecciona todos los clientes visibles (respetando búsqueda activa)."""
        for name in self._visible_client_names:
            self._selected_clients.add(name)
        self._refresh_client_tree()
        self._update_provider_count()

    def _refresh_client_selection_label(self) -> None:
        count = len(self._selected_clients)
        if count == 0:
            self._client_selection_label_var.set("sin selección → ejecuta todos")
        elif count == 1:
            name = next(iter(self._selected_clients))
            short = name if len(name) <= 35 else name[:32] + "…"
            self._client_selection_label_var.set(f"— {short}")
        else:
            self._client_selection_label_var.set(f"— {count} clientes seleccionados")

    def _get_providers_override(self) -> list[Proveedor] | None:
        """Devuelve los proveedores seleccionados, o None si no hay selección."""
        if not self._selected_clients:
            return None
        portal_types = self._selected_portal_types()
        result = [
            p for p in self.active_providers
            if p.display_name in self._selected_clients
            and (portal_types is None or p.portal_origen in portal_types)
        ]
        return result or None

    # ── Panel derecho (resultados + log + errores) ────────────────────────────

    def _build_right_panel(self, parent: tk.Frame) -> None:
        panel = tk.Frame(parent, bg=CLR_BG)
        panel.grid(row=0, column=1, sticky="nsew", padx=(4, 0))
        panel.columnconfigure(0, weight=1)
        panel.rowconfigure(0, weight=0)   # results – fixed height
        panel.rowconfigure(1, weight=1)   # log     – expands
        panel.rowconfigure(2, weight=0)   # errors  – fixed height
        panel.rowconfigure(3, weight=0)   # history – fixed height

        # ── row 0: Archivos generados ──────────────────────────────────────────
        result_frame = ttk.LabelFrame(panel, text="Archivos generados")
        result_frame.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        result_frame.rowconfigure(0, weight=1)
        result_frame.columnconfigure(0, weight=1)

        result_columns = ("proveedor", "ventas", "inventario")
        self.result_tree = ttk.Treeview(
            result_frame, columns=result_columns, show="headings",
            height=3, selectmode="none",
        )
        self.result_tree.heading("proveedor", text="Proveedor")
        self.result_tree.heading("ventas", text="Ventas")
        self.result_tree.heading("inventario", text="Inventario")
        self.result_tree.column("proveedor", width=200, anchor="w")
        self.result_tree.column("ventas", width=220, anchor="w")
        self.result_tree.column("inventario", width=220, anchor="w")
        self.result_tree.tag_configure("ok", background=CLR_SUCCESS_ROW)
        self.result_tree.tag_configure("partial", background="#FEF9C3")
        self.result_tree.tag_configure("error", background=CLR_ERROR_ROW)
        self.result_tree.tag_configure("running", background="#DBEAFE")  # azul claro = en curso
        self.result_tree.grid(row=0, column=0, sticky="nsew")

        result_scroll = ttk.Scrollbar(result_frame, orient="vertical", command=self.result_tree.yview)
        result_scroll.grid(row=0, column=1, sticky="ns")
        self.result_tree.configure(yscrollcommand=result_scroll.set)

        # ── row 1: Log ────────────────────────────────────────────────────────
        log_frame = ttk.LabelFrame(panel, text="Registros de ejecución")
        log_frame.grid(row=1, column=0, sticky="nsew", pady=(0, 4))
        log_frame.rowconfigure(1, weight=1)
        log_frame.columnconfigure(0, weight=1)

        ttk.Button(
            log_frame, text="Ver log en vivo ↗",
            command=self._open_log_terminal,
        ).grid(row=0, column=0, sticky="w", padx=6, pady=(4, 0))

        self.log_text = tk.Text(
            log_frame, wrap="word", state="disabled", height=8,
            font=FONT_MONO, bg=CLR_SURFACE, fg=CLR_TEXT,
            relief="flat", borderwidth=0,
            selectbackground="#BFDBFE",
        )
        self.log_text.grid(row=1, column=0, sticky="nsew", padx=6, pady=6)

        # Etiquetas de color para el log
        self.log_text.tag_configure("ERROR",   foreground=CLR_ERROR)
        self.log_text.tag_configure("WARNING", foreground=CLR_WARNING)
        self.log_text.tag_configure("SUCCESS", foreground=CLR_SUCCESS)
        self.log_text.tag_configure("MUTED",   foreground=CLR_MUTED)
        self.log_text.tag_configure("BOLD",    font=(*FONT_MONO[:1], FONT_MONO[1], "bold"))

        log_scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scrollbar.grid(row=1, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scrollbar.set)

        # ── row 2: Clientes con errores ───────────────────────────────────────
        self.error_frame = ttk.LabelFrame(panel, text="Clientes con errores")
        self.error_frame.grid(row=2, column=0, sticky="ew", pady=(0, 4))
        self.error_frame.rowconfigure(0, weight=1)
        self.error_frame.columnconfigure(0, weight=1)

        columns = ("proveedor", "razon", "screenshot")
        self.error_tree = ttk.Treeview(
            self.error_frame, columns=columns, show="headings",
            height=3, selectmode="browse",
        )
        self.error_tree.heading("proveedor", text="Proveedor")
        self.error_tree.heading("razon", text="Razón del error")
        self.error_tree.heading("screenshot", text="Captura")
        self.error_tree.column("proveedor", width=200, anchor="w")
        self.error_tree.column("razon", width=360, anchor="w")
        self.error_tree.column("screenshot", width=70, anchor="center")
        self.error_tree.tag_configure("error_row", background=CLR_ERROR_ROW)
        self.error_tree.grid(row=0, column=0, sticky="nsew")
        self.error_tree.bind("<<TreeviewSelect>>", self._on_error_select)

        error_scroll = ttk.Scrollbar(self.error_frame, orient="vertical", command=self.error_tree.yview)
        error_scroll.grid(row=0, column=1, sticky="ns")
        self.error_tree.configure(yscrollcommand=error_scroll.set)

        # Acciones de errores
        error_actions = tk.Frame(self.error_frame, bg=CLR_SURFACE)
        error_actions.grid(row=1, column=0, columnspan=2, sticky="ew", padx=4, pady=(6, 6))

        self.screenshot_button = ttk.Button(
            error_actions, text="🔍  Ver captura",
            command=self._open_selected_screenshot, state=tk.DISABLED,
        )
        self.screenshot_button.pack(side=tk.LEFT)

        self.retry_errors_button = ttk.Button(
            error_actions, text="↺  Reintentar errores",
            command=self._retry_errors, state=tk.DISABLED,
        )
        self.retry_errors_button.pack(side=tk.RIGHT)

        # ── row 3: Historial ──────────────────────────────────────────────────
        self._build_history_panel(panel)

    def _build_history_panel(self, parent: tk.Frame) -> None:
        """Build the execution history LabelFrame (row 3 of right panel)."""
        history_frame = ttk.LabelFrame(parent, text="Historial de ejecuciones")
        history_frame.grid(row=3, column=0, sticky="ew", pady=(0, 4))
        history_frame.rowconfigure(0, weight=1)
        history_frame.columnconfigure(0, weight=1)

        self._history_data: list[dict] = []

        hist_columns = ("fecha", "semana", "ok", "fallidos", "duracion", "filas_homol")
        self.history_tree = ttk.Treeview(
            history_frame, columns=hist_columns, show="headings",
            height=4, selectmode="browse",
        )
        self.history_tree.heading("fecha",       text="Fecha")
        self.history_tree.heading("semana",      text="Semana")
        self.history_tree.heading("ok",          text="OK")
        self.history_tree.heading("fallidos",    text="Fallidos")
        self.history_tree.heading("duracion",    text="Duración")
        self.history_tree.heading("filas_homol", text="Filas Homol.")
        self.history_tree.column("fecha",       width=145, anchor="w")
        self.history_tree.column("semana",      width=70,  anchor="center")
        self.history_tree.column("ok",          width=50,  anchor="center")
        self.history_tree.column("fallidos",    width=60,  anchor="center")
        self.history_tree.column("duracion",    width=75,  anchor="center")
        self.history_tree.column("filas_homol", width=90,  anchor="center")
        self.history_tree.grid(row=0, column=0, sticky="nsew")

        hist_scroll = ttk.Scrollbar(history_frame, orient="vertical", command=self.history_tree.yview)
        hist_scroll.grid(row=0, column=1, sticky="ns")
        self.history_tree.configure(yscrollcommand=hist_scroll.set)
        self.history_tree.bind("<<TreeviewSelect>>", self._on_history_select)

        hist_actions = tk.Frame(history_frame, bg=CLR_SURFACE)
        hist_actions.grid(row=1, column=0, columnspan=2, sticky="ew", padx=4, pady=(4, 6))

        ttk.Button(
            hist_actions, text="Actualizar",
            command=self._refresh_history,
        ).pack(side=tk.LEFT)

        # Populate on build
        self._refresh_history()

    def _refresh_history(self) -> None:
        """Load history entries and populate the history treeview."""
        for item in self.history_tree.get_children():
            self.history_tree.delete(item)
        self._history_data.clear()
        try:
            entries = load_history()
        except Exception:
            entries = []
        for entry in entries:
            ts = entry.get("timestamp", "")
            year = entry.get("year", "")
            week = entry.get("week", "")
            semana = f"S{week:02d}/{year}" if isinstance(week, int) and isinstance(year, int) else f"S{week}/{year}"
            ok = entry.get("success", "")
            failed = entry.get("failed", "")
            duration_s = entry.get("duration_s", 0)
            if isinstance(duration_s, (int, float)):
                mins, secs = divmod(int(duration_s), 60)
                duracion = f"{mins}m {secs:02d}s"
            else:
                duracion = str(duration_s)
            homol_rows = entry.get("homologation_rows", "")
            self.history_tree.insert(
                "", tk.END,
                values=(ts, semana, ok, failed, duracion, homol_rows),
            )
            self._history_data.append(entry)

    def _on_history_select(self, event=None) -> None:
        sel = self.history_tree.selection()
        if not sel:
            return
        children = list(self.history_tree.get_children())
        try:
            idx = children.index(sel[0])
        except ValueError:
            return
        if idx >= len(self._history_data):
            return
        self._show_history_detail(self._history_data[idx])

    def _show_history_detail(self, entry: dict) -> None:
        import os, subprocess
        from pathlib import Path

        top = tk.Toplevel(self)
        top.title(f"Detalle ejecución — {entry.get('timestamp', '')}")
        top.geometry("720x540")
        top.resizable(True, True)
        top.configure(bg=CLR_BG)

        # ── Cabecera ──────────────────────────────────────────────────────────
        hdr = tk.Frame(top, bg=CLR_ACCENT, padx=14, pady=10)
        hdr.pack(fill=tk.X)
        year = entry.get("year", ""); week = entry.get("week", "")
        semana = f"S{week:02d}/{year}" if isinstance(week, int) and isinstance(year, int) else f"S{week}/{year}"
        duration_s = entry.get("duration_s", 0)
        mins, secs = divmod(int(duration_s), 60) if isinstance(duration_s, (int, float)) else (0, 0)
        info_txt = (
            f"{entry.get('timestamp','')}   |   {semana}   |   "
            f"✓ {entry.get('success',0)}  ✗ {entry.get('failed',0)} / {entry.get('total',0)}   |   "
            f"{mins}m {secs:02d}s   |   {entry.get('homologation_rows',0)} filas homol."
        )
        tk.Label(hdr, text=info_txt, bg=CLR_ACCENT, fg="white", font=FONT_BODY).pack(anchor="w")

        body = tk.Frame(top, bg=CLR_BG, padx=10, pady=8)
        body.pack(fill=tk.BOTH, expand=True)
        body.columnconfigure(0, weight=1)

        # ── Clientes con errores ──────────────────────────────────────────────
        tk.Label(body, text="Clientes con errores", font=FONT_HEADING, bg=CLR_BG, fg=CLR_ERROR).grid(
            row=0, column=0, sticky="w", pady=(0, 2))
        err_frame = tk.Frame(body, bg=CLR_SURFACE, bd=1, relief="solid")
        err_frame.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        err_frame.columnconfigure(0, weight=1)

        error_details = entry.get("error_details", [])
        if not error_details:
            tk.Label(err_frame, text="  Sin errores en esta ejecución.", font=FONT_BODY,
                     bg=CLR_SURFACE, fg=CLR_MUTED, pady=4).pack(anchor="w")
        else:
            for det in error_details:
                prov = det.get("proveedor", "?")
                msg  = det.get("message", "")
                row_f = tk.Frame(err_frame, bg=CLR_ERROR_ROW, padx=8, pady=3)
                row_f.pack(fill=tk.X)
                tk.Label(row_f, text=prov, font=FONT_HEADING, bg=CLR_ERROR_ROW,
                         fg=CLR_ERROR, width=28, anchor="w").pack(side=tk.LEFT)
                tk.Label(row_f, text=msg[:120], font=FONT_SMALL, bg=CLR_ERROR_ROW,
                         fg=CLR_TEXT, anchor="w", wraplength=420, justify="left").pack(side=tk.LEFT, fill=tk.X)

        # ── Archivos generados ────────────────────────────────────────────────
        tk.Label(body, text="Archivos generados", font=FONT_HEADING, bg=CLR_BG, fg=CLR_TEXT).grid(
            row=2, column=0, sticky="w", pady=(0, 2))

        files_outer = tk.Frame(body, bg=CLR_SURFACE, bd=1, relief="solid")
        files_outer.grid(row=3, column=0, sticky="nsew", pady=(0, 8))
        files_outer.columnconfigure(0, weight=1)
        body.rowconfigure(3, weight=1)

        files_scroll = tk.Scrollbar(files_outer, orient="vertical")
        files_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        files_txt = tk.Text(files_outer, font=FONT_MONO, bg=CLR_SURFACE, fg=CLR_TEXT,
                            relief="flat", wrap="none", height=8,
                            yscrollcommand=files_scroll.set, state="normal")
        files_txt.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        files_scroll.config(command=files_txt.yview)

        exec_dir = entry.get("execution_dir")
        if exec_dir and Path(exec_dir).exists():
            found = sorted(Path(exec_dir).rglob("*.*"))
            if found:
                for f in found:
                    files_txt.insert(tk.END, str(f.relative_to(Path(exec_dir))) + "\n")
            else:
                files_txt.insert(tk.END, "(carpeta vacía)")
        else:
            files_txt.insert(tk.END, "(carpeta de ejecución no disponible)")
        files_txt.config(state="disabled")

        # ── Botones inferiores ────────────────────────────────────────────────
        btn_frame = tk.Frame(top, bg=CLR_BG, padx=10, pady=6)
        btn_frame.pack(fill=tk.X)

        hom_path = entry.get("homologation_path")
        if hom_path and Path(hom_path).exists():
            ttk.Button(
                btn_frame, text="Abrir homologación",
                command=lambda p=hom_path: os.startfile(p),
            ).pack(side=tk.LEFT, padx=(0, 6))

        if exec_dir and Path(exec_dir).exists():
            ttk.Button(
                btn_frame, text="Abrir carpeta archivos",
                command=lambda d=exec_dir: os.startfile(d),
            ).pack(side=tk.LEFT, padx=(0, 6))

        ttk.Button(btn_frame, text="Cerrar", command=top.destroy).pack(side=tk.RIGHT)

    # ── Helper: tarjeta ───────────────────────────────────────────────────────

    def _card(self, parent) -> tk.Frame:
        """Devuelve un frame estilo tarjeta (borde + fondo blanco + padding).
        El caller es responsable de hacer .pack() o .grid() sobre él."""
        outer = tk.Frame(parent, bg=CLR_BORDER, padx=1, pady=1)
        inner = tk.Frame(outer, bg=CLR_SURFACE, padx=12, pady=10)
        inner.pack(fill=tk.BOTH, expand=True)
        # Guardar referencia al contenedor exterior para que el caller pueda hacer grid/pack
        inner._outer = outer
        return inner

    # ── Lógica de ejecución ───────────────────────────────────────────────────

    def _start_execution(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Ejecucion en curso", "Ya hay una ejecucion en proceso.")
            return

        provider_source, source_path = self._provider_source_with_path()
        if provider_source == "excel" and (not source_path or not source_path.exists()):
            messagebox.showerror("Excel no encontrado", f"No existe el archivo:\n{source_path}")
            return

        self.stop_event.clear()
        self._set_running_state(True)
        self.execution_start = datetime.now()
        self._reset_ui()
        week_number = int(self.week_var.get())
        portal_filter_label = self.portal_filter_var.get()

        # Clientes específicos tienen prioridad sobre el filtro de portal
        client_override = self._get_providers_override()
        if client_override is not None:
            portal_types = None
            label_clientes = f"{len(client_override)} cliente(s) seleccionado(s)"
        else:
            portal_types = self._selected_portal_types()
            label_clientes = f"Portal: {portal_filter_label}"

        source_label = "Excel" if provider_source == "excel" else "catálogo JSON"
        self._append_log(
            f"Fuente {source_label}: {source_path or 'interno'} | Semana {week_number} | {label_clientes}"
        )

        callbacks = UiCallbacks(
            on_status=lambda text: self.root.after(0, self.status_var.set, text),
            on_progress=lambda current, total: self.root.after(0, self._update_progress, current, total),
            on_summary=lambda summary: self.root.after(0, self._handle_summary, summary),
            on_errors=lambda errors: self.root.after(0, self._handle_errors, errors),
            on_last_homologation=lambda path: self.root.after(0, self._handle_last_homologation, path),
            on_result=lambda result: self.root.after(0, self._handle_provider_result, result),
            on_worker_status=lambda name, status: self.root.after(0, self._handle_worker_status, name, status),
        )
        orchestrator = Orchestrator(logger=self.logger, callbacks=callbacks, stop_event=self.stop_event)

        self.worker_thread = threading.Thread(
            target=self._run_orchestrator,
            args=(
                orchestrator,
                source_path,
                self.iso_year,
                week_number,
                portal_types,
                client_override,
                provider_source,
            ),
            daemon=True,
        )
        self.worker_thread.start()

    def _retry_errors(self) -> None:
        if not self.error_details:
            return
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Ejecucion en curso", "Ya hay una ejecucion en proceso.")
            return

        provider_source, source_path = self._provider_source_with_path()
        if provider_source == "excel" and (not source_path or not source_path.exists()):
            messagebox.showerror("Excel no encontrado", f"No existe el archivo:\n{source_path}")
            return

        providers_override = [detail.proveedor_obj for detail in self.error_details]
        week_number = int(self.week_var.get())
        self.stop_event.clear()
        self._reset_result_tree()
        self._set_running_state(True)
        self.progress_var.set(0)
        self.progress_pct_var.set("0%")
        self.status_var.set("Reintentando proveedores con errores...")
        self.execution_start = datetime.now()
        self.eta_var.set("Calculando tiempo estimado...")
        self._append_log(f"Reintentando {len(providers_override)} proveedor(es) fallidos.")

        callbacks = UiCallbacks(
            on_status=lambda text: self.root.after(0, self.status_var.set, text),
            on_progress=lambda current, total: self.root.after(0, self._update_progress, current, total),
            on_summary=lambda summary: self.root.after(0, self._handle_summary, summary),
            on_errors=lambda errors: self.root.after(0, self._handle_errors, errors),
            on_last_homologation=lambda path: self.root.after(0, self._handle_last_homologation, path),
            on_result=lambda result: self.root.after(0, self._handle_provider_result, result),
            on_worker_status=lambda name, status: self.root.after(0, self._handle_worker_status, name, status),
        )
        orchestrator = Orchestrator(logger=self.logger, callbacks=callbacks, stop_event=self.stop_event)
        self.worker_thread = threading.Thread(
            target=self._run_orchestrator,
            args=(
                orchestrator,
                source_path,
                self.iso_year,
                week_number,
                None,
                providers_override,
                provider_source,
            ),
            daemon=True,
        )
        self.worker_thread.start()

    def _run_orchestrator(
        self,
        orchestrator: Orchestrator,
        excel_path: Path | None,
        year: int,
        week_number: int,
        portal_types: list[str] | None,
        providers_override: Sequence[Proveedor] | None,
        provider_source: str,
    ) -> None:
        try:
            orchestrator.run(
                excel_path,
                year,
                week_number,
                portal_types,
                providers_override,
                provider_source=provider_source,
            )
        except Exception as exc:
            self.logger.exception("La ejecucion termino con error fatal: %s", exc)
            self.root.after(
                0,
                lambda: messagebox.showerror("Error fatal", f"La ejecucion fallo:\n{exc}"),
            )
            self.root.after(0, self._set_running_state, False)

    # ── Actualización de estado ───────────────────────────────────────────────

    def _update_progress(self, current: int, total: int) -> None:
        value = (current / total) * 100 if total else 0
        self.progress_var.set(value)
        self.progress_pct_var.set(f"{int(value)}%")
        self._update_estimated_time(current, total)

    def _update_estimated_time(self, current: int, total: int) -> None:
        if not self.execution_start or current == 0 or total <= current:
            return
        elapsed = datetime.now() - self.execution_start
        avg_per_provider = elapsed / current
        remaining = total - current
        estimated_remaining = avg_per_provider * remaining
        self.eta_var.set(f"Tiempo restante estimado: {self._format_timedelta(estimated_remaining)}")

    def _handle_summary(self, summary: ExecutionSummary) -> None:
        self._set_running_state(False)
        duration = summary.finished_at - summary.started_at
        self.progress_var.set(100)
        self.progress_pct_var.set("100%")
        message = (
            f"Ejecución finalizada.\n"
            f"Total: {summary.total}  |  "
            f"Correctos: {summary.success_count}  |  "
            f"Fallidos: {summary.failure_count}\n"
            f"Duración: {self._format_timedelta(duration)}"
        )
        self._append_log(message, tag="SUCCESS" if summary.failure_count == 0 else "WARNING")
        messagebox.showinfo("Resumen de ejecución", message)
        self.eta_var.set(f"Tiempo total: {self._format_timedelta(duration)}")
        self.execution_start = None

    def _handle_errors(self, errors: list[ExecutionErrorDetail]) -> None:
        self.error_details = list(errors)
        self._refresh_error_tree()
        self._update_error_actions()
        self._update_provider_count()

    def _handle_last_homologation(self, summary: HomologationSummary | None) -> None:
        self.latest_homologation_summary = summary
        self._update_error_actions()

    def _handle_provider_result(self, result: ExecutionResult) -> None:
        self._update_result_tree(result)

    def _handle_worker_status(self, name: str, status: str) -> None:
        """Actualiza (o crea) la fila del proveedor con el estado actual del worker."""
        item_id = self.result_entries.get(name)
        values = (name, f"⏳ {status}", "")
        if item_id and item_id in self.result_tree.get_children():
            # Solo actualiza si la fila sigue siendo "running" (no sobreescribir resultado final)
            current_tag = self.result_tree.item(item_id, "tags")
            if current_tag and current_tag[0] == "running":
                self.result_tree.item(item_id, values=values)
        else:
            new_id = self.result_tree.insert("", "end", values=values, tags=("running",))
            self.result_entries[name] = new_id

    def _update_result_tree(self, result: ExecutionResult) -> None:
        # 1) Preferir organized_files (postprocesado completo)
        if result.organized_files:
            ventas_files = [
                item.path.name for item in result.organized_files if item.category == "ventas"
            ]
            inventario_files = [
                item.path.name for item in result.organized_files if item.category == "inventario"
            ]
        # 2) Fallback: archivos crudos descargados (descarga parcial o sin postproceso)
        elif result.downloaded_files:
            ventas_files = [f.name for f in result.downloaded_files if "venta" in f.name.lower()]
            inventario_files = [
                f.name for f in result.downloaded_files
                if any(w in f.name.lower() for w in ("inventario", "saldo", "stock", "inv"))
            ]
            # Si no se pudo clasificar por nombre, asignar todo a ventas
            if not ventas_files and not inventario_files:
                ventas_files = [f.name for f in result.downloaded_files]
        elif result.downloaded_file:
            ventas_files = [result.downloaded_file.name]
            inventario_files = []
        else:
            ventas_files = []
            inventario_files = []

        # Determinar tag de color según estado
        if not result.success and not ventas_files and not inventario_files:
            ventas_value = "✗ error"
            inventario_value = "✗ error"
            tag = "error"
        else:
            ventas_value = ", ".join(ventas_files) if ventas_files else "—"
            inventario_value = ", ".join(inventario_files) if inventario_files else "—"
            has_both = bool(ventas_files and inventario_files)
            tag = "ok" if has_both else "partial"

        values = (result.proveedor, ventas_value, inventario_value)
        item_id = self.result_entries.get(result.proveedor)
        if item_id and item_id in self.result_tree.get_children():
            self.result_tree.item(item_id, values=values, tags=(tag,))
            return

        new_id = self.result_tree.insert("", "end", values=values, tags=(tag,))
        self.result_entries[result.proveedor] = new_id

    def _reset_ui(self) -> None:
        """Limpia todos los paneles de resultados antes de una nueva ejecución."""
        # Barra de progreso y estado
        self.progress_var.set(0)
        self.progress_pct_var.set("0%")
        self.status_var.set("Preparando ejecución...")
        self.eta_var.set("Calculando tiempo estimado...")

        # Tabla de resultados
        self.result_tree.delete(*self.result_tree.get_children())
        self.result_entries.clear()

        # Log
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

        # Errores
        self.error_details.clear()
        self.error_item_map.clear()
        self.selected_error_id = None
        self.error_tree.delete(*self.error_tree.get_children())
        self.error_frame.configure(text="Clientes con errores")

        # Homologación
        self.latest_homologation_summary = None
        self._update_error_actions()

    @staticmethod
    def _format_timedelta(delta: timedelta) -> str:
        total_seconds = int(delta.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        parts = []
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        parts.append(f"{seconds}s")
        return " ".join(parts)

    def _set_running_state(self, running: bool) -> None:
        self.run_button.configure(state=tk.DISABLED if running else tk.NORMAL)
        self.stop_button.configure(state=tk.NORMAL if running else tk.DISABLED)
        self.is_running = running

        # Indicador de estado (punto de color)
        if running:
            self.status_dot.configure(fg=CLR_ACCENT)
        else:
            has_errors = bool(self.error_details)
            self.status_dot.configure(fg=CLR_ERROR if has_errors else CLR_SUCCESS)

        self._update_error_actions()

    def _update_error_actions(self) -> None:
        has_errors = bool(self.error_details)
        retry_state = tk.NORMAL if has_errors and not self.is_running else tk.DISABLED
        self.retry_errors_button.configure(state=retry_state)

        screenshot_state = tk.DISABLED
        if self.selected_error_id:
            detail = self.error_item_map.get(self.selected_error_id)
            if detail and detail.screenshot:
                screenshot_state = tk.NORMAL
        self.screenshot_button.configure(state=screenshot_state)

        if self.latest_homologation_summary:
            s = self.latest_homologation_summary
            if s.total_providers > 0:
                if s.included_providers >= s.total_providers:
                    label = f"Homolog. S ✓ {s.included_providers}/{s.total_providers}"
                else:
                    label = f"Homolog. ⚠ {s.included_providers}/{s.total_providers}"
            else:
                label = "Homolog. ✓"
            self.open_last_button.configure(text=label, state=tk.NORMAL)
        else:
            self.open_last_button.configure(text="📊 Homolog.", state=tk.DISABLED)
        self.provider_count_var.set(
            self._provider_count_label(self._selected_portal_types())
        )

    def _refresh_error_tree(self) -> None:
        self.error_tree.delete(*self.error_tree.get_children())
        self.error_item_map.clear()
        self.selected_error_id = None
        for detail in self.error_details:
            screenshot_flag = "✓" if detail.screenshot else "—"
            item_id = self.error_tree.insert(
                "", "end",
                values=(detail.proveedor, detail.message, screenshot_flag),
                tags=("error_row",),
            )
            self.error_item_map[item_id] = detail

        # Actualizar título del frame con conteo
        count = len(self.error_details)
        label = f"Clientes con errores ({count})" if count else "Clientes con errores"
        self.error_frame.configure(text=label)
        self._update_error_actions()

    def _on_error_select(self, event=None) -> None:
        selection = self.error_tree.selection()
        self.selected_error_id = selection[0] if selection else None
        self._update_error_actions()

    def _open_selected_screenshot(self) -> None:
        if not self.selected_error_id:
            return
        detail = self.error_item_map.get(self.selected_error_id)
        if not detail or not detail.screenshot:
            messagebox.showinfo("Sin screenshot", "El proveedor seleccionado no tiene screenshot disponible.")
            return
        if detail.screenshot.exists():
            os.startfile(detail.screenshot)
        else:
            messagebox.showwarning("Archivo no encontrado", "La captura de pantalla ya no existe.")

    def _open_latest_homologation(self) -> None:
        path = self.latest_homologation_summary.path if self.latest_homologation_summary else None
        if not path:
            messagebox.showinfo("Sin archivo", "Aún no se ha generado un archivo de homologación.")
            return
        if not path.exists():
            messagebox.showwarning("Archivo no encontrado", "El archivo de homologación ya no existe.")
            return
        os.startfile(path)

    def _open_onedrive_dir(self) -> None:
        if not self.onedrive_path:
            messagebox.showinfo(
                "OneDrive no configurado",
                "Define la variable de entorno ONEDRIVE o OneDriveCommercial para habilitar la sincronización.",
            )
            return
        if not self.onedrive_path.exists():
            try:
                self.onedrive_path.mkdir(parents=True, exist_ok=True)
            except OSError:
                messagebox.showwarning(
                    "OneDrive inaccesible",
                    "No se pudo crear la carpeta de OneDrive. Verifica permisos.",
                )
                return
        os.startfile(self.onedrive_path)

    def _update_onedrive_status(self) -> None:
        if not self.onedrive_path:
            self.onedrive_status_var.set("OneDrive no detectado")
            self.open_onedrive_button.configure(state=tk.DISABLED)
            return
        try:
            self.onedrive_path.mkdir(parents=True, exist_ok=True)
            self.onedrive_status_var.set(f"☁ OneDrive: {self.onedrive_path}")
            self.open_onedrive_button.configure(state=tk.NORMAL)
        except OSError:
            self.onedrive_status_var.set("OneDrive inaccesible")
            self.open_onedrive_button.configure(state=tk.DISABLED)

    # ── Log ───────────────────────────────────────────────────────────────────

    def _open_log_terminal(self) -> None:
        import subprocess
        log_path = str(settings.LOGS_DIR / settings.LOG_FILE_NAME)
        cmd = (
            f'Get-Content -Path "{log_path}" -Wait -Tail 50'
        )
        subprocess.Popen(
            ["powershell", "-NoExit", "-Command", cmd],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )

    def _poll_log_queue(self) -> None:
        while True:
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self._append_log(message)
        self.root.after(150, self._poll_log_queue)

    def _request_stop(self) -> None:
        if not self.stop_event.is_set():
            self.stop_event.set()
            self._append_log("Solicitud de parada enviada.", tag="WARNING")

    def _append_log(self, message: str, tag: str | None = None) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")

        # Timestamp en gris
        self.log_text.insert(tk.END, f"[{ts}] ", "MUTED")

        # Detectar nivel si no se pasó tag explícito
        if tag is None:
            msg_upper = message.upper()
            if "ERROR" in msg_upper or "FALLO" in msg_upper or "FAIL" in msg_upper:
                tag = "ERROR"
            elif "WARNING" in msg_upper or "ADVERTENCIA" in msg_upper or "WARN" in msg_upper:
                tag = "WARNING"
            elif any(w in msg_upper for w in ("CORRECTO", "ÉXITO", "SUCCESS", "FINALIZ", "COMPLETADO")):
                tag = "SUCCESS"

        if tag:
            self.log_text.insert(tk.END, f"{message}\n", tag)
        else:
            self.log_text.insert(tk.END, f"{message}\n")

        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    # ── Semana / portal ───────────────────────────────────────────────────────

    @staticmethod
    def _last_available_iso_year_week() -> tuple[int, int]:
        """Devuelve (año, semana) de la semana pasada (última con datos disponibles).

        Si hoy es semana 1 del año, retrocede al último año para obtener su
        última semana ISO (semana 52 o 53 según corresponda).
        """
        today = date.today()
        year, week, _ = today.isocalendar()
        if week > 1:
            return year, week - 1
        # Semana 1 del año → última semana del año anterior
        prev_year = year - 1
        # El 28 de diciembre siempre pertenece a la última semana ISO del año
        last_week = date(prev_year, 12, 28).isocalendar()[1]
        return prev_year, last_week

    def _describe_week(self, week_number: int) -> str:
        try:
            start = date.fromisocalendar(self.iso_year, week_number, 1)
        except ValueError:
            return "Semana inválida"
        end = start + timedelta(days=6)
        return f"{start:%d %b %Y} — {end:%d %b %Y}"

    def _on_week_change(self, event=None) -> None:
        try:
            week = int(self.week_var.get())
        except ValueError:
            week = self.default_week
        self.week_description_var.set(self._describe_week(week))

    def _build_portal_filter_map(self, providers: list[Proveedor]) -> dict[str, Sequence[str] | None]:
        labels: dict[str, Sequence[str] | None] = {"Todos": None}
        portal_types = sorted({p.portal_origen for p in providers if p.portal_origen})
        for portal_type in portal_types:
            label = PORTAL_DISPLAY_NAMES.get(portal_type, portal_type)
            display = f"{label} ({portal_type})" if label != portal_type else portal_type
            labels[display] = [portal_type]
        return labels

    def _selected_portal_types(self) -> Sequence[str] | None:
        return self.portal_filter_map.get(self.portal_filter_var.get())

    def _load_active_providers(self) -> list[Proveedor]:
        provider_source, source_path = self._provider_source_with_path()
        try:
            return self.provider_loader.load(provider_source, source_path)
        except Exception as exc:
            self.logger.exception("No se pudieron cargar los proveedores: %s", exc)
            return []

    def _provider_source_with_path(self) -> tuple[str, Path | None]:
        source = settings.PROVIDERS_SOURCE
        if source == "excel":
            return source, Path(settings.DATA_DIR) / settings.EXCEL_FILE_NAME
        return source, self.provider_catalog_path

    def _provider_count_label(self, portal_types: Sequence[str] | None) -> str:
        count = self._count_providers_for_ports(portal_types)
        return f"{count} proveedor{'es' if count != 1 else ''}"

    def _update_provider_count(self) -> None:
        if self._selected_clients:
            n = len(self._selected_clients)
            label = f"{n} cliente{'s' if n != 1 else ''} seleccionado{'s' if n != 1 else ''}"
        else:
            label = self._provider_count_label(self._selected_portal_types())
        self.provider_count_var.set(label)

    def _count_providers_for_ports(self, portal_types: Sequence[str] | None) -> int:
        if portal_types is None:
            return len(self.active_providers)
        return sum(1 for p in self.active_providers if p.portal_origen in portal_types)

    def _on_portal_filter_change(self, event=None) -> None:
        # Al cambiar el portal, limpiar selección de clientes y reconstruir lista
        self._selected_clients.clear()
        self._refresh_client_tree()
        self._update_provider_count()
