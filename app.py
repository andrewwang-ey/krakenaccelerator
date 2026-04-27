"""
app.py  –  Kraken Migration Accelerator GUI
Launch via: run_app.bat
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from tkinter.scrolledtext import ScrolledText
import threading
import subprocess
import shutil
import sys
import os
import json
import queue
from pathlib import Path
from datetime import datetime

import yaml
import duckdb

from cohort_manager import CohortPlan

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent
DATA_DIR      = BASE_DIR / "data"
MAPPING_DIR   = BASE_DIR / "mapping"
TEMPLATES_DIR = MAPPING_DIR / "mapping_templates"
COHORT_CFG    = BASE_DIR / "cohort.yml"
DBT_PROJECT   = BASE_DIR / "dbt_project.yml"
DB_DIR        = BASE_DIR / "db"
DB_PATH       = DB_DIR / "kraken.duckdb"

DATA_DIR.mkdir(exist_ok=True)
DB_DIR.mkdir(exist_ok=True)

# ── Colours ────────────────────────────────────────────────────────────────────
BG   = "#1e1e2e"
CARD = "#181825"
BTN  = "#313244"
SEL  = "#45475a"
FG   = "#cdd6f4"
ACC  = "#f5de46"
GRN  = "#a6e3a1"
RED  = "#f38ba8"
MUT  = "#6c7086"
ORG  = "#fab387"   # orange — used for partial / warning status


# ── Pipeline helpers (mirrored from pipeline.py) ───────────────────────────────
def detect_filterable_columns(csv_path: Path):
    skip_patterns = ['_ID', '_DATE', '_AT', 'CREATED', 'UPDATED', 'START_', 'END_']
    skip_types    = {'INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'HUGEINT'}

    con       = duckdb.connect()
    csv_str   = str(csv_path).replace('\\', '/')
    schema    = con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{csv_str}')").fetchall()
    total_rows = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_str}')").fetchone()[0]

    filterable = []
    for col_name, col_type, *_ in schema:
        if any(p in col_name.upper() for p in skip_patterns):
            continue
        if col_type.upper().split('(')[0] in skip_types:
            continue
        distinct = con.execute(
            f'SELECT COUNT(DISTINCT "{col_name}") FROM read_csv_auto(\'{csv_str}\')'
        ).fetchone()[0]
        if 2 <= distinct <= 20 and distinct < total_rows:
            filterable.append(col_name)

    con.close()
    return filterable


def get_valid_values(csv_path: Path, field: str):
    con     = duckdb.connect()
    csv_str = str(csv_path).replace('\\', '/')
    rows    = con.execute(f"""
        SELECT "{field}", COUNT(*) AS records
        FROM read_csv_auto('{csv_str}')
        GROUP BY "{field}"
        ORDER BY "{field}"
    """).fetchall()
    con.close()
    return rows


# ── Application ────────────────────────────────────────────────────────────────
class KrakenApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Kraken Migration Accelerator")
        self.geometry("1150x700")
        self.minsize(900, 560)
        self.configure(bg=BG)

        self._output_queue     = queue.Queue()
        self._filter_vars     = {}   # col -> (StringVar, {display: raw_value})
        self._filter_cols     = []
        self._csv_path        = None
        self._mapping_data    = None
        self._edit_current_path = None
        self._cohort_plan       = CohortPlan()
        self._selected_cohort_id: int | None = None

        self._build_styles()
        self._build_ui()
        self._poll_queue()
        self.refresh_csv_list()

    # ── Styles ─────────────────────────────────────────────────────────────────
    def _build_styles(self):
        style = ttk.Style(self)
        style.theme_use("clam")

        style.configure("TNotebook",          background=BG,  borderwidth=0)
        style.configure("TNotebook.Tab",      background=BTN, foreground=FG,  padding=[14, 7])
        style.map("TNotebook.Tab",            background=[("selected", ACC)],
                                              foreground=[("selected", BG)],
                                              padding=[("selected", [14, 7])])
        style.configure("TFrame",             background=BG)
        style.configure("TLabel",             background=BG,  foreground=FG,  font=("Segoe UI", 10))
        style.configure("Muted.TLabel",       background=BG,  foreground=MUT, font=("Segoe UI", 9))
        style.configure("Head.TLabel",        background=BG,  foreground=ACC, font=("Segoe UI", 11, "bold"))
        style.configure("TButton",            background=BTN, foreground=FG,  font=("Segoe UI", 10),
                                              borderwidth=0, focusthickness=0, relief="flat", padding=[8, 5])
        style.map("TButton",                  background=[("active", SEL)])
        style.configure("Accent.TButton",     background=ACC, foreground=BG,  font=("Segoe UI", 10, "bold"),
                                              borderwidth=0, focusthickness=0, relief="flat", padding=[8, 5])
        style.map("Accent.TButton",           background=[("active", "#74c7ec")])
        style.configure("Danger.TButton",     background="#45475a", foreground=RED, font=("Segoe UI", 10),
                                              borderwidth=0, focusthickness=0, relief="flat", padding=[8, 5])
        style.map("Danger.TButton",           background=[("active", "#585b70")])
        style.configure("TCombobox",          fieldbackground=BTN, background=BTN,
                                              foreground=FG,  selectbackground=BTN,
                                              selectforeground=FG, arrowcolor=ACC)
        style.map("TCombobox",
                  fieldbackground=[  ("readonly", BTN),  ("disabled", CARD)],
                  foreground=[       ("readonly", FG),   ("disabled", MUT)],
                  selectbackground=[ ("readonly", BTN)],
                  selectforeground=[ ("readonly", FG)])
        # Style the popup listbox that Combobox creates
        self.option_add("*TCombobox*Listbox.background",       CARD)
        self.option_add("*TCombobox*Listbox.foreground",       FG)
        self.option_add("*TCombobox*Listbox.selectBackground", ACC)
        self.option_add("*TCombobox*Listbox.selectForeground", BG)
        style.configure("TEntry",             fieldbackground=BTN, foreground=FG,  insertcolor=FG, relief="flat")
        style.configure("TLabelframe",        background=BG,  bordercolor=SEL)
        style.configure("TLabelframe.Label",  background=BG,  foreground=ACC, font=("Segoe UI", 10, "bold"))
        style.configure("TSeparator",         background=SEL)
        style.configure("TScrollbar",         background=BTN, troughcolor=BG, arrowcolor=MUT, borderwidth=0)

    # ── Root layout ────────────────────────────────────────────────────────────
    def _build_ui(self):
        ttk.Label(self, text="  Kraken Migration Accelerator",
                  font=("Segoe UI", 14, "bold"),
                  foreground=ACC, background=BG).pack(fill=tk.X, padx=12, pady=(10, 4))

        ttk.Separator(self).pack(fill=tk.X)

        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)

        self._tab_maps = ttk.Frame(nb)
        self._tab_pipe = ttk.Frame(nb)
        self._tab_edit = ttk.Frame(nb)
        self._tab_cp   = ttk.Frame(nb)
        self._tab_load = ttk.Frame(nb)
        nb.add(self._tab_maps, text="  Generate Mappings  ")
        nb.add(self._tab_edit, text="  Edit Mappings  ")
        nb.add(self._tab_cp,   text="  Cohort Plan  ")
        nb.add(self._tab_pipe, text="  Run Pipeline  ")
        nb.add(self._tab_load, text="  Load to Kraken  ")

        self._build_mappings_tab()
        self._build_pipeline_tab()
        self._build_edit_tab()
        self._build_cohort_plan_tab()
        self._build_load_tab()

    # ── Tab 1 ──────────────────────────────────────────────────────────────────
    def _build_mappings_tab(self):
        # Left panel
        left = ttk.Frame(self._tab_maps, width=260)
        left.pack(side=tk.LEFT, fill=tk.Y, padx=12, pady=12)
        left.pack_propagate(False)

        ttk.Label(left, text="CSV Files in data/", style="Head.TLabel").pack(anchor="w", pady=(0, 6))

        list_box = tk.Frame(left, bg=BTN, bd=0)
        list_box.pack(fill=tk.BOTH, expand=True)

        self._csv_listbox = tk.Listbox(
            list_box, bg=CARD, fg=FG,
            selectbackground=ACC, selectforeground=BG,
            font=("Segoe UI", 10), borderwidth=0, highlightthickness=0,
            activestyle="none"
        )
        sb = ttk.Scrollbar(list_box, command=self._csv_listbox.yview)
        self._csv_listbox.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._csv_listbox.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

        btn_row = ttk.Frame(left)
        btn_row.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(btn_row, text="Upload CSV",
                   command=self.upload_csv,
                   style="Accent.TButton").pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        ttk.Button(btn_row, text="Remove",
                   command=self.remove_csv,
                   style="Danger.TButton").pack(side=tk.LEFT)

        ttk.Separator(left).pack(fill=tk.X, pady=12)

        ttk.Button(left, text="Run Generate Mappings",
                   command=self.run_generate_mappings,
                   style="Accent.TButton").pack(fill=tk.X)
        ttk.Label(left,
                  text="Analyses each CSV and writes YAML\nmapping templates for the pipeline.",
                  style="Muted.TLabel", wraplength=220, justify="left").pack(anchor="w", pady=(6, 0))

        # Right panel: log
        right = ttk.Frame(self._tab_maps)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 12), pady=12)

        hdr = ttk.Frame(right)
        hdr.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(hdr, text="Output", style="Head.TLabel").pack(side=tk.LEFT)
        ttk.Button(hdr, text="Clear",
                   command=lambda: self._clear_log(self._map_log)).pack(side=tk.RIGHT)

        self._map_log = ScrolledText(
            right, wrap=tk.WORD, state=tk.DISABLED,
            bg=CARD, fg=FG, font=("Consolas", 9),
            borderwidth=0, relief="flat", insertbackground=FG
        )
        self._map_log.pack(fill=tk.BOTH, expand=True)

    # ── Tab 2 ──────────────────────────────────────────────────────────────────
    def _build_pipeline_tab(self):
        # Left scrollable config panel
        left_outer = ttk.Frame(self._tab_pipe, width=320)
        left_outer.pack(side=tk.LEFT, fill=tk.Y, padx=12, pady=12)
        left_outer.pack_propagate(False)

        canvas = tk.Canvas(left_outer, bg=BG, highlightthickness=0)
        vsb    = ttk.Scrollbar(left_outer, orient="vertical", command=canvas.yview)
        self._pipe_inner = ttk.Frame(canvas)

        self._pipe_inner.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=self._pipe_inner, anchor="nw")
        canvas.configure(yscrollcommand=vsb.set)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        # Mouse-wheel scroll — scoped to this canvas only, clamped to content bounds
        def _on_mousewheel(event):
            top, bottom = canvas.yview()
            if event.delta > 0 and top <= 0:
                return
            if event.delta < 0 and bottom >= 1:
                return
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def _bind_wheel(e):   canvas.bind("<MouseWheel>", _on_mousewheel)
        def _unbind_wheel(e): canvas.unbind("<MouseWheel>")
        canvas.bind("<Enter>", _bind_wheel)
        canvas.bind("<Leave>", _unbind_wheel)
        self._pipe_inner.bind("<Enter>", _bind_wheel)
        self._pipe_inner.bind("<Leave>", _unbind_wheel)

        self._build_pipeline_form(self._pipe_inner)

        # Right log panel
        right = ttk.Frame(self._tab_pipe)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 12), pady=12)

        hdr = ttk.Frame(right)
        hdr.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(hdr, text="Output", style="Head.TLabel").pack(side=tk.LEFT)
        ttk.Button(hdr, text="Clear",
                   command=lambda: self._clear_log(self._pipe_log)).pack(side=tk.RIGHT)

        self._pipe_log = ScrolledText(
            right, wrap=tk.WORD, state=tk.DISABLED,
            bg=CARD, fg=FG, font=("Consolas", 9),
            borderwidth=0, relief="flat", insertbackground=FG
        )
        self._pipe_log.pack(fill=tk.BOTH, expand=True)

    def _build_pipeline_form(self, parent):
        pad = {"pady": (0, 4)}

        # ── Step 1 ──────────────────────────────────────────────────────────
        ttk.Label(parent, text="Step 1 — Select CSV", style="Head.TLabel").pack(anchor="w", **pad)
        self._pipe_csv_var = tk.StringVar()
        self._pipe_csv_cb  = ttk.Combobox(parent, textvariable=self._pipe_csv_var,
                                          state="readonly", width=32)
        self._pipe_csv_cb.pack(fill=tk.X)
        self._pipe_csv_cb.bind("<<ComboboxSelected>>", self._on_csv_selected)

        ttk.Button(parent, text="Upload CSV",
                   command=self._upload_csv_pipeline).pack(anchor="w", pady=(4, 0))

        ttk.Separator(parent).pack(fill=tk.X, pady=10)

        # ── Step 2 ──────────────────────────────────────────────────────────
        ttk.Label(parent, text="Step 2 — Select Entity Mapping", style="Head.TLabel").pack(anchor="w", **pad)
        self._pipe_map_var = tk.StringVar()
        self._pipe_map_cb  = ttk.Combobox(parent, textvariable=self._pipe_map_var,
                                          state="readonly", width=32)
        self._pipe_map_cb.pack(fill=tk.X)
        self._pipe_map_cb.bind("<<ComboboxSelected>>", self._on_mapping_selected)

        ttk.Separator(parent).pack(fill=tk.X, pady=10)

        # ── Step 3 ──────────────────────────────────────────────────────────
        ttk.Label(parent, text="Step 3 — Cohort Filters", style="Head.TLabel").pack(anchor="w", **pad)
        self._filter_hint = ttk.Label(parent,
                                      text="Select a CSV and mapping to load filters.",
                                      style="Muted.TLabel")
        self._filter_hint.pack(anchor="w")
        self._filter_frame = ttk.Frame(parent)
        self._filter_frame.pack(fill=tk.X)

        ttk.Separator(parent).pack(fill=tk.X, pady=10)

        # ── Step 4 ──────────────────────────────────────────────────────────
        ttk.Label(parent, text="Step 4 — Cohort Name", style="Head.TLabel").pack(anchor="w", **pad)
        self._cohort_name_var = tk.StringVar()
        ttk.Entry(parent, textvariable=self._cohort_name_var, width=32).pack(fill=tk.X)
        ttk.Label(parent, text='e.g. "Cohort 1: Victoria Billing"',
                  style="Muted.TLabel").pack(anchor="w", pady=(2, 0))

        ttk.Separator(parent).pack(fill=tk.X, pady=10)

        # ── Step 5 ──────────────────────────────────────────────────────────
        ttk.Label(parent, text="Step 5 — Run", style="Head.TLabel").pack(anchor="w", **pad)

        self._preview_label = ttk.Label(parent, text="", foreground=GRN)
        self._preview_label.pack(anchor="w", pady=(0, 6))

        ttk.Button(parent, text="Preview Row Count",
                   command=self.preview_cohort).pack(fill=tk.X, pady=(0, 6))
        ttk.Button(parent, text="Run Pipeline",
                   command=self.run_pipeline,
                   style="Accent.TButton").pack(fill=tk.X)

        ttk.Label(parent, text="").pack()  # bottom padding

    # ── Tab 3: Edit Mappings ────────────────────────────────────────────────────
    def _build_edit_tab(self):
        left = ttk.Frame(self._tab_edit, width=260)
        left.pack(side=tk.LEFT, fill=tk.Y, padx=12, pady=12)
        left.pack_propagate(False)

        ttk.Label(left, text="Select CSV", style="Head.TLabel").pack(anchor="w", pady=(0, 4))
        self._edit_csv_var = tk.StringVar()
        self._edit_csv_cb  = ttk.Combobox(left, textvariable=self._edit_csv_var,
                                           state="readonly", width=30)
        self._edit_csv_cb.pack(fill=tk.X)
        self._edit_csv_cb.bind("<<ComboboxSelected>>", self._on_edit_csv_selected)

        ttk.Separator(left).pack(fill=tk.X, pady=10)

        ttk.Label(left, text="Mapping Templates", style="Head.TLabel").pack(anchor="w", pady=(0, 4))

        list_box = tk.Frame(left, bg=BTN, bd=0)
        list_box.pack(fill=tk.BOTH, expand=True)

        self._edit_map_listbox = tk.Listbox(
            list_box, bg=CARD, fg=FG,
            selectbackground=ACC, selectforeground=BG,
            font=("Segoe UI", 10), borderwidth=0, highlightthickness=0,
            activestyle="none"
        )
        sb = ttk.Scrollbar(list_box, command=self._edit_map_listbox.yview)
        self._edit_map_listbox.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._edit_map_listbox.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        self._edit_map_listbox.bind("<<ListboxSelect>>", self._on_edit_map_selected)

        right = ttk.Frame(self._tab_edit)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 12), pady=12)

        hdr = ttk.Frame(right)
        hdr.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(hdr, text="YAML Editor", style="Head.TLabel").pack(side=tk.LEFT)
        self._edit_path_label = ttk.Label(hdr, text="", style="Muted.TLabel")
        self._edit_path_label.pack(side=tk.LEFT, padx=(10, 0))

        self._edit_text = tk.Text(
            right, wrap=tk.NONE,
            bg=CARD, fg=FG, font=("Consolas", 10),
            borderwidth=0, relief="flat", insertbackground=FG,
            selectbackground=ACC, selectforeground=BG,
            undo=True
        )
        xsb = ttk.Scrollbar(right, orient="horizontal", command=self._edit_text.xview)
        ysb = ttk.Scrollbar(right, orient="vertical",   command=self._edit_text.yview)
        self._edit_text.configure(xscrollcommand=xsb.set, yscrollcommand=ysb.set)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)
        self._edit_text.pack(fill=tk.BOTH, expand=True)

        footer = ttk.Frame(right)
        footer.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(footer, text="Save Mapping",
                   command=self.save_mapping,
                   style="Accent.TButton").pack(side=tk.LEFT)
        self._edit_status = ttk.Label(footer, text="", foreground=MUT,
                                      background=BG, font=("Segoe UI", 10))
        self._edit_status.pack(side=tk.LEFT, padx=(12, 0))

    def _refresh_edit_csv_list(self):
        files = sorted(p.name for p in DATA_DIR.glob("*.csv"))
        self._edit_csv_cb["values"] = files
        if self._edit_csv_var.get() not in files:
            self._edit_csv_cb.set("")
            self._edit_map_listbox.delete(0, tk.END)

    def _on_edit_csv_selected(self, _=None):
        name = self._edit_csv_var.get()
        if not name:
            return
        stem = Path(name).stem
        template_dir = TEMPLATES_DIR / stem
        maps = sorted(p.name for p in template_dir.glob("*.yml")) if template_dir.exists() else []
        self._edit_map_listbox.delete(0, tk.END)
        for m in maps:
            self._edit_map_listbox.insert(tk.END, m)
        self._edit_text.delete("1.0", tk.END)
        self._edit_path_label.configure(text="")
        self._edit_current_path = None
        if maps:
            self._edit_status.configure(
                text=f"{len(maps)} template(s) — click one to edit", foreground=MUT)
        else:
            self._edit_status.configure(
                text="No templates — run Generate Mappings first", foreground=RED)

    def _on_edit_map_selected(self, _=None):
        sel = self._edit_map_listbox.curselection()
        if not sel:
            return
        csv_name = self._edit_csv_var.get()
        if not csv_name:
            return
        map_name = self._edit_map_listbox.get(sel[0])
        path = TEMPLATES_DIR / Path(csv_name).stem / map_name
        try:
            content = path.read_text(encoding="utf-8")
            self._edit_text.delete("1.0", tk.END)
            self._edit_text.insert("1.0", content)
            self._edit_text.edit_reset()
            self._edit_current_path = path
            self._edit_path_label.configure(text=f"\u2014 {map_name}")
            self._edit_status.configure(text="", foreground=MUT)
        except Exception as exc:
            self._edit_status.configure(text=f"Load error: {exc}", foreground=RED)

    def save_mapping(self):
        if self._edit_current_path is None:
            messagebox.showwarning("No File", "Select a mapping template to edit first.")
            return
        content = self._edit_text.get("1.0", tk.END)
        try:
            yaml.safe_load(content)
        except yaml.YAMLError as exc:
            self._edit_status.configure(text=f"YAML error: {exc}", foreground=RED)
            return
        self._edit_current_path.write_text(content, encoding="utf-8")
        self._edit_status.configure(text="Saved successfully.", foreground=GRN)
        # If this file is currently loaded in the pipeline tab, hot-reload it
        if (self._mapping_data is not None
                and self._pipe_map_var.get() == self._edit_current_path.name
                and self._csv_path is not None
                and self._csv_path.stem == self._edit_current_path.parent.name):
            self._mapping_data = yaml.safe_load(content)

    # ── Tab: Cohort Plan ──────────────────────────────────────────────────────
    def _build_cohort_plan_tab(self):
        """
        Cohort Plan tab — two-panel layout:
          Left  : cohort list + plan-level controls
          Right : selected cohort detail, rejection summary, run controls, log
        """
        # ── Left panel ───────────────────────────────────────────────────────
        left = ttk.Frame(self._tab_cp, width=290)
        left.pack(side=tk.LEFT, fill=tk.Y, padx=12, pady=12)
        left.pack_propagate(False)

        ttk.Label(left, text="Cohort Plan", style="Head.TLabel").pack(anchor="w", pady=(0, 6))

        # Dataset / Mapping display
        info_frame = ttk.Frame(left)
        info_frame.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(info_frame, text="Dataset:", style="Muted.TLabel").grid(
            row=0, column=0, sticky="w", padx=(0, 4))
        self._cp_dataset_lbl = ttk.Label(info_frame, text="—", wraplength=190,
                                          font=("Segoe UI", 9))
        self._cp_dataset_lbl.grid(row=0, column=1, sticky="w")
        ttk.Label(info_frame, text="Mapping:", style="Muted.TLabel").grid(
            row=1, column=0, sticky="w", padx=(0, 4))
        self._cp_mapping_lbl = ttk.Label(info_frame, text="—", wraplength=190,
                                          font=("Segoe UI", 9))
        self._cp_mapping_lbl.grid(row=1, column=1, sticky="w")

        ttk.Separator(left).pack(fill=tk.X, pady=8)

        # Action buttons
        btn_grid = ttk.Frame(left)
        btn_grid.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(btn_grid, text="Download Template",
                   command=self._download_cohort_template).pack(fill=tk.X, pady=(0, 4))
        ttk.Button(btn_grid, text="Import Plan (CSV/XLSX)",
                   command=self._import_cohort_plan).pack(fill=tk.X, pady=(0, 4))
        ttk.Button(btn_grid, text="Create Cohort from Selection File",
                   command=self._create_cohort_from_selection_file).pack(fill=tk.X, pady=(0, 4))
        ttk.Button(btn_grid, text="Add Blank Cohort",
                   command=self._add_blank_cohort).pack(fill=tk.X, pady=(0, 4))
        ttk.Button(btn_grid, text="Refresh Plan",
                   command=self._refresh_cohort_list).pack(fill=tk.X, pady=(0, 4))
        ttk.Button(btn_grid, text="Export to PowerBI",
                   command=self._export_powerbi_recon,
                   style="Accent.TButton").pack(fill=tk.X)

        ttk.Separator(left).pack(fill=tk.X, pady=8)

        # Cohort list
        self._cp_list_hdr = ttk.Label(left, text="Cohorts", style="Head.TLabel")
        self._cp_list_hdr.pack(anchor="w", pady=(0, 4))

        list_box = tk.Frame(left, bg=BTN, bd=0)
        list_box.pack(fill=tk.BOTH, expand=True)

        self._cp_listbox = tk.Listbox(
            list_box, bg=CARD, fg=FG,
            selectbackground=ACC, selectforeground=BG,
            font=("Consolas", 9), borderwidth=0, highlightthickness=0,
            activestyle="none"
        )
        sb = ttk.Scrollbar(list_box, command=self._cp_listbox.yview)
        self._cp_listbox.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._cp_listbox.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        self._cp_listbox.bind("<<ListboxSelect>>", self._on_cp_cohort_selected)

        # ── Right panel ──────────────────────────────────────────────────────
        right = ttk.Frame(self._tab_cp)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 12), pady=12)

        # Top: cohort detail grid
        detail = ttk.Frame(right)
        detail.pack(fill=tk.X, pady=(0, 8))

        def lbl(parent, text, col, row, style="Muted.TLabel", colspan=1):
            ttk.Label(parent, text=text, style=style).grid(
                row=row, column=col, sticky="w", padx=(0, 8), pady=1,
                columnspan=colspan)

        lbl(detail, "ID",       0, 0)
        lbl(detail, "Name",     1, 0)
        lbl(detail, "Version",  3, 0)
        lbl(detail, "Status",   4, 0)
        lbl(detail, "Priority", 5, 0)

        self._cp_id_lbl       = ttk.Label(detail, text="—", font=("Segoe UI", 10, "bold"))
        self._cp_name_lbl     = ttk.Label(detail, text="—", font=("Segoe UI", 10, "bold"),
                                           wraplength=340)
        self._cp_ver_lbl      = ttk.Label(detail, text="—", font=("Segoe UI", 10))
        self._cp_status_lbl   = ttk.Label(detail, text="—", font=("Segoe UI", 10, "bold"))
        self._cp_priority_lbl = ttk.Label(detail, text="—", font=("Segoe UI", 10))

        self._cp_id_lbl.grid      (row=1, column=0, sticky="w", padx=(0, 8))
        self._cp_name_lbl.grid    (row=1, column=1, sticky="w", padx=(0, 8), columnspan=2)
        self._cp_ver_lbl.grid     (row=1, column=3, sticky="w", padx=(0, 8))
        self._cp_status_lbl.grid  (row=1, column=4, sticky="w", padx=(0, 8))
        self._cp_priority_lbl.grid(row=1, column=5, sticky="w")

        ttk.Separator(right).pack(fill=tk.X, pady=(0, 6))

        # Description
        desc_frame = ttk.Frame(right)
        desc_frame.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(desc_frame, text="Description:", style="Muted.TLabel").pack(anchor="w")
        self._cp_desc_lbl = ttk.Label(desc_frame, text="—", wraplength=680,
                                       justify="left", font=("Segoe UI", 9))
        self._cp_desc_lbl.pack(anchor="w", padx=(8, 0))

        # Filters
        filter_frame = ttk.Frame(right)
        filter_frame.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(filter_frame, text="Filters:", style="Muted.TLabel").pack(anchor="w")
        self._cp_filter_lbl = ttk.Label(filter_frame, text="—", wraplength=680,
                                         justify="left", font=("Consolas", 9),
                                         foreground=ACC)
        self._cp_filter_lbl.pack(anchor="w", padx=(8, 0))

        # Estimated / target rows + Notes in a grid
        meta_frame = ttk.Frame(right)
        meta_frame.pack(fill=tk.X, pady=(0, 6))

        ttk.Label(meta_frame, text="Estimated rows:", style="Muted.TLabel").grid(
            row=0, column=0, sticky="w", padx=(0, 8))
        self._cp_est_lbl = ttk.Label(meta_frame, text="—", font=("Segoe UI", 9))
        self._cp_est_lbl.grid(row=0, column=1, sticky="w", padx=(0, 16))

        ttk.Button(meta_frame, text="Calculate",
                   command=self._estimate_cp_rows).grid(row=0, column=2, sticky="w")

        ttk.Label(meta_frame, text="Target rows:", style="Muted.TLabel").grid(
            row=0, column=3, sticky="w", padx=(16, 8))
        self._cp_target_lbl = ttk.Label(meta_frame, text="—", font=("Segoe UI", 9))
        self._cp_target_lbl.grid(row=0, column=4, sticky="w")

        ttk.Label(meta_frame, text="Notes:", style="Muted.TLabel").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=(4, 0))
        self._cp_notes_lbl = ttk.Label(meta_frame, text="—", wraplength=600,
                                        justify="left", font=("Segoe UI", 9))
        self._cp_notes_lbl.grid(row=1, column=1, sticky="w", pady=(4, 0), columnspan=4)

        ttk.Separator(right).pack(fill=tk.X, pady=6)

        # Rejection summary (last run)
        rej_frame = ttk.Frame(right)
        rej_frame.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(rej_frame, text="Rejection Summary (last run):",
                  style="Muted.TLabel").pack(anchor="w")
        self._cp_rej_lbl = ttk.Label(rej_frame, text="—", font=("Consolas", 9),
                                      foreground=RED, justify="left", wraplength=680)
        self._cp_rej_lbl.pack(anchor="w", padx=(8, 0))

        ttk.Separator(right).pack(fill=tk.X, pady=6)

        # Action row
        btn_row = ttk.Frame(right)
        btn_row.pack(fill=tk.X, pady=(0, 8))
        ttk.Button(btn_row, text="▶  Run This Cohort",
               command=self._run_cohort_from_plan,
               style="Accent.TButton").pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_row, text="✓  Mark Complete",
               command=lambda: self._set_cohort_status("complete")).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_row, text="⊘  Skip",
               command=lambda: self._set_cohort_status("skipped")).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_row, text="↺  Reset to Pending",
               command=lambda: self._set_cohort_status("pending")).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_row, text="🗑  Delete",
               command=self._delete_selected_cohort,
               style="Danger.TButton").pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_row, text="✎  Edit",
               command=self._edit_selected_cohort,
               style="Accent.TButton").pack(side=tk.LEFT)
    
        # Output log
        log_hdr = ttk.Frame(right)
        log_hdr.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(log_hdr, text="Pipeline Output", style="Head.TLabel").pack(side=tk.LEFT)
        ttk.Button(log_hdr, text="Clear",
                   command=lambda: self._clear_log(self._cp_log)).pack(side=tk.RIGHT)

        self._cp_log = ScrolledText(
            right, wrap=tk.WORD, state=tk.DISABLED,
            bg=CARD, fg=FG, font=("Consolas", 9),
            borderwidth=0, relief="flat", insertbackground=FG
        )
        self._cp_log.pack(fill=tk.BOTH, expand=True)

        # Load the plan on startup
        self._refresh_cohort_list()

    # ── Cohort Plan helpers ───────────────────────────────────────────────────

    _STATUS_FG = {
        "pending":  MUT,
        "running":  ACC,
        "complete": GRN,
        "failed":   RED,
        "partial":  ORG,
        "skipped":  MUT,
    }

    _STATUS_ICON = {
        "pending":  "○",
        "running":  "◉",
        "complete": "●",
        "failed":   "✕",
        "partial":  "◑",
        "skipped":  "—",
    }

    def _refresh_cohort_list(self):
        plan = self._cohort_plan.load()
        self._cp_dataset_lbl.configure(text=plan.get("dataset") or "—")
        self._cp_mapping_lbl.configure(text=plan.get("mapping") or "—")

        cohorts = plan.get("cohorts", [])
        self._cp_list_hdr.configure(text=f"Cohorts  ({len(cohorts)})")
        self._cp_listbox.delete(0, tk.END)

        counts = {"pending": 0, "running": 0, "complete": 0,
                  "failed": 0, "partial": 0, "skipped": 0}

        for c in cohorts:
            status = c.get("status", "pending")
            counts[status] = counts.get(status, 0) + 1
            icon  = self._STATUS_ICON.get(status, "○")
            label = f"{icon} [{c['cohort_id']}] {c['name']}"
            self._cp_listbox.insert(tk.END, label)

        # Colour each row
        for i, c in enumerate(cohorts):
            status = c.get("status", "pending")
            fg     = self._STATUS_FG.get(status, MUT)
            self._cp_listbox.itemconfigure(i, foreground=fg)

        # Summary in header
        done  = counts["complete"]
        total = len(cohorts)
        pct   = f"  {done}/{total} complete" if total else ""
        self._cp_list_hdr.configure(text=f"Cohorts  ({total}){pct}")

    def _on_cp_cohort_selected(self, _=None):
        sel = self._cp_listbox.curselection()
        if not sel:
            return
        plan    = self._cohort_plan.load()
        cohorts = plan.get("cohorts", [])
        if sel[0] >= len(cohorts):
            return
        c = cohorts[sel[0]]
        self._selected_cohort_id = c["cohort_id"]
        self._populate_cohort_detail(c)

    def _populate_cohort_detail(self, c: dict):
        status = c.get("status", "pending")
        fg     = self._STATUS_FG.get(status, MUT)

        self._cp_id_lbl.configure      (text=str(c["cohort_id"]))
        self._cp_name_lbl.configure    (text=c.get("name", "—"))
        self._cp_ver_lbl.configure     (text=f"v{c.get('version', 1)}")
        self._cp_status_lbl.configure  (text=status.upper(), foreground=fg)
        self._cp_priority_lbl.configure(text=f"Priority {c.get('priority', '—')}")
        self._cp_desc_lbl.configure    (text=c.get("description") or "—")
        self._cp_notes_lbl.configure   (text=c.get("notes") or "—")

        # Filters as human-readable text
        filters = c.get("filters", [])
        if filters:
            parts = []
            for f in filters:
                op = f.get("operator", "=")
                if op.upper() in ("IN", "NOT IN"):
                    vals = ", ".join(str(v) for v in f.get("values", []))
                    parts.append(f'{f["field"]} {op} ({vals})')
                elif op.upper() in ("IS NULL", "IS NOT NULL"):
                    parts.append(f'{f["field"]} {op}')
                else:
                    parts.append(f'{f["field"]} {op} {f.get("value", "")}')
            self._cp_filter_lbl.configure(text="\n".join(parts))
        else:
            self._cp_filter_lbl.configure(text="(no filters — full dataset)")

        # Estimated / target rows
        est = c.get("estimated_rows")
        self._cp_est_lbl.configure(text=f"{est:,}" if isinstance(est, int) else "—")
        tgt = c.get("target_rows")
        self._cp_target_lbl.configure(text=f"{tgt:,}" if isinstance(tgt, int) else "—")

        # Rejection summary
        rej_rows = self._cohort_plan.get_rejection_detail(c["cohort_id"])
        if rej_rows:
            lines = [f"  {reason}: {cnt:,} rows ({pct}%)"
                     for reason, cnt, pct in rej_rows]
            self._cp_rej_lbl.configure(text="\n".join(lines), foreground=RED)
        else:
            self._cp_rej_lbl.configure(text="No rejection data yet.", foreground=MUT)

    def _estimate_cp_rows(self):
        cid = self._selected_cohort_id
        if cid is None:
            messagebox.showwarning("No Cohort", "Select a cohort first.")
            return
        plan    = self._cohort_plan.load()
        dataset = plan.get("dataset")
        if not dataset:
            messagebox.showwarning("No Dataset", "The cohort plan has no dataset configured.")
            return
        csv_path = DATA_DIR / dataset
        if not csv_path.exists():
            messagebox.showwarning("File Not Found", f"Dataset not found:\n{csv_path}")
            return
        c       = self._cohort_plan.get_cohort(cid)
        filters = c.get("filters", [])

        self._cp_est_lbl.configure(text="Calculating…")

        def _worker():
            try:
                count = self._cohort_plan.estimate_rows(csv_path, filters)
                self._cohort_plan.update_status(cid, c.get("status", "pending"),
                                                estimated_rows=count)
                self._q("cp_est", count)
            except Exception as exc:
                self._q("cp_est_err", str(exc))

        threading.Thread(target=_worker, daemon=True).start()

    def _set_cohort_status(self, new_status: str):
        cid = self._selected_cohort_id
        if cid is None:
            messagebox.showwarning("No Cohort", "Select a cohort first.")
            return
        self._cohort_plan.update_status(cid, new_status)
        self._refresh_cohort_list()
        # Re-select and refresh detail
        c = self._cohort_plan.get_cohort(cid)
        if c:
            self._populate_cohort_detail(c)

    def _download_cohort_template(self):
        """Generate and save a cohort plan CSV template based on the active dataset."""
        # Determine the dataset CSV to derive filter columns from
        dataset_name = self._pipe_csv_var.get() if hasattr(self, '_pipe_csv_var') else ""
        filter_cols: list[str] = []

        if dataset_name:
            csv_path = DATA_DIR / dataset_name
            if csv_path.exists():
                try:
                    filter_cols = detect_filterable_columns(csv_path)
                except Exception:
                    filter_cols = []

        if not filter_cols:
            # Fallback: let user pick a CSV to derive columns, or use empty template
            pick = filedialog.askopenfilename(
                title="Select Dataset CSV to derive filter columns (or Cancel for blank template)",
                initialdir=str(DATA_DIR),
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            )
            if pick:
                try:
                    filter_cols = detect_filterable_columns(Path(pick))
                except Exception:
                    filter_cols = []

        save_path = filedialog.asksaveasfilename(
            title="Save Cohort Plan Template",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile="cohort_plan_template.csv",
        )
        if not save_path:
            return

        import csv as _csv
        base_cols = ["cohort_id", "name", "priority", "description", "target_rows", "notes"]
        all_cols  = base_cols + filter_cols
        num_cols  = len(all_cols)

        def _pad(row: list) -> list:
            """Extend row to full column width."""
            return row + [""] * (num_cols - len(row))

        filter_hint = ", ".join(filter_cols) if filter_cols else "(no filter columns detected)"

        comment_rows = [
            _pad(["# HOW TO USE THIS TEMPLATE"]),
            _pad(["# - Fill in one row per cohort below the EXAMPLE rows. "
                  "The EXAMPLE and # rows are ignored on import."]),
            _pad(["# - cohort_id: unique number for each cohort (1 2 3 ..."]),
            _pad(["# - name: short display name shown in the tool"]),
            _pad(["# - priority: processing order — 1 = run first"]),
            _pad(["# - description: free-text explanation of this cohort"]),
            _pad(["# - target_rows: the expected/agreed record count (leave blank if unknown)"]),
            _pad(["# - notes: any sign-off or dependency notes"]),
            _pad([f"# - FILTER COLUMNS ({filter_hint} and any others you add):"]),
            _pad(["#   Leave blank or write * to apply no filter on that column."]),
            _pad(["#   Single value  →  exact match  e.g.  VIC"]),
            _pad(["# - Comma-separated values → IN filter e.g. VIC,NSW "
                  "(quote the cell in Excel if value contains a comma)"]),
            _pad(["# - You can ADD extra filter columns by inserting new column headers to the right."]),
            _pad(["# - Save as CSV and use Import Plan (CSV) in the Cohort Plan tab."]),
        ]

        example_rows = [
            _pad(["EXAMPLE 1", "Cohort 1 — First Batch", "1",
                  "First cohort to validate the end-to-end process.",
                  "25000", "Signed off " + datetime.now().strftime("%Y-%m-%d")]),
            _pad(["EXAMPLE 2", "Cohort 2 — Second Batch", "2",
                  "Second batch.", "18000", ""]),
        ]

        data_rows = [_pad([str(i), "", str(i), "", "", ""]) for i in range(1, 11)]

        try:
            with open(save_path, "w", newline="", encoding="utf-8") as f:
                writer = _csv.writer(f)
                writer.writerow(all_cols)
                writer.writerows(comment_rows)
                writer.writerows(example_rows)
                writer.writerows(data_rows)
            messagebox.showinfo(
                "Template Saved",
                f"Template saved to:\n{save_path}\n\n"
                "Fill in rows 1–10 (or add more), then use \'Import Plan\' to load it.",
            )
        except Exception as exc:
            messagebox.showerror("Save Failed", str(exc))

    def _import_cohort_plan(self):
        path = filedialog.askopenfilename(
            title="Import Cohort Plan",
            filetypes=[
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return

        # Ask for dataset and mapping to associate with the plan
        dataset = self._pipe_csv_var.get() or ""
        mapping = self._pipe_map_var.get() or ""

        try:
            plan = CohortPlan.from_csv_import(path, dataset=dataset, mapping=mapping)
            self._cohort_plan.save(plan)
            self._refresh_cohort_list()
            self._q("cp",
                    f"Imported {len(plan['cohorts'])} cohort(s) from "
                    f"{Path(path).name}\n")
            messagebox.showinfo(
                "Import Complete",
                f"Imported {len(plan['cohorts'])} cohort(s) from {Path(path).name}.\n\n"
                "Review the list and run each cohort in priority order.",
            )
        except Exception as exc:
            messagebox.showerror("Import Failed", str(exc))

    def _add_blank_cohort(self):
        name = simpledialog.askstring(
            "Add Blank Cohort",
            "Cohort name:",
            parent=self,
        )
        if not name:
            return

        description = simpledialog.askstring(
            "Add Blank Cohort",
            "Description (optional):",
            parent=self,
        ) or ""

        priority = self._ask_integer(
            "Cohort Priority",
            "Priority (1 = highest):",
            default=1,
            allow_empty=True,
        ) or 1

        target_rows = self._ask_integer(
            "Target Rows",
            "Target row count (optional):",
            default=None,
            allow_empty=True,
        )

        notes = simpledialog.askstring(
            "Add Blank Cohort",
            "Notes (optional):",
            parent=self,
        ) or ""

        cohort = {
            "cohort_id":      self._cohort_plan.next_available_id(),
            "name":           name.strip(),
            "version":        1,
            "description":    description.strip(),
            "priority":       priority,
            "status":         "pending",
            "filters":        [],
            "estimated_rows": None,
            "target_rows":    target_rows,
            "notes":          notes.strip(),
        }
        self._cohort_plan.add_cohort(cohort)
        self._refresh_cohort_list()
        self._q("cp", f"Added cohort: {cohort['name']}\n")

    def _create_cohort_from_selection_file(self):
        plan = self._cohort_plan.load()
        dataset = plan.get("dataset")
        mapping = plan.get("mapping")
        if not dataset or not mapping:
            messagebox.showwarning(
                "Missing Plan Context",
                "Dataset and mapping must be set in the cohort plan before importing a selection file.",
            )
            return

        csv_path = DATA_DIR / dataset
        if not csv_path.exists():
            messagebox.showerror("File Not Found", f"Dataset not found: {csv_path}")
            return

        selection_path = filedialog.askopenfilename(
            title="Select row selection CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not selection_path:
            return

        try:
            sel_headers, sel_rows = self._read_csv_rows(selection_path)
        except Exception as exc:
            messagebox.showerror("Read Failed", str(exc))
            return

        if not sel_headers:
            messagebox.showerror("Invalid File", "The selection file has no headers.")
            return

        try:
            dataset_headers = self._get_csv_headers(csv_path)
        except Exception as exc:
            messagebox.showerror("Read Failed", str(exc))
            return

        common = self._match_columns(dataset_headers, sel_headers)
        if not common:
            messagebox.showerror(
                "No Matching Columns",
                "No common columns were found between the dataset and the selection file.\n"
                "Make sure the selection CSV contains at least one matching key column.",
            )
            return

        if len(common) == 1:
            dataset_col, selection_col = common[0]
        else:
            choice = self._ask_choice(
                "Select Join Column",
                "Select the column that identifies rows in both the dataset and selection file:",
                [f"{ds}  (selection: {sel})" for ds, sel in common],
            )
            if not choice:
                return
            idx = [f"{ds}  (selection: {sel})" for ds, sel in common].index(choice)
            dataset_col, selection_col = common[idx]

        values = {
            (row.get(selection_col) or "").strip()
            for row in sel_rows
            if (row.get(selection_col) or "").strip()
        }
        if not values:
            messagebox.showerror(
                "No Values Found",
                f"No non-empty values found for column '{selection_col}' in the selection file.",
            )
            return

        if len(values) > 10000:
            if not messagebox.askyesno(
                "Large Selection",
                f"The selection file contains {len(values):,} unique values for '{selection_col}'.\n"
                "This may produce a very large cohort filter. Continue?",
            ):
                return

        cohort_name = simpledialog.askstring(
            "Manual Cohort Name",
            "Cohort name:",
            initialvalue=f"Manual cohort — {Path(selection_path).stem}",
            parent=self,
        )
        if not cohort_name:
            return

        description = simpledialog.askstring(
            "Manual Cohort Description",
            "Description (optional):",
            parent=self,
        ) or ""

        priority = self._ask_integer(
            "Cohort Priority",
            "Priority (1 = highest):",
            default=1,
            allow_empty=True,
        ) or 1

        notes = simpledialog.askstring(
            "Manual Cohort Notes",
            "Notes (optional):",
            parent=self,
        ) or ""

        operator = "=" if len(values) == 1 else "IN"
        filters = [
            {
                "field": dataset_col,
                "operator": operator,
                "value": next(iter(values)) if operator == "=" else None,
                "values": sorted(values) if operator == "IN" else None,
            }
        ]
        try:
            estimated_rows = self._cohort_plan.estimate_rows(csv_path, filters)
        except Exception:
            estimated_rows = None

        cohort = {
            "cohort_id":      self._cohort_plan.next_available_id(),
            "name":           cohort_name.strip(),
            "version":        1,
            "description":    description.strip(),
            "priority":       priority,
            "status":         "pending",
            "filters":        filters,
            "estimated_rows": estimated_rows,
            "target_rows":    None,
            "notes":          notes.strip() or f"Imported from selection file: {Path(selection_path).name}",
        }
        self._cohort_plan.add_cohort(cohort)
        self._refresh_cohort_list()
        self._q("cp",
                f"Created manual cohort '{cohort['name']}' from {Path(selection_path).name}\n")
        messagebox.showinfo(
            "Cohort Created",
            f"Manual cohort '{cohort['name']}' added with {len(values):,} unique selection values.\n"
            f"Dataset key column: {dataset_col}",
        )

    def _get_csv_headers(self, csv_path: Path) -> list[str]:
        import csv as _csv
        with open(csv_path, newline='', encoding='utf-8-sig') as f:
            reader = _csv.DictReader(f)
            return [h for h in (reader.fieldnames or []) if h is not None]

    def _read_csv_rows(self, csv_path: str | Path) -> tuple[list[str], list[dict[str, str]]]:
        import csv as _csv
        with open(csv_path, newline='', encoding='utf-8-sig') as f:
            reader = _csv.DictReader(f)
            headers = [h for h in (reader.fieldnames or []) if h is not None]
            rows = [row for row in reader]
        return headers, rows

    def _normalize_header(self, header: str) -> str:
        return "".join(ch for ch in header.lower() if ch.isalnum())

    def _match_columns(self, dataset_headers: list[str], selection_headers: list[str]) -> list[tuple[str, str]]:
        dataset_map = {self._normalize_header(h): h for h in dataset_headers}
        selection_map = {self._normalize_header(h): h for h in selection_headers}

        # Exact normalized match first
        exact = sorted(set(dataset_map) & set(selection_map))
        if exact:
            return [(dataset_map[name], selection_map[name]) for name in exact]

        # Fallback: allow short-selection headers like ID to match dataset headers ending in ID
        matches: list[tuple[str, str]] = []
        for sel_norm, sel_orig in selection_map.items():
            for ds_norm, ds_orig in dataset_map.items():
                if sel_norm == ds_norm:
                    continue
                if ds_norm.endswith(sel_norm) or sel_norm.endswith(ds_norm):
                    matches.append((ds_orig, sel_orig))
        return matches

    def _ask_choice(self, title: str, prompt: str, options: list[str]) -> str | None:
        dlg = tk.Toplevel(self)
        dlg.title(title)
        dlg.transient(self)
        dlg.grab_set()

        ttk.Label(dlg, text=prompt, wraplength=420).pack(fill=tk.X, padx=12, pady=(12, 4))
        listbox = tk.Listbox(dlg, selectmode=tk.SINGLE, activestyle='none')
        for opt in options:
            listbox.insert(tk.END, opt)
        listbox.pack(fill=tk.BOTH, expand=True, padx=12, pady=(0, 12))

        result = {"value": None}

        def on_ok():
            sel = listbox.curselection()
            if not sel:
                messagebox.showwarning("Selection Required", "Choose one item from the list.", parent=dlg)
                return
            result["value"] = options[sel[0]]
            dlg.destroy()

        def on_cancel():
            dlg.destroy()

        button_frame = ttk.Frame(dlg)
        button_frame.pack(fill=tk.X, padx=12, pady=(0, 12))
        ttk.Button(button_frame, text="Cancel", command=on_cancel).pack(side=tk.RIGHT)
        ttk.Button(button_frame, text="OK", command=on_ok).pack(side=tk.RIGHT, padx=(0, 4))

        dlg.wait_window()
        return result["value"]

    def _ask_integer(
        self,
        title: str,
        prompt: str,
        default: int | None = None,
        allow_empty: bool = False,
    ) -> int | None:
        while True:
            initial = str(default) if default is not None else ""
            response = simpledialog.askstring(title, prompt, initialvalue=initial, parent=self)
            if response is None:
                return None
            response = response.strip()
            if response == "":
                return None if allow_empty else default
            if response.lstrip("+-").isdigit():
                return int(response)
            messagebox.showwarning(title, "Please enter a whole number or leave blank.", parent=self)

    def _export_powerbi_recon(self):
        try:
            self._cohort_plan.export_powerbi()
            self._q("cp", "PowerBI reconciliation CSVs exported to powerbi/\n")
            messagebox.showinfo(
                "Export Complete",
                "Reconciliation data written to powerbi/:\n"
                "  • cohort_plan.csv\n"
                "  • cohort_run_log.csv\n"
                "  • rejection_breakdown.csv\n"
                "  • cohort_history.csv",
            )
        except Exception as exc:
            messagebox.showerror("Export Failed", str(exc))

    def _run_cohort_from_plan(self):
        cid = self._selected_cohort_id
        if cid is None:
            messagebox.showwarning("No Cohort", "Select a cohort to run.")
            return
        plan    = self._cohort_plan.load()
        c       = self._cohort_plan.get_cohort(cid)
        dataset = plan.get("dataset")
        mapping = plan.get("mapping")

        if not dataset:
            messagebox.showwarning("No Dataset",
                                   "The cohort plan has no dataset. "
                                   "Import a plan or set the dataset first.")
            return
        if not mapping:
            messagebox.showwarning("No Mapping",
                                   "The cohort plan has no mapping template. "
                                   "Import a plan or set the mapping first.")
            return

        csv_path = DATA_DIR / dataset
        if not csv_path.exists():
            messagebox.showerror("File Not Found",
                                 f"Dataset not found in data/:\n{dataset}")
            return

        map_path = TEMPLATES_DIR / csv_path.stem / mapping
        if not map_path.exists():
            messagebox.showerror("Mapping Not Found",
                                 f"Mapping template not found:\n{map_path}")
            return

        with open(map_path, encoding="utf-8") as f:
            mapping_data = yaml.safe_load(f)

        filters  = c.get("filters", [])
        override = {
            "cohort_id":    cid,
            "version":      c.get("version", 1),
            "name":         c["name"],
            "csv_path":     csv_path,
            "mapping_data": mapping_data,
            "mapping_file": mapping,
            "filters":      filters,   # new list format
        }

        self._cohort_plan.update_status(cid, "running")
        self._refresh_cohort_list()
        threading.Thread(target=self._worker_pipeline,
                         args=(override,), daemon=True).start()

    # ── Tab 4: Load to Kraken ─────────────────────────────────────────────────
    def _build_load_tab(self):
        left = ttk.Frame(self._tab_load, width=320)
        left.pack(side=tk.LEFT, fill=tk.Y, padx=12, pady=12)
        left.pack_propagate(False)

        ttk.Label(left, text="Kraken API Settings", style="Head.TLabel").pack(anchor="w", pady=(0, 8))

        ttk.Label(left, text="API Endpoint URL").pack(anchor="w")
        self._load_url_var = tk.StringVar(value="https://your-kraken-endpoint/graphql")
        ttk.Entry(left, textvariable=self._load_url_var, width=36).pack(fill=tk.X, pady=(2, 10))

        ttk.Label(left, text="Bearer Token").pack(anchor="w")
        self._load_token_var = tk.StringVar()
        ttk.Entry(left, textvariable=self._load_token_var, show="*", width=36).pack(fill=tk.X, pady=(2, 10))

        ttk.Separator(left).pack(fill=tk.X, pady=10)

        ttk.Label(left, text="Data to Load", style="Head.TLabel").pack(anchor="w", pady=(0, 6))
        self._load_row_count_label = ttk.Label(left, text="", style="Muted.TLabel")
        self._load_row_count_label.pack(anchor="w", pady=(0, 6))
        ttk.Button(left, text="Refresh Row Count",
                   command=self._load_refresh_count).pack(fill=tk.X, pady=(0, 10))

        ttk.Separator(left).pack(fill=tk.X, pady=10)

        ttk.Button(left, text="Load to Kraken",
                   command=self.run_load_kraken,
                   style="Accent.TButton").pack(fill=tk.X)
        ttk.Label(left,
                  text="Reads output.output_data and POSTs\neach row to the Kraken API endpoint.",
                  style="Muted.TLabel", wraplength=280, justify="left").pack(anchor="w", pady=(6, 0))

        right = ttk.Frame(self._tab_load)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 12), pady=12)

        hdr = ttk.Frame(right)
        hdr.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(hdr, text="Output", style="Head.TLabel").pack(side=tk.LEFT)
        ttk.Button(hdr, text="Clear",
                   command=lambda: self._clear_log(self._load_log)).pack(side=tk.RIGHT)

        self._load_log = ScrolledText(
            right, wrap=tk.WORD, state=tk.DISABLED,
            bg=CARD, fg=FG, font=("Consolas", 9),
            borderwidth=0, relief="flat", insertbackground=FG
        )
        self._load_log.pack(fill=tk.BOTH, expand=True)

    def _load_refresh_count(self):
        try:
            con   = duckdb.connect(str(DB_PATH))
            count = con.execute("SELECT COUNT(*) FROM output.output_data").fetchone()[0]
            con.close()
            self._load_row_count_label.configure(
                text=f"{count:,} rows ready in output.output_data", foreground=GRN)
        except Exception as exc:
            self._load_row_count_label.configure(
                text=f"Could not read output data: {exc}", foreground=RED)

    def run_load_kraken(self):
        url   = self._load_url_var.get().strip()
        token = self._load_token_var.get().strip()
        if not url:
            messagebox.showwarning("Missing Input", "Please enter the Kraken API endpoint URL.")
            return
        if not token:
            if not messagebox.askyesno("No Token",
                                       "No bearer token entered. Continue anyway?"):
                return
        self._q("load", "=" * 52 + "\n")
        self._q("load", f"  Endpoint: {url}\n")
        self._q("load", "=" * 52 + "\n\n")
        threading.Thread(target=self._worker_load_kraken,
                         args=(url, token), daemon=True).start()

    def _worker_load_kraken(self, url: str, token: str):
        try:
            con  = duckdb.connect(str(DB_PATH))
            rows = con.execute("SELECT * FROM output.output_data").fetchall()
            cols = [d[0] for d in con.description]
            con.close()
        except Exception as exc:
            self._q("load", f"  ERROR reading output data: {exc}\n")
            return

        self._q("load", f"  {len(rows):,} rows to load\n\n")

        import requests as req
        mutation = """
        mutation CreateAccount($input: CreateAccountInput!) {
            createAccount(input: $input) { id }
        }
        """
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        success = failed = 0
        for i, row in enumerate(rows, 1):
            record  = {k: v for k, v in zip(cols, row) if v is not None}
            payload = {"query": mutation, "variables": {"input": record}}
            try:
                resp = req.post(url, json=payload, headers=headers, timeout=30)
                if resp.status_code in (200, 201):
                    success += 1
                    self._q("load", f"  Row {i}: OK ({resp.status_code})\n")
                else:
                    failed += 1
                    self._q("load", f"  Row {i}: FAILED ({resp.status_code}) — {resp.text[:120]}\n")
            except Exception as exc:
                failed += 1
                self._q("load", f"  Row {i}: ERROR — {exc}\n")

        self._q("load", f"\n  Done — {success} succeeded, {failed} failed.\n")
        self._q("load_done", (success, failed))

    # ── CSV Upload ─────────────────────────────────────────────────────────────
    def upload_csv(self):
        """Upload from the Mappings tab."""
        paths = filedialog.askopenfilenames(
            title="Select CSV file(s)",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        for src in paths:
            dest = DATA_DIR / Path(src).name
            shutil.copy2(src, dest)
            self._q("map", f"Uploaded: {Path(src).name}\n")
        self.refresh_csv_list()

    def _upload_csv_pipeline(self):
        """Upload from the Pipeline tab."""
        paths = filedialog.askopenfilenames(
            title="Select CSV file(s)",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        for src in paths:
            dest = DATA_DIR / Path(src).name
            shutil.copy2(src, dest)
            self._q("pipe", f"Uploaded: {Path(src).name}\n")
        self.refresh_csv_list()

    def remove_csv(self):
        sel = self._csv_listbox.curselection()
        if not sel:
            return
        name = self._csv_listbox.get(sel[0])
        if messagebox.askyesno("Remove CSV", f"Delete '{name}' from the data folder?"):
            (DATA_DIR / name).unlink(missing_ok=True)
            self.refresh_csv_list()

    def refresh_csv_list(self):
        files = sorted(p.name for p in DATA_DIR.glob("*.csv"))
        self._csv_listbox.delete(0, tk.END)
        for f in files:
            self._csv_listbox.insert(tk.END, f)
        self._pipe_csv_cb["values"] = files
        if self._pipe_csv_var.get() not in files:
            self._pipe_csv_cb.set("")
        self._refresh_edit_csv_list()

    # ── Generate Mappings ──────────────────────────────────────────────────────
    def run_generate_mappings(self):
        if not list(DATA_DIR.glob("*.csv")):
            messagebox.showwarning("No CSVs", "Upload at least one CSV file first.")
            return
        self._q("map", "=" * 52 + "\n")
        self._q("map", "  Running generate_mappings.py...\n")
        self._q("map", "=" * 52 + "\n\n")
        threading.Thread(target=self._worker_gen_maps, daemon=True).start()

    def _worker_gen_maps(self):
        script = BASE_DIR / "generate_mappings.py"
        proc   = subprocess.Popen(
            [sys.executable, str(script)],
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=os.environ.copy()
        )
        for line in proc.stdout:
            self._q("map", line)
        proc.wait()
        if proc.returncode == 0:
            self._q("map", "\n  Done — mapping templates written.\n")
        else:
            self._q("map", f"\n  Process exited with code {proc.returncode}.\n")
        self._q("refresh_maps", None)

    # ── Pipeline: CSV & mapping selection ─────────────────────────────────────
    def _on_csv_selected(self, _=None):
        name = self._pipe_csv_var.get()
        if not name:
            return
        self._csv_path = DATA_DIR / name

        template_dir = TEMPLATES_DIR / self._csv_path.stem
        maps = sorted(p.name for p in template_dir.glob("*.yml")) if template_dir.exists() else []
        self._pipe_map_cb["values"] = maps
        self._pipe_map_cb.set("")
        self._clear_filter_widgets()
        self._mapping_data = None
        self._preview_label.configure(text="")

        self._q("pipe", f"CSV: {name}\n")
        if maps:
            self._q("pipe", f"  {len(maps)} mapping template(s) found.\n")
        else:
            self._q("pipe", "  No mapping templates found — run Generate Mappings first.\n")

    def _on_mapping_selected(self, _=None):
        map_name = self._pipe_map_var.get()
        if not map_name or self._csv_path is None:
            return
        map_path = TEMPLATES_DIR / self._csv_path.stem / map_name
        with open(map_path, encoding="utf-8") as f:
            self._mapping_data = yaml.safe_load(f)

        self._q("pipe", f"Mapping: {map_name}\n")
        self._q("pipe", f"  Entity: {self._mapping_data.get('kraken_entity', '?')}\n")
        self._q("pipe", "  Loading filterable columns…\n")
        self._filter_hint.configure(text="Loading…")
        threading.Thread(target=self._worker_load_filters, daemon=True).start()

    def _worker_load_filters(self):
        try:
            cols       = detect_filterable_columns(self._csv_path)
            col_values = {col: get_valid_values(self._csv_path, col) for col in cols}
            self._q("build_filters", col_values)
        except Exception as exc:
            self._q("pipe", f"  Filter load error: {exc}\n")

    # ── Pipeline: filters ──────────────────────────────────────────────────────
    def _clear_filter_widgets(self):
        for w in self._filter_frame.winfo_children():
            w.destroy()
        self._filter_vars.clear()
        self._filter_cols.clear()

    def _build_filter_widgets(self, col_values: dict):
        self._clear_filter_widgets()
        self._filter_cols = list(col_values.keys())

        if not self._filter_cols:
            self._filter_hint.configure(text="No filterable columns found.")
            self._q("pipe", "  No filterable columns found.\n")
            return

        self._filter_hint.configure(text=f"{len(self._filter_cols)} filter(s) available:")

        for col, rows in col_values.items():
            ttk.Label(self._filter_frame, text=col).pack(anchor="w", pady=(6, 0))
            var     = tk.StringVar(value="(skip)")
            display = {f"{v}  ({c} records)": str(v) for v, c in rows}
            options = ["(skip)"] + list(display.keys())
            ttk.Combobox(self._filter_frame, textvariable=var,
                         values=options, state="readonly", width=30).pack(fill=tk.X)
            self._filter_vars[col] = (var, display)

        self._q("pipe", f"  {len(self._filter_cols)} filterable column(s) loaded.\n")

    # ── Pipeline: preview ──────────────────────────────────────────────────────
    def preview_cohort(self):
        if not self._validate_inputs():
            return
        self._preview_label.configure(text="Calculating…", foreground=MUT)
        threading.Thread(target=self._worker_preview, daemon=True).start()

    def _worker_preview(self):
        try:
            filters = self._collect_filters()
            csv_str = str(self._csv_path).replace("\\", "/")
            where   = " AND ".join([f'"{k}" = \'{v}\'' for k, v in filters.items()]) or "1=1"
            con     = duckdb.connect()
            count   = con.execute(
                f"SELECT COUNT(*) FROM read_csv_auto('{csv_str}') WHERE {where}"
            ).fetchone()[0]
            con.close()
            self._q("preview", f"Estimated rows: {count:,}")
        except Exception as exc:
            self._q("preview_err", f"Preview error: {exc}")

    # ── Pipeline: run ──────────────────────────────────────────────────────────
    def run_pipeline(self):
        if not self._validate_inputs():
            return
        cohort_name = self._cohort_name_var.get().strip()
        if not cohort_name:
            messagebox.showwarning("Cohort Name", "Please enter a cohort name.")
            return
        threading.Thread(target=self._worker_pipeline, daemon=True).start()

    def _worker_pipeline(self, cohort_override: dict | None = None):
        """
        Run the dbt pipeline.

        cohort_override — when called from the Cohort Plan tab, a dict with:
            cohort_id    : int
            version      : int
            name         : str
            csv_path     : Path
            mapping_data : dict
            mapping_file : str
            filters      : list[dict]   ← new list format
        When None, reads UI state from the pipeline tab (legacy dict format).
        """
        run_start = datetime.now()
        from_plan = cohort_override is not None
        log_target = "cp" if from_plan else "pipe"

        try:
            if from_plan:
                cohort_id    = cohort_override["cohort_id"]
                cohort_ver   = cohort_override.get("version", 1)
                cohort_name  = cohort_override["name"]
                csv_path     = cohort_override["csv_path"]
                mapping_data = cohort_override["mapping_data"]
                map_name     = cohort_override["mapping_file"]
                filters      = cohort_override["filters"]   # list format
                # Flatten to legacy dict for cohort.yml (backward compat)
                legacy_filters = CohortPlan.filters_to_legacy_dict(filters)
            else:
                cohort_id    = None
                cohort_ver   = 1
                filters      = self._collect_filters()   # returns legacy dict
                cohort_name  = self._cohort_name_var.get().strip()
                csv_path     = self._csv_path
                mapping_data = self._mapping_data
                map_name     = self._pipe_map_var.get()
                legacy_filters = filters  # already dict

            self._q(log_target, "\n" + "=" * 52 + "\n")
            self._q(log_target, f"  Cohort:  {cohort_name}\n")
            self._q(log_target, f"  Entity:  {mapping_data.get('kraken_entity')}\n")
            filter_display = (
                "\n".join(
                    f"    {f['field']} {f.get('operator','=')} "
                    f"{f.get('value') or f.get('values','')}"
                    for f in filters
                )
                if isinstance(filters, list)
                else json.dumps(filters)
            ) or "(none)"
            self._q(log_target, f"  Filters:\n{filter_display}\n")
            self._q(log_target, "=" * 52 + "\n\n")

            # cohort.yml (legacy, keeps pipeline.py compatible)
            with open(COHORT_CFG, "w") as f:
                yaml.dump({"cohort_name": cohort_name, "filters": legacy_filters},
                          f, default_flow_style=False)
            self._q(log_target, "cohort.yml updated.\n")

            # dbt_project.yml
            with open(DBT_PROJECT) as f:
                dbt_proj = yaml.safe_load(f)
            dbt_proj["vars"]["csv_path"] = f"data/{csv_path.name}"
            with open(DBT_PROJECT, "w") as f:
                yaml.dump(dbt_proj, f, default_flow_style=False, sort_keys=False)
            self._q(log_target, "dbt_project.yml updated.\n")

            # valid_values.md
            self._write_valid_values(csv_path)
            self._q(log_target, "valid_values.md updated.\n\n")

            # dbt run
            self._q(log_target, "Running dbt…\n")
            dbt_exe = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
            if not dbt_exe.exists():
                dbt_exe = "dbt"

            dbt_vars = json.dumps({
                "filters":              filters,   # pass as-is (list or dict)
                "required_fields":      mapping_data.get("required_fields", []),
                "column_map":           mapping_data.get("column_map", {}),
                "transformations":      mapping_data.get("transformations", {}),
                "active_record_filter": mapping_data.get("active_record_filter", {}),
            })

            env = os.environ.copy()
            env["DUCKDB_PATH"] = str(DB_PATH)

            proc = subprocess.Popen(
                [str(dbt_exe), "run", "--profiles-dir", ".", "--vars", dbt_vars],
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, env=env
            )
            for line in proc.stdout:
                self._q(log_target, line)
            proc.wait()

            duration = (datetime.now() - run_start).total_seconds()

            if proc.returncode != 0:
                self._q(log_target, "\n  dbt run failed — see output above.\n")
                if from_plan:
                    self._cohort_plan.update_status(cohort_id, "failed")
                    self._q("cp_refresh", None)
                return

            # Log to legacy cohort_history
            self._log_cohort_history(
                cohort_name, mapping_data.get("kraken_entity", ""),
                csv_path.name, map_name, legacy_filters
            )

            # Log to new cohort_run_log (plan-driven runs only)
            if from_plan:
                try:
                    con_r = duckdb.connect(str(DB_PATH))
                    rows_loaded   = con_r.execute(
                        "SELECT COUNT(*) FROM output.output_data").fetchone()[0]
                    rows_rejected = con_r.execute(
                        "SELECT COUNT(*) FROM validated.validated_data_rejected").fetchone()[0]
                    con_r.close()
                except Exception:
                    rows_loaded = rows_rejected = 0

                rows_read = rows_loaded + rows_rejected
                run_id = self._cohort_plan.log_run(
                    cohort_id=cohort_id,
                    cohort_version=cohort_ver,
                    cohort_name=cohort_name,
                    kraken_entity=mapping_data.get("kraken_entity", ""),
                    csv_file=csv_path.name,
                    mapping_file=map_name,
                    filters=filters if isinstance(filters, list) else
                            CohortPlan.legacy_dict_to_filters(filters),
                    rows_read=rows_read,
                    rows_loaded=rows_loaded,
                    rows_rejected=rows_rejected,
                    duration_secs=duration,
                    run_status="complete" if rows_rejected == 0 else "partial",
                )
                new_status = "complete" if rows_rejected == 0 else "partial"
                self._cohort_plan.update_status(cohort_id, new_status,
                                                estimated_rows=rows_read)
                self._q(log_target,
                        f"\n  Cohort complete — {rows_loaded:,} loaded, "
                        f"{rows_rejected:,} rejected  (run #{run_id})\n"
                        f"  Status set to: {new_status.upper()}\n")
                self._q("cp_refresh", None)

        except Exception as exc:
            self._q(log_target, f"\n  ERROR: {exc}\n")
            if from_plan and self._selected_cohort_id is not None:
                self._cohort_plan.update_status(self._selected_cohort_id, "failed")
                self._q("cp_refresh", None)

    def _write_valid_values(self, csv_path: Path):
        filterable = detect_filterable_columns(csv_path)
        lines = [
            "# Valid filter values",
            f"_Generated from `data/{csv_path.name}` on {datetime.now().strftime('%Y-%m-%d %H:%M')}_\n",
        ]
        for col in filterable:
            rows = get_valid_values(csv_path, col)
            lines.append(f"## {col}")
            for value, count in rows:
                lines.append(f"- `{value}` ({count} records)")
            lines.append("")
        (BASE_DIR / "valid_values.md").write_text("\n".join(lines))

    def _log_cohort_history(self, cohort_name, entity, csv_name, map_name, filters):
        con = duckdb.connect(str(DB_PATH))

        schema_sql = """
            CREATE TABLE cohort_history (
                run_id        INTEGER PRIMARY KEY,
                cohort_name   VARCHAR,
                kraken_entity VARCHAR,
                csv_file      VARCHAR,
                mapping_file  VARCHAR,
                filters       VARCHAR,
                rows_read     INTEGER,
                rows_loaded   INTEGER,
                rows_rejected INTEGER,
                run_at        TIMESTAMP
            )
        """
        table_exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'cohort_history'
        """).fetchone()[0]

        if table_exists:
            col_count = con.execute("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'cohort_history'
            """).fetchone()[0]
            if col_count != 10:
                con.execute("DROP TABLE cohort_history")
                con.execute(schema_sql)
        else:
            con.execute(schema_sql)

        try:
            rows_loaded   = con.execute("SELECT COUNT(*) FROM gold.gold_output").fetchone()[0]
            rows_rejected = con.execute("SELECT COUNT(*) FROM bronze.bronze_data_rejected").fetchone()[0]
        except Exception:
            rows_loaded, rows_rejected = 0, 0

        rows_read = rows_loaded + rows_rejected
        next_id   = con.execute(
            "SELECT COALESCE(MAX(run_id), 0) + 1 FROM cohort_history"
        ).fetchone()[0]

        con.execute(
            "INSERT INTO cohort_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [next_id, cohort_name, entity, csv_name, map_name,
             json.dumps(filters), rows_read, rows_loaded, rows_rejected, datetime.now()]
        )
        con.close()

        self._q("pipe",
                f"\n  Pipeline complete — {rows_loaded} rows loaded, "
                f"{rows_rejected} rejected, {rows_read} total\n"
                f"  Run #{next_id} logged to cohort_history.\n")

    # ── Helpers ────────────────────────────────────────────────────────────────
    def _collect_filters(self) -> dict:
        filters = {}
        for col, (var, display_map) in self._filter_vars.items():
            chosen = var.get()
            if chosen and chosen != "(skip)" and chosen in display_map:
                filters[col] = display_map[chosen]
        return filters

    def _validate_inputs(self) -> bool:
        if self._csv_path is None or not self._csv_path.exists():
            messagebox.showwarning("Missing Input", "Please select a CSV file.")
            return False
        if self._mapping_data is None:
            messagebox.showwarning("Missing Input", "Please select an entity mapping.")
            return False
        return True

    def _q(self, kind: str, content):
        self._output_queue.put((kind, content))

    @staticmethod
    def _clear_log(widget: ScrolledText):
        widget.configure(state=tk.NORMAL)
        widget.delete("1.0", tk.END)
        widget.configure(state=tk.DISABLED)

    def _poll_queue(self):
        try:
            while True:
                kind, content = self._output_queue.get_nowait()
                if kind == "map":
                    self._write_log(self._map_log, content)
                elif kind == "pipe":
                    self._write_log(self._pipe_log, content)
                elif kind == "load":
                    self._write_log(self._load_log, content)
                elif kind == "load_done":
                    success, failed = content
                    if failed == 0:
                        messagebox.showinfo("Load Complete",
                            f"All {success} rows loaded successfully.")
                    else:
                        messagebox.showwarning("Load Complete",
                            f"{success} rows succeeded, {failed} failed.\n"
                            "Check the output log for details.")
                elif kind == "preview":
                    self._preview_label.configure(text=content, foreground=GRN)
                elif kind == "preview_err":
                    self._preview_label.configure(text=content, foreground=RED)
                elif kind == "build_filters":
                    self._build_filter_widgets(content)
                elif kind == "refresh_maps":
                    self._on_csv_selected()
                    if self._edit_csv_var.get():
                        self._on_edit_csv_selected()
                elif kind == "cp":
                    self._write_log(self._cp_log, content)
                elif kind == "cp_est":
                    self._cp_est_lbl.configure(text=f"{content:,}")
                elif kind == "cp_est_err":
                    self._cp_est_lbl.configure(text=f"Error: {content}", foreground=RED)
                elif kind == "cp_refresh":
                    self._refresh_cohort_list()
                    cid = self._selected_cohort_id
                    if cid is not None:
                        c = self._cohort_plan.get_cohort(cid)
                        if c:
                            self._populate_cohort_detail(c)
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)

    @staticmethod
    def _write_log(widget: ScrolledText, text: str):
        widget.configure(state=tk.NORMAL)
        widget.insert(tk.END, text)
        widget.see(tk.END)
        widget.configure(state=tk.DISABLED)


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    KrakenApp().mainloop()
