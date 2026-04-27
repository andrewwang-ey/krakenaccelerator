"""
cohort_manager.py — Cohort Plan management for the Kraken Migration Accelerator

Designed around a long-running migration scenario (e.g. 1 M rows over a year):
  - A cohorts.yml plan file defines ALL cohorts upfront (like the stakeholder   spreadsheet), each with version tracking and rich filter expressions.
  - Every pipeline run is logged to DuckDB for full reconciliation.
  - PowerBI-ready CSV exports give visibility into progress, failures, and causes.

Filter operators supported per cohort:  =, !=, IN, NOT IN, IS NULL, IS NOT NULL, >, <, >=, <=
"""

from __future__ import annotations

import csv as _csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import yaml

BASE_DIR     = Path(__file__).parent
COHORTS_FILE = BASE_DIR / "cohorts.yml"
DB_PATH      = BASE_DIR / "db" / "kraken.duckdb"
POWERBI_DIR  = BASE_DIR / "powerbi"

# Valid status values for a cohort
STATUSES = ("pending", "running", "complete", "failed", "partial", "skipped")

# Colour hints for UI layers (return values, not Tk colours)
STATUS_COLOUR = {
    "pending":  "muted",
    "running":  "accent",
    "complete": "green",
    "failed":   "red",
    "partial":  "orange",
    "skipped":  "muted",
}


class CohortPlan:

    def __init__(
        self,
        plan_path: Path | None = None,
        db_path:   Path | None = None,
    ) -> None:
        self.plan_path = plan_path or COHORTS_FILE
        self.db_path   = db_path   or DB_PATH

    # ── Plan file I/O ─────────────────────────────────────────────────────────

    def load(self) -> dict:
        """Load cohorts.yml. Returns an empty plan structure if the file is missing."""
        if not self.plan_path.exists():
            return self._empty_plan()
        with open(self.plan_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if data else self._empty_plan()

    def save(self, plan: dict) -> None:
        """Persist the plan dict back to cohorts.yml."""
        plan["updated_at"] = datetime.now().isoformat()
        with open(self.plan_path, "w", encoding="utf-8") as f:
            yaml.dump(plan, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    def new_plan(self, dataset: str, mapping: str) -> dict:
        """Create and save a blank plan for a given dataset / mapping."""
        plan = self._empty_plan()
        plan["dataset"] = dataset
        plan["mapping"] = mapping
        self.save(plan)
        return plan

    @staticmethod
    def _empty_plan() -> dict:
        return {
            "plan_version": 1,
            "dataset":      None,
            "mapping":      None,
            "created_at":   datetime.now().isoformat(),
            "updated_at":   datetime.now().isoformat(),
            "cohorts":      [],
        }

    # ── Cohort accessors ──────────────────────────────────────────────────────

    def get_cohort(self, cohort_id: int) -> dict | None:
        for c in self.load()["cohorts"]:
            if c["cohort_id"] == cohort_id:
                return c
        return None

    def get_pending(self) -> list[dict]:
        return [c for c in self.load()["cohorts"] if c.get("status", "pending") == "pending"]

    def next_available_id(self) -> int:
        cohorts = self.load()["cohorts"]
        return max((c["cohort_id"] for c in cohorts), default=0) + 1

    def add_cohort(self, cohort: dict) -> None:
        plan = self.load()
        plan["cohorts"].append(cohort)
        self.save(plan)

    def update_status(self, cohort_id: int, status: str, **extra: Any) -> None:
        """Update a cohort's status and any additional fields (e.g. estimated_rows)."""
        if status not in STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of: {STATUSES}")
        plan = self.load()
        for c in plan["cohorts"]:
            if c["cohort_id"] == cohort_id:
                c["status"] = status
                c.update(extra)
                break
        self.save(plan)

    def bump_version(self, cohort_id: int) -> int:
        """Increment a cohort's version number (call when filters are edited)."""
        plan = self.load()
        for c in plan["cohorts"]:
            if c["cohort_id"] == cohort_id:
                c["version"] = c.get("version", 1) + 1
                new_ver = c["version"]
                break
        else:
            raise ValueError(f"Cohort {cohort_id} not found in plan.")
        self.save(plan)
        return new_ver

    # ── SQL filter builder ────────────────────────────────────────────────────

    @staticmethod
    def build_where(filters: list[dict]) -> str:
        """Convert a filter-spec list into a SQL WHERE fragment (no leading WHERE).

        Each filter dict:
            field    : str        — column name
            operator : str        — =  !=  IN  NOT IN  IS NULL  IS NOT NULL  >  <  >=  <=
            value    : str/number — single value (for =, !=, comparison operators)
            values   : list       — multiple values (for IN, NOT IN)
        """
        if not filters:
            return "1=1"
        parts: list[str] = []
        for f in filters:
            field = f["field"]
            op    = f.get("operator", "=").strip().upper()
            if op in ("IS NULL", "IS NOT NULL"):
                parts.append(f'"{field}" {op}')
            elif op in ("IN", "NOT IN"):
                vals = ", ".join(f"'{v}'" for v in f.get("values", []))
                parts.append(f'"{field}" {op} ({vals})')
            else:
                val = f.get("value", "")
                parts.append(f'"{field}" {op} \'{val}\'')
        return " AND ".join(parts)

    @staticmethod
    def filters_to_legacy_dict(filters: list[dict]) -> dict:
        """Convert new filter list → old {'FIELD': 'value'} dict (equality only)."""
        result = {}
        for f in filters:
            if f.get("operator", "=") == "=":
                result[f["field"]] = f.get("value", "")
        return result

    @staticmethod
    def legacy_dict_to_filters(d: dict) -> list[dict]:
        """Convert old {'FIELD': 'value'} dict → new filter list."""
        return [{"field": k, "operator": "=", "value": str(v)} for k, v in d.items()]

    # ── Row count estimate ────────────────────────────────────────────────────

    def estimate_rows(self, csv_path: Path, filters: list[dict]) -> int:
        csv_str = str(csv_path).replace("\\", "/")
        where   = self.build_where(filters)
        con     = duckdb.connect()
        count   = con.execute(
            f"SELECT COUNT(*) FROM read_csv_auto('{csv_str}') WHERE {where}"
        ).fetchone()[0]
        con.close()
        return count

    # ── Reconciliation DB tables ──────────────────────────────────────────────

    def ensure_tables(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create cohort_run_log and rejection_detail tables if they don't exist."""
        con.execute("""
            CREATE TABLE IF NOT EXISTS cohort_run_log (
                run_id          INTEGER PRIMARY KEY,
                cohort_id       INTEGER,
                cohort_version  INTEGER,
                cohort_name     VARCHAR,
                kraken_entity   VARCHAR,
                csv_file        VARCHAR,
                mapping_file    VARCHAR,
                filters_json    VARCHAR,
                rows_read       INTEGER,
                rows_loaded     INTEGER,
                rows_rejected   INTEGER,
                rows_api_failed INTEGER,
                duration_secs   DOUBLE,
                run_status      VARCHAR,
                run_at          TIMESTAMP,
                completed_at    TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS rejection_detail (
                run_id           INTEGER,
                cohort_id        INTEGER,
                cohort_name      VARCHAR,
                rejection_reason VARCHAR,
                row_count        INTEGER,
                pct_of_cohort    DOUBLE
            )
        """)

    def log_run(
        self,
        cohort_id:      int,
        cohort_version: int,
        cohort_name:    str,
        kraken_entity:  str,
        csv_file:       str,
        mapping_file:   str,
        filters:        list[dict],
        rows_read:      int,
        rows_loaded:    int,
        rows_rejected:  int,
        duration_secs:  float,
        run_status:     str,
    ) -> int:
        """Log a cohort pipeline run. Returns the new run_id.

        Also populates rejection_detail by reading
        validated.validated_data_rejected from DuckDB.
        """
        con = duckdb.connect(str(self.db_path))
        self.ensure_tables(con)

        next_id = con.execute(
            "SELECT COALESCE(MAX(run_id), 0) + 1 FROM cohort_run_log"
        ).fetchone()[0]

        now = datetime.now()
        con.execute(
            """
            INSERT INTO cohort_run_log
            VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?)
            """,
            [
                next_id, cohort_id, cohort_version, cohort_name,
                kraken_entity, csv_file, mapping_file,
                json.dumps(filters, ensure_ascii=False),
                rows_read, rows_loaded, rows_rejected,
                round(duration_secs, 2), run_status, now, now,
            ],
        )

        # Capture rejection breakdown from the validated schema
        try:
            rej_rows = con.execute("""
                SELECT rejection_reason, COUNT(*) AS cnt
                FROM validated.validated_data_rejected
                GROUP BY rejection_reason
                ORDER BY cnt DESC
            """).fetchall()
            for reason, cnt in rej_rows:
                pct = round(cnt / rows_read * 100, 2) if rows_read > 0 else 0.0
                con.execute(
                    "INSERT INTO rejection_detail VALUES (?,?,?,?,?,?)",
                    [next_id, cohort_id, cohort_name, reason, cnt, pct],
                )
        except Exception:
            pass  # table may not be populated on this run

        con.close()
        return next_id

    def get_rejection_detail(self, cohort_id: int | None = None) -> list[dict]:
        """Return rejection rows for a cohort (or all cohorts if None)."""
        try:
            con = duckdb.connect(str(self.db_path))
            self.ensure_tables(con)
            if cohort_id is not None:
                rows = con.execute(
                    """
                    SELECT rd.rejection_reason, SUM(rd.row_count) AS total_count,
                           ROUND(SUM(rd.row_count) * 100.0 /
                                 NULLIF(SUM(SUM(rd.row_count)) OVER (), 0), 2) AS pct
                    FROM rejection_detail rd
                    WHERE rd.cohort_id = ?
                    GROUP BY rd.rejection_reason
                    ORDER BY total_count DESC
                    """,
                    [cohort_id],
                ).fetchall()
            else:
                rows = con.execute(
                    """
                    SELECT rd.cohort_name, rd.rejection_reason,
                           SUM(rd.row_count) AS total_count
                    FROM rejection_detail rd
                    GROUP BY rd.cohort_name, rd.rejection_reason
                    ORDER BY total_count DESC
                    """
                ).fetchall()
            con.close()
            return rows
        except Exception:
            return []

    def get_run_summary(self, cohort_id: int | None = None) -> list[dict]:
        """Latest run stats for a cohort (or all cohorts)."""
        try:
            con = duckdb.connect(str(self.db_path))
            self.ensure_tables(con)
            if cohort_id is not None:
                rows = con.execute(
                    """
                    SELECT run_id, cohort_name, rows_read, rows_loaded,
                           rows_rejected, duration_secs, run_status, run_at
                    FROM cohort_run_log
                    WHERE cohort_id = ?
                    ORDER BY run_at DESC
                    """,
                    [cohort_id],
                ).fetchall()
            else:
                rows = con.execute(
                    """
                    SELECT run_id, cohort_name, rows_read, rows_loaded,
                           rows_rejected, duration_secs, run_status, run_at
                    FROM cohort_run_log
                    ORDER BY run_at DESC
                    """
                ).fetchall()
            con.close()
            return rows
        except Exception:
            return []

    # ── PowerBI CSV export ────────────────────────────────────────────────────

    def export_powerbi(self, powerbi_dir: Path | None = None) -> None:
        """Write all reconciliation data to the powerbi/ folder.

        Outputs:
          cohort_plan.csv        — all cohorts and their current status
          cohort_run_log.csv     — every pipeline run with row counts
          rejection_breakdown.csv— rejection reasons per cohort / run
          cohort_history.csv     — legacy run history (backward compat)
        """
        powerbi_dir = powerbi_dir or POWERBI_DIR
        powerbi_dir.mkdir(exist_ok=True)

        # 1. Cohort plan status
        plan = self.load()
        plan_rows = [
            {
                "cohort_id":      c["cohort_id"],
                "name":           c["name"],
                "version":        c.get("version", 1),
                "description":    c.get("description", ""),
                "priority":       c.get("priority", ""),
                "status":         c.get("status", "pending"),
                "filters":        json.dumps(c.get("filters", []), ensure_ascii=False),
                "estimated_rows": c.get("estimated_rows") or "",
                "target_rows":    c.get("target_rows") or "",
                "notes":          c.get("notes", ""),
            }
            for c in plan.get("cohorts", [])
        ]
        self._write_csv(powerbi_dir / "cohort_plan.csv", plan_rows)

        # 2. Run log and rejection breakdown from DuckDB
        try:
            con = duckdb.connect(str(self.db_path))
            self.ensure_tables(con)

            try:
                runs_df = con.execute(
                    "SELECT * FROM cohort_run_log ORDER BY run_at"
                ).df()
                runs_df.to_csv(powerbi_dir / "cohort_run_log.csv", index=False)
            except Exception:
                pass

            try:
                rej_df = con.execute(
                    "SELECT * FROM rejection_detail ORDER BY run_id, row_count DESC"
                ).df()
                rej_df.to_csv(powerbi_dir / "rejection_breakdown.csv", index=False)
            except Exception:
                pass

            # Backward-compat: also refresh cohort_history.csv
            try:
                hist_df = con.execute(
                    "SELECT * FROM cohort_history ORDER BY run_at"
                ).df()
                hist_df.to_csv(powerbi_dir / "cohort_history.csv", index=False)
            except Exception:
                pass

            con.close()
        except Exception:
            pass

    @staticmethod
    def _write_csv(path: Path, rows: list[dict]) -> None:
        if not rows:
            path.write_text("", encoding="utf-8")
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    # ── Import from stakeholder spreadsheet (CSV) ─────────────────────────────

    @classmethod
    def from_csv_import(
        cls,
        csv_path: str | Path,
        dataset:  str,
        mapping:  str,
    ) -> dict:
        """Import a cohort plan from a spreadsheet CSV exported by the client.

        Expected columns (case-insensitive, order does not matter):
            cohort_id   — integer (auto-incremented if absent)
            name        — cohort display name
            description — optional description
            priority    — processing order (1 = first)
            target_rows — expected row count (business-agreed)
            notes       — free-text notes

        Any other column is treated as a DATA FILTER field:
            Single value  → equality filter  (STATE = VIC)
            Comma-separated values → IN filter  (STATE IN (VIC, NSW))
            Empty / '*' / 'all' → no filter on that column
        """
        STANDARD = {"cohort_id", "name", "description", "priority", "target_rows", "notes"}
        cohorts: list[dict] = []

        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader      = _csv.DictReader(f)
            raw_headers = reader.fieldnames or []
            fieldnames  = [h.strip() for h in raw_headers]
            filter_cols = [
                c for c in fieldnames
                if c.lower().replace(" ", "_") not in STANDARD
            ]

            SKIP_PREFIXES = ("INSTRUCTION", "EXAMPLE", "#")

            for i, raw_row in enumerate(reader, start=1):
                row = {k.strip(): (v or "").strip() for k, v in raw_row.items() if k is not None}

                # Skip instruction / example / comment / blank rows.
                # Check both the cohort_id and name columns so either can carry the marker.
                cid_raw_check  = row.get("cohort_id", "").upper()
                name_raw_check = row.get("name", "").upper()
                if (
                    not row.get("name", "")
                    or cid_raw_check.startswith(SKIP_PREFIXES)
                    or name_raw_check.startswith(SKIP_PREFIXES)
                ):
                    continue

                filters: list[dict] = []
                for col in filter_cols:
                    val = row.get(col, "")
                    if not val or val in ("*", "-") or val.lower() == "all":
                        continue
                    if "," in val:
                        values = [v.strip() for v in val.split(",") if v.strip()]
                        filters.append({"field": col, "operator": "IN", "values": values})
                    else:
                        filters.append({"field": col, "operator": "=", "value": val})

                cid_raw = row.get("cohort_id", "")
                cohorts.append({
                    "cohort_id":    int(cid_raw) if cid_raw.lstrip("-").isdigit() else i,
                    "name":         row.get("name", f"Cohort {i}"),
                    "version":      1,
                    "description":  row.get("description", ""),
                    "priority":     (
                        int(row["priority"])
                        if row.get("priority", "").lstrip("-").isdigit()
                        else i
                    ),
                    "status":       "pending",
                    "filters":      filters,
                    "estimated_rows": None,
                    "target_rows":  row.get("target_rows") or None,
                    "notes":        row.get("notes", ""),
                })

        return {
            "plan_version": 1,
            "dataset":      dataset,
            "mapping":      mapping,
            "created_at":   datetime.now().isoformat(),
            "updated_at":   datetime.now().isoformat(),
            "cohorts":      cohorts,
        }
