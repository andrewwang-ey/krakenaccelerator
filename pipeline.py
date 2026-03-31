import subprocess
import json
import os
from pathlib import Path
from datetime import datetime

import yaml
import duckdb

# ── Paths ─────────────────────────────────────────────────────────────────────
repo_root  = Path(__file__).parent
db_dir     = repo_root / 'db'
db_path    = db_dir / 'kraken.duckdb'
cohort_cfg = repo_root / 'cohort.yml'
csv_path   = repo_root / 'data' / 'oracle_address_sample_100_au_streets.csv'

db_dir.mkdir(exist_ok=True)


# ── Helper: load CSV into a temp DuckDB to query valid values ─────────────────
def get_valid_values(field):
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT {field}, COUNT(*) AS records
        FROM read_csv_auto('{csv_path}')
        GROUP BY {field}
        ORDER BY {field}
    """).fetchall()
    con.close()
    return rows  # list of (value, count) tuples


# ── Helper: prompt user to pick from a list ───────────────────────────────────
def prompt_choice(field, options):
    print(f"\nWhich {field} would you like to load?")
    for i, (value, count) in enumerate(options, 1):
        print(f"  {i}. {value}  ({count} records)")
    while True:
        raw = input("> ")
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1][0]
        print(f"  Please enter a number between 1 and {len(options)}")


# ── Step 1: Interactive prompt ────────────────────────────────────────────────
print("=" * 50)
print("  Kraken Migration — Cohort Builder")
print("=" * 50)

state        = prompt_choice("STATE",        get_valid_values("STATE"))
address_type = prompt_choice("ADDRESS_TYPE", get_valid_values("ADDRESS_TYPE"))
is_primary   = prompt_choice("IS_PRIMARY",   get_valid_values("IS_PRIMARY"))

print("\nCohort name (e.g. victoria_billing):")
cohort_name = input("> ").strip() or "unnamed_cohort"

filters = {
    "state":        state,
    "address_type": address_type,
    "is_primary":   is_primary,
}

# Preview row count before running
con = duckdb.connect()
preview_count = con.execute(f"""
    SELECT COUNT(*) FROM read_csv_auto('{csv_path}')
    WHERE STATE        = '{state}'
      AND ADDRESS_TYPE = '{address_type}'
      AND IS_PRIMARY   = '{is_primary}'
      AND END_DATE IS NULL
""").fetchone()[0]
con.close()

print(f"\nCohort: {cohort_name}")
print(f"Filters: {json.dumps(filters, indent=2)}")
print(f"Estimated rows: {preview_count}")
print("\nProceed? (y/n)")
if input("> ").strip().lower() != 'y':
    raise SystemExit("Cancelled.")

# ── Step 2: Save selection back to cohort.yml ─────────────────────────────────
with open(cohort_cfg, 'w') as f:
    yaml.dump({'cohort_name': cohort_name, 'filters': filters}, f, default_flow_style=False)

print(f"\ncohort.yml updated.")


# ── Step 3: Generate valid_values.md ─────────────────────────────────────────
filterable_fields = ["STATE", "ADDRESS_TYPE", "IS_PRIMARY", "COUNTRY_CODE"]
lines = [
    "# Valid filter values",
    f"_Generated from `data/oracle_address_sample_100_au_streets.csv` on {datetime.now().strftime('%Y-%m-%d %H:%M')}_\n",
]
for field in filterable_fields:
    rows = get_valid_values(field)
    lines.append(f"## {field}")
    for value, count in rows:
        lines.append(f"- `{value}` ({count} records)")
    lines.append("")

valid_values_path = repo_root / 'valid_values.md'
valid_values_path.write_text("\n".join(lines))
print(f"valid_values.md updated.")


# ── Step 4: Run dbt ───────────────────────────────────────────────────────────
env = os.environ.copy()
env['DUCKDB_PATH'] = str(db_path)

result = subprocess.run(
    ['dbt', 'run', '--profiles-dir', '.', '--vars', json.dumps(filters)],
    cwd=repo_root,
    capture_output=False,
    env=env
)

if result.returncode != 0:
    raise SystemExit("dbt run failed — check the output above")


# ── Step 5: Log to cohort_history ─────────────────────────────────────────────
con = duckdb.connect(str(db_path))

con.execute("""
    CREATE TABLE IF NOT EXISTS cohort_history (
        run_id       INTEGER PRIMARY KEY,
        cohort_name  VARCHAR,
        filters      VARCHAR,
        rows_loaded  INTEGER,
        run_at       TIMESTAMP
    )
""")

rows_loaded = con.execute(
    "SELECT COUNT(*) FROM gold.gold_billing_address"
).fetchone()[0]

next_id = con.execute(
    "SELECT COALESCE(MAX(run_id), 0) + 1 FROM cohort_history"
).fetchone()[0]

con.execute(
    "INSERT INTO cohort_history VALUES (?, ?, ?, ?, ?)",
    [next_id, cohort_name, json.dumps(filters), rows_loaded, datetime.now()]
)

con.close()

print(f"\nPipeline complete — {rows_loaded} rows loaded into gold layer")
print(f"Run #{next_id} logged to cohort_history in db/kraken.duckdb")