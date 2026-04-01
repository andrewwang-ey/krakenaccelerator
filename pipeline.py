import subprocess
import json
import os
import sys
from pathlib import Path
from datetime import datetime

import yaml
import duckdb

# ── Paths ─────────────────────────────────────────────────────────────────────
repo_root        = Path(__file__).parent
db_dir           = repo_root / 'db'
db_path          = db_dir / 'kraken.duckdb'
cohort_cfg       = repo_root / 'cohort.yml'
data_dir         = repo_root / 'data'
mapping_dir      = repo_root / 'mapping'
templates_dir    = mapping_dir / 'mapping_templates'

powerbi_dir = repo_root / 'powerbi'

db_dir.mkdir(exist_ok=True)
powerbi_dir.mkdir(exist_ok=True)


# ── Helper: pick a file from a folder ────────────────────────────────────────
def pick_file(folder, extension, label):
    files = sorted(folder.glob(f'*.{extension}'))
    if not files:
        raise SystemExit(
            f"ERROR: No {extension} files found in {folder.name}/.\n"
            f"Please add a {extension} file and run again."
        )
    if len(files) == 1:
        print(f"{label}: {files[0].name}\n")
        return files[0]
    print(f"Multiple {label}s found. Which would you like to use?")
    for i, f in enumerate(files, 1):
        print(f"  {i}. {f.name}")
    while True:
        raw = input("> ")
        if raw.isdigit() and 1 <= int(raw) <= len(files):
            return files[int(raw) - 1]
        print(f"  Please enter a number between 1 and {len(files)}")


# ── Helper: auto-detect columns good for filtering ────────────────────────────
def detect_filterable_columns(csv):
    skip_patterns = ['_ID', '_DATE', '_AT', 'CREATED', 'UPDATED', 'START_', 'END_']
    skip_types    = {'INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'HUGEINT'}

    con        = duckdb.connect()
    csv_str    = str(csv).replace('\\', '/')
    schema     = con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{csv_str}')").fetchall()
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


# ── Helper: get value options for a column ────────────────────────────────────
def get_valid_values(csv, field):
    con     = duckdb.connect()
    csv_str = str(csv).replace('\\', '/')
    rows    = con.execute(f"""
        SELECT "{field}", COUNT(*) AS records
        FROM read_csv_auto('{csv_str}')
        GROUP BY "{field}"
        ORDER BY "{field}"
    """).fetchall()
    con.close()
    return rows


# ── Helper: prompt user to pick one value ─────────────────────────────────────
def prompt_choice(field, options):
    print(f"\nWhich {field} would you like to filter by?")
    for i, (value, count) in enumerate(options, 1):
        print(f"  {i}. {value}  ({count} records)")
    print(f"  {len(options) + 1}. (skip — do not filter on this column)")
    while True:
        raw = input("> ")
        if raw.isdigit() and 1 <= int(raw) <= len(options) + 1:
            idx = int(raw)
            if idx == len(options) + 1:
                return None
            return str(options[idx - 1][0])
        print(f"  Please enter a number between 1 and {len(options) + 1}")


# ── Step 1: Pick CSV, then load its matching mapping templates ───────────────
print("=" * 50)
print("  Kraken Migration — Cohort Builder")
print("=" * 50)
print()

csv_path     = pick_file(data_dir, 'csv', 'CSV file')

# Look for mapping templates generated for this specific CSV
csv_template_dir = templates_dir / csv_path.stem
if not csv_template_dir.exists() or not list(csv_template_dir.glob('*.yml')):
    raise SystemExit(
        f"ERROR: No mapping templates found for '{csv_path.name}'.\n"
        f"Please run generate_mappings.py first to generate templates in:\n"
        f"  {csv_template_dir}"
    )

print(f"Mapping templates found for: {csv_path.name}")
mapping_file = pick_file(csv_template_dir, 'yml', 'Entity mapping')

with open(mapping_file) as f:
    mapping = yaml.safe_load(f)

kraken_entity        = mapping['kraken_entity']
required_fields      = mapping['required_fields']
column_map           = mapping['column_map']
transformations      = mapping.get('transformations', {})
active_record_filter = mapping.get('active_record_filter', {})

print(f"Entity type: {kraken_entity}")


# ── Step 2: Cohort filter prompts ─────────────────────────────────────────────
print("\nScanning columns for filters...\n")
filterable_cols = detect_filterable_columns(csv_path)

filters = {}
for col in filterable_cols:
    options = get_valid_values(csv_path, col)
    value   = prompt_choice(col, options)
    if value is not None:
        filters[col] = value

print("\nCohort name (e.g. Cohort 1: Billing State Victoria only):")
cohort_name = input("> ").strip() or "unnamed_cohort"


# ── Step 3: Preview row count ─────────────────────────────────────────────────
csv_str = str(csv_path).replace('\\', '/')
where   = " AND ".join([f'"{k}" = \'{v}\'' for k, v in filters.items()]) or "1=1"
con     = duckdb.connect()
preview = con.execute(
    f"SELECT COUNT(*) FROM read_csv_auto('{csv_str}') WHERE {where}"
).fetchone()[0]
con.close()

print(f"\nCohort:         {cohort_name}")
print(f"Entity:         {kraken_entity}")
print(f"Filters:        {json.dumps(filters, indent=2)}")
print(f"Estimated rows: {preview}")
print("\nProceed? (y/n)")
if input("> ").strip().lower() != 'y':
    raise SystemExit("Cancelled.")


# ── Step 4: Save cohort.yml ───────────────────────────────────────────────────
with open(cohort_cfg, 'w') as f:
    yaml.dump({'cohort_name': cohort_name, 'filters': filters}, f, default_flow_style=False)
print("\ncohort.yml updated.")


# ── Step 5: Update dbt_project.yml csv_path ──────────────────────────────────
dbt_project_path = repo_root / 'dbt_project.yml'
with open(dbt_project_path) as f:
    dbt_project = yaml.safe_load(f)
dbt_project['vars']['csv_path'] = f"data/{csv_path.name}"
with open(dbt_project_path, 'w') as f:
    yaml.dump(dbt_project, f, default_flow_style=False, sort_keys=False)
print("dbt_project.yml updated.")


# ── Step 6: Regenerate valid_values.md ───────────────────────────────────────
lines = [
    "# Valid filter values",
    f"_Generated from `data/{csv_path.name}` on {datetime.now().strftime('%Y-%m-%d %H:%M')}_\n",
]
for col in filterable_cols:
    rows = get_valid_values(csv_path, col)
    lines.append(f"## {col}")
    for value, count in rows:
        lines.append(f"- `{value}` ({count} records)")
    lines.append("")
(repo_root / 'valid_values.md').write_text("\n".join(lines))
print("valid_values.md updated.")


# ── Step 7: Run dbt ───────────────────────────────────────────────────────────
env = os.environ.copy()
env['DUCKDB_PATH'] = str(db_path)

dbt_vars = json.dumps({
    "filters":               filters,
    "required_fields":       required_fields,
    "column_map":            column_map,
    "transformations":       transformations,
    "active_record_filter":  active_record_filter,
})

# Use the dbt executable from the same virtual environment as this Python interpreter.
dbt_executable = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
if not dbt_executable.exists():
    dbt_executable = Path("dbt")

result = subprocess.run(
    [str(dbt_executable), 'run', '--profiles-dir', '.', '--vars', dbt_vars],
    cwd=repo_root,
    capture_output=False,
    env=env
)

if result.returncode != 0:
    raise SystemExit("dbt run failed — check the output above")


# ── Step 8: Log to cohort_history ─────────────────────────────────────────────
con = duckdb.connect(str(db_path))

cohort_schema = """
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
# Drop and recreate if schema has changed (column count mismatch)
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
        con.execute(cohort_schema)
else:
    con.execute(cohort_schema)

rows_loaded   = con.execute("SELECT COUNT(*) FROM gold.gold_output").fetchone()[0]
rows_rejected = con.execute("SELECT COUNT(*) FROM bronze.bronze_data_rejected").fetchone()[0]
rows_read     = rows_loaded + rows_rejected

next_id = con.execute(
    "SELECT COALESCE(MAX(run_id), 0) + 1 FROM cohort_history"
).fetchone()[0]

con.execute(
    "INSERT INTO cohort_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [next_id, cohort_name, kraken_entity, csv_path.name,
     mapping_file.name, json.dumps(filters), rows_read, rows_loaded, rows_rejected, datetime.now()]
)

con.close()

print(f"\nPipeline complete — {rows_loaded} rows loaded, {rows_rejected} rejected, {rows_read} total read")
print(f"Run #{next_id} logged to cohort_history in db/kraken.duckdb")


# ── Step 9: Export tables to CSV for Power BI ─────────────────────────────────
print("\nExporting tables for Power BI...")

export_con = duckdb.connect(str(db_path))

export_tables = {
    'cohort_history':      'SELECT * FROM cohort_history',
    'bronze_data':         'SELECT * FROM bronze.bronze_data',
    'bronze_data_rejected':'SELECT * FROM bronze.bronze_data_rejected',
    'gold_output':         'SELECT * FROM gold.gold_output',
}

for table_name, query in export_tables.items():
    out_path = str(powerbi_dir / f'{table_name}.csv').replace('\\', '/')
    try:
        export_con.execute(f"COPY ({query}) TO '{out_path}' (FORMAT CSV, HEADER TRUE)")
        print(f"  ✓ powerbi/{table_name}.csv")
    except Exception as e:
        print(f"  ✗ {table_name} export failed: {e}")

export_con.close()
print(f"\nPower BI CSVs written to: powerbi/")
