import subprocess
from pathlib import Path

# All paths are relative to Kraken-Accelerator/ (the git repo root)
repo_root = Path(__file__).parent
db_dir = repo_root / 'db'
dbt_dir = repo_root / 'kraken_dbt'

db_dir.mkdir(exist_ok=True)

result = subprocess.run(
    ['dbt', 'run', '--profiles-dir', '.'],
    cwd=dbt_dir,
    capture_output=False
)

if result.returncode != 0:
    raise SystemExit("dbt run failed — check the output above")

print("Pipeline complete. Gold layer ready in db/kraken.duckdb")
