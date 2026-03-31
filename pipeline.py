import subprocess
from pathlib import Path
import os

# All paths are relative to Kraken-Accelerator/ (the git repo root)
repo_root = Path(__file__).parent
db_dir = repo_root / 'db'
db_dir.mkdir(exist_ok=True)

env = os.environ.copy()
env['DUCKDB_PATH'] = str(db_dir / 'kraken.duckdb')

result = subprocess.run(
    ['dbt', 'run', '--profiles-dir', '.'],
    cwd=repo_root,
    capture_output=False,
    env=env
)

if result.returncode != 0:
    raise SystemExit("dbt run failed — check the output above")

print("Pipeline complete. Gold layer ready in db/kraken.duckdb")
