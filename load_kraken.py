import duckdb
import requests
import json
from pathlib import Path

repo_root = Path(__file__).parent

con  = duckdb.connect(str(repo_root / 'db' / 'kraken.duckdb'))
rows = con.execute("SELECT * FROM gold.gold_output").fetchall()
cols = [d[0] for d in con.description]
con.close()

KRAKEN_URL = "https://your-kraken-endpoint/graphql"
MUTATION = """
mutation CreateAccount($input: CreateAccountInput!) {
    createAccount(input: $input) {
        id
    }
}
"""

for row in rows:
    record = dict(zip(cols, row))

    # Gold output columns are already named to match the Kraken API field names
    # so we can pass the record directly as the input payload
    payload = {
        "query": MUTATION,
        "variables": {
            "input": record
        }
    }
    response = requests.post(KRAKEN_URL, json=payload)
    print(f"Row {record}: {response.status_code}")
