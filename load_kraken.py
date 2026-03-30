import duckdb, requests, json

con = duckdb.connect('db/kraken.duckdb')
rows = con.execute("SELECT * FROM gold.billing_address").fetchall()
cols = [d[0] for d in con.description]

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
    payload = {
        "query": MUTATION,
        "variables": {
            "input": {
                "billingAddress": {
                    "address1": record["address1"],
                    "address2": record["address2"],  # may be None — Kraken accepts null
                    "city":     record["city"],
                    "state":    record["state"],
                    "zipCode":  record["zipCode"],
                }
            }
        }
    }
    response = requests.post(KRAKEN_URL, json=payload)
    print(f"{record['PARTY_ID']}: {response.status_code}")