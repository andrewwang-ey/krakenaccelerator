import duckdb
con = duckdb.connect('db/kraken.duckdb')
print(con.execute("SELECT * FROM cohort_history ORDER BY run_at DESC").df())