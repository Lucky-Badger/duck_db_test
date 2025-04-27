import duckdb
import pandas as pd

# Sample DataFrame
df = pd.DataFrame({
    'name': ['CSV', 'JSON', 'Parquet', 'XML'],
    'time_taken': [42, 30, 55, 60]
})

# Create DuckDB connection and relation
duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

# Combine multiple contains checks with OR to check for 'CSV' or 'JSON'
sql_query = """
    SELECT *,
           (contains(name, 'CSV') OR contains(name, 'JSON') OR contains(name, 'XML')) AS contains_multiple_values
    FROM rel
"""

# Execute the query
res = duck_connection.sql(sql_query).show()

# Display the result
print(res)