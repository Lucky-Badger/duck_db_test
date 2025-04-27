import duckdb
import pandas as pd

df = pd.DataFrame({
    'name': ['CSV', 'JSON', 'Parquet', 'XML'],
    'time_taken': [42, 30, 55, 60]
})

duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

sql_query = """
    SELECT *,
           (contains(name, 'CSV') OR contains(name, 'JSON') OR contains(name, 'XML')) AS contains_multiple_values
    FROM rel
"""

res = duck_connection.sql(sql_query).show()

print(res)
'''
┌─────────┬────────────┬──────────────────────────┐
│  name   │ time_taken │ contains_multiple_values │
│ varchar │   int64    │         boolean          │
├─────────┼────────────┼──────────────────────────┤
│ CSV     │         42 │ true                     │
│ JSON    │         30 │ true                     │
│ Parquet │         55 │ false                    │
│ XML     │         60 │ true                     │
└─────────┴────────────┴──────────────────────────┘
'''