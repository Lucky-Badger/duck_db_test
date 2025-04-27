import duckdb
import pandas as pd

df = pd.DataFrame({
    'name': ['CSV', 'JSON', 'Parquet'],
    'time_taken': [42, 30, 55]
})

duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

formatted_column = duckdb.FunctionExpression(
    'contains',
    duckdb.ColumnExpression('name'),
    duckdb.ConstantExpression('CSV')
)

res = rel.select(f"name, {formatted_column} AS test")
res.show()

'''
┌─────────┬─────────┐
│  name   │  test   │
│ varchar │ boolean │
├─────────┼─────────┤
│ CSV     │ true    │
│ JSON    │ false   │
│ Parquet │ false   │
└─────────┴─────────┘

'''