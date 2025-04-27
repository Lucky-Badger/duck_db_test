import duckdb
import pandas as pd

df = pd.DataFrame({
    'name': ['CSV', 'JSON', 'Parquet'],
    'time_taken': [42, 30, 55]
})

duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

formatted_column = duckdb.FunctionExpression(
    'format',
    duckdb.ConstantExpression('Benchmark "{}" took {} seconds'),
    duckdb.ColumnExpression('name'),
    duckdb.ColumnExpression('time_taken')
)

res = rel.select(f"name, {formatted_column} AS test")
res.show()

'''
┌─────────┬─────────────────────────────────────┐
│  name   │                test                 │
│ varchar │               varchar               │
├─────────┼─────────────────────────────────────┤
│ CSV     │ Benchmark "CSV" took 42 seconds     │
│ JSON    │ Benchmark "JSON" took 30 seconds    │
│ Parquet │ Benchmark "Parquet" took 55 seconds │
└─────────┴─────────────────────────────────────┘
'''