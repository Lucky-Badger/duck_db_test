import duckdb
import pandas as pd

# Sample DataFrame
df = pd.DataFrame({
    'name': ['CSV', 'JSON', 'Parquet'],
    'time_taken': [42, 30, 55]
})

# Create DuckDB connection and relation
duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

# Apply the format function using columns as inputs
formatted_column = duckdb.FunctionExpression(
    'format',
    duckdb.ConstantExpression('Benchmark "{}" took {} seconds'),
    duckdb.ColumnExpression('name'),
    duckdb.ColumnExpression('time_taken')
)

# Select the formatted string
res = rel.select(f"name, {formatted_column} AS test")
res.show()
