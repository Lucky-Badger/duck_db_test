import duckdb
import pandas as pd
from duckdb import (
    ColumnExpression as col,
    ConstantExpression as const
)

# Sample DataFrame
df = pd.DataFrame({
    'a': ['a','b','c','d'],
    'date_str': ['20241017', '20231105', '20230220', '20240101'],
    'date_str2': ['20241017', '20231105', '20230220', '20240101']
})

# Create DuckDB connection and relation
duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

# Apply the format function using columns as inputs
formatted_column1 = duckdb.FunctionExpression(
    'strptime',
    col('date_str'),
    const('%Y%m%d')
    ).cast('DATE')

# Select the formatted string
res = rel.select(f"a, {formatted_column1} AS date")
res.show()


formatted_column2 = duckdb.FunctionExpression(
    'strptime',
    col('date_str2'),
    const('%Y%m%d')
).cast('DATE')

# Select the columns including formatted date columns
res = rel.select(f"a, {formatted_column1} AS date1, {formatted_column2} AS date2")

# Show the result
res.show()