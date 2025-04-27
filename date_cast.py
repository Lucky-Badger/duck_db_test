import duckdb
import pandas as pd
from duckdb import (
    ColumnExpression as col,
    ConstantExpression as const
)

# Sample DataFrame
df = pd.DataFrame({
    'a': ['a','b','c','d'],
    'd': ['a','b','c','d'],
    'date_str': ['20241017', '20231105', '20230220', '20240101'],
    'date_str2': ['20241017', '20231105', '20230220', '20240101']
})

# Create DuckDB connection and relation
duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

# List of date column names to format
date_columns = ['date_str', 'date_str2']
formatted_columns = []

# Loop through each column and create the formatted columns
for i, date_col in enumerate(date_columns, 1):
    formatted_column = duckdb.FunctionExpression(
        'strptime',
        col(date_col),
        const('%Y%m%d')
    ).cast('DATE').alias(f'{date_col}_date')  # Aliasing dynamically as date1, date2, etc.
    
    # Add the formatted column to the list
    formatted_columns.append(formatted_column)

print(formatted_columns)
# Dynamically select columns, including formatted date columns
unformat_cols = list(set(rel.columns) - set(date_columns))
columns_to_select = [*unformat_cols] + formatted_columns  # Add 'a' along with formatted columns
res = rel.select(*columns_to_select)

# Show the results
res.show()

