import duckdb
import pandas as pd
from typing import cast as t_cast
from duckdb import (
    ColumnExpression as col,
    ConstantExpression as const
)
from duckdb.typing import DuckDBPyType 

df = pd.DataFrame({
    'a': ['a','b','c','d'],
    'd': ['a','b','c','d'],
    'date_str': ['20241017', '20231105', '20230220', '20240101'],
    'date_str2': ['20241017', '20231105', '20230220', '20240101']
})

duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

date_columns = ['date_str', 'date_str2']
formatted_columns = []

for i, date_col in enumerate(date_columns, 1):
    formatted_column = duckdb.FunctionExpression(
        'strptime',
        col(date_col),
        const('%Y%m%d')
    ).cast(t_cast(DuckDBPyType,'DATE')).alias(f'{date_col}_date')  # To avoid mypy from complaining either ignore line or cast like this
    
    formatted_columns.append(formatted_column)

print(formatted_columns)
unformat_cols = list(set(rel.columns) - set(date_columns))
columns_to_select = [*unformat_cols] + formatted_columns
res = rel.select(*columns_to_select)

res.show()

'''
┌─────────┬─────────┬───────────────┬────────────────┐
│    a    │    d    │ date_str_date │ date_str2_date │
│ varchar │ varchar │     date      │      date      │
├─────────┼─────────┼───────────────┼────────────────┤
│ a       │ a       │ 2024-10-17    │ 2024-10-17     │
│ b       │ b       │ 2023-11-05    │ 2023-11-05     │
│ c       │ c       │ 2023-02-20    │ 2023-02-20     │
│ d       │ d       │ 2024-01-01    │ 2024-01-01     │
└─────────┴─────────┴───────────────┴────────────────┘
'''
