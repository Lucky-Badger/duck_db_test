import duckdb
import pandas as pd
from duckdb import (
    ConstantExpression as const,
    ColumnExpression as col,
    CaseExpression
)

df = pd.DataFrame({
    'a': [1, 2, 3, 4],
    'b': [True, None, False, True],
    'c': [42, 21, 13, 14]
})

# Create a DuckDB relation from the DataFrame
duck_connection = duckdb.connect()

rel = duck_connection.from_df(df)

hello = const('hello')
world = const('world')

case = \
    CaseExpression(condition = col('a') == const(1), value = col('a').cast(str))\
        .when(condition = col('a') == const(2), value = const('test')) \
        .otherwise(hello)

res = rel.select(f'c,{case} AS case_expression')

print(res)
'''
┌───────┬─────────────────┐
│   c   │ case_expression │
│ int64 │     varchar     │
├───────┼─────────────────┤
│    42 │ world           │
│    21 │ test            │
│    13 │ hello           │
│    14 │ hello           │
└───────┴─────────────────┘
'''
