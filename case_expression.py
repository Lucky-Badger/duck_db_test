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

duck_connection = duckdb.connect()

rel = duck_connection.from_df(df)

hello = const('hello')
world = const('world')

case = \
    CaseExpression(condition = col('a') == const(1), value = world)\
        .when(condition = col('a') == const(2), value = const('test')) \
        .otherwise(hello).alias('case_expression')

res = rel.select(f'c,{case} AS case_expression') # Alias not working as expected on case expression

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