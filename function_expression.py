import duckdb
import pandas as pd
from duckdb import (
    ConstantExpression,
    ColumnExpression,
    FunctionExpression
)

df = pd.DataFrame({
    'a': [
        'test',
        'pest',
        'text',
        'rest',
    ]
})

duck_connection = duckdb.connect()

rel = duck_connection.from_df(df)

ends_with = FunctionExpression('ends_with', ColumnExpression('a'), ConstantExpression('est')).alias('c') # Ends with documentation, https://duckdb.org/docs/stable/sql/functions/char#ends_withstring-search_string
res = rel.select(ends_with)
res.show()

# List of functions can be found here
# https://duckdb.org/docs/stable/sql/functions/overview

'''
┌─────────┐
│    c    │
│ boolean │
├─────────┤
│ true    │
│ true    │
│ false   │
│ true    │
└─────────┘

'''