import duckdb
import pandas as pd
from duckdb import ConstantExpression as const
from duckdb import DuckDBPyRelation

df = pd.DataFrame({
    'a': [1, 2, 3, 4],
    'b': [True, None, False, True],
    'c': [42, 21, 13, 14]
})

duck_connection = duckdb.connect()

rel = duck_connection.from_df(df)



col_list = [
    (duckdb.ColumnExpression('a') * const(10)).alias('a'),
    duckdb.ColumnExpression('b').isnull().alias('b'), 
    (duckdb.ColumnExpression('c') + const(5)).alias('c_d')
]

rel = rel.select(*col_list)
rel.show()

'''
┌───────┬─────────┬─────────┐
│   a   │    b    │ (c + 5) │
│ int64 │ boolean │  int64  │
├───────┼─────────┼─────────┤
│    10 │ false   │      47 │
│    20 │ true    │      26 │
│    30 │ false   │      18 │
│    40 │ false   │      19 │
└───────┴─────────┴─────────┘
'''