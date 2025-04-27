import duckdb
import pandas as pd
from duckdb import ConstantExpression as const
from duckdb import DuckDBPyRelation

# Create a sample DataFrame
df = pd.DataFrame({
    'a': [1, 2, 3, 4],
    'b': [True, None, False, True],
    'c': [42, 21, 13, 14]
})

# Create a DuckDB relation from the DataFrame
duck_connection = duckdb.connect()

rel = duck_connection.from_df(df)


# Create a list of columns with transformations

col_list = [
    (duckdb.ColumnExpression('a') * const(10)).alias('a'),  # Multiply column 'a' by 10
    duckdb.ColumnExpression('b').isnull().alias('b'),  # Check if 'b' is null
    (duckdb.ColumnExpression('c') + const(5)).alias('c_d')   # Add 5 to column 'c'
]

# Select the transformed columns and fetch results
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