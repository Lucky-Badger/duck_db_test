import duckdb
import pandas as pd
from duckdb import (
    ConstantExpression as const,
    ColumnExpression as col
)


df = pd.DataFrame({
    'name': ['CSV', 'JSON', 'Parquet', 'XML'],
    'time_taken': [42, 30, 55, 60]
})

duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)

val_list = ['CSV','JSON']
values = [const(val) for val in val_list]

rel.filter(col('name').isin(*values)).show()

'''
┌─────────┬────────────┐
│  name   │ time_taken │
│ varchar │   int64    │
├─────────┼────────────┤
│ CSV     │         42 │
│ JSON    │         30 │
└─────────┴────────────┘
'''