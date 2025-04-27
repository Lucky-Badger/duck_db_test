import pandas as pd
from datetime import datetime
import duckdb
from duckdb import (
    ConstantExpression as const,
    ColumnExpression as col
)

# Create the data
data = {
    'term': ['long', 'short', 'short', 'short'],
    'event_date': [
        datetime(2024, 10, 18),
        datetime(2024, 10, 17),
        datetime(2024, 10, 17),
        datetime(2024, 10, 16)
    ]
}

df = pd.DataFrame(data)

duck_connection = duckdb.connect(database=':memory:', read_only=False)

relation = duck_connection.from_df(df)

exp = (col('term') == const('short'))
relation  = relation.filter(exp)
max_relation = relation.max('event_date')

max_date = max_relation.fetchone()[0]

date_filtered_relation = relation.filter((col('event_date') == const(max_date))).show()

'''
┌─────────┬─────────────────────┐
│  term   │     event_date      │
│ varchar │    timestamp_ns     │
├─────────┼─────────────────────┤
│ short   │ 2024-10-17 00:00:00 │
│ short   │ 2024-10-17 00:00:00 │
└─────────┴─────────────────────┘

'''