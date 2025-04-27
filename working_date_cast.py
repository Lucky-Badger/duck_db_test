import duckdb
import pandas as pd


data = {
    'id': [1, 2, 3, 4],
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'age': ["20241017", "20241003", "20240910", "20240914"]
}

cast_dict = {"age": "%Y%m%d"}

df = pd.DataFrame(data)

duck_connection = duckdb.connect()

relation = duck_connection.from_df(df)


select_columns = []
for column, date_format in cast_dict.items():
    select_columns.append(f"CAST(strptime({column}, '{date_format}') AS DATE) AS {column}")

for column in df.columns:
    if column not in cast_dict:
        select_columns.append(column)
print(select_columns)

query = "SELECT " + ", ".join(select_columns) + " FROM relation"


result = duck_connection.sql(query)
result.show()

'''
┌────────────┬───────┬─────────┐
│    age     │  id   │  name   │
│    date    │ int64 │ varchar │
├────────────┼───────┼─────────┤
│ 2024-10-17 │     1 │ Alice   │
│ 2024-10-03 │     2 │ Bob     │
│ 2024-09-10 │     3 │ Charlie │
│ 2024-09-14 │     4 │ David   │
└────────────┴───────┴─────────┘

'''