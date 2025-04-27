import duckdb
import pandas as pd


data = {
    'id': [1, 1, 3, 4],
    'name': ['Alice', 'Alice', 'Charlie', 'David'],
    'age': [9,8,6,4]
}

cast_dict = {"age": "%Y%m%d"}

df = pd.DataFrame(data)

duck_connection = duckdb.connect()

relation = duck_connection.from_df(df)


relation =  relation.select("id,name,age").aggregate("sum(age) as age,name,id", "name,id")
relation.show()

'''
┌────────┬─────────┬───────┐
│  age   │  name   │  id   │
│ int128 │ varchar │ int64 │
├────────┼─────────┼───────┤
│     17 │ Alice   │     1 │
│      4 │ David   │     4 │
│      6 │ Charlie │     3 │
└────────┴─────────┴───────┘

'''