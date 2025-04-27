import duckdb
import pandas as pd


data = {
    'id': [1, 1, 3, 4],
    'name': ['Alice', 'Trey', 'Charlie', 'David'],
    'age': [9,43,90,None]
}

cast_dict = {"age": "%Y%m%d"}

df = pd.DataFrame(data)

duck_connection = duckdb.connect(database=':memory:', read_only=False)

relation = duck_connection.from_df(df)


relation_1 = duck_connection.sql("""
    SELECT * FROM relation where id = 1
                               """)

relation = duck_connection.sql("""
                               SELECT * FROM relation_1 where name = 'Alice'
                               """)


relation_3 = duck_connection.sql("""
                               SELECT * FROM relation_1 where name = 'Trey'
                               """)
relation.show()
relation_3.show()

'''
┌───────┬─────────┬────────┐
│  id   │  name   │  age   │
│ int64 │ varchar │ double │
├───────┼─────────┼────────┤
│     1 │ Alice   │    9.0 │
└───────┴─────────┴────────┘

┌───────┬─────────┬────────┐
│  id   │  name   │  age   │
│ int64 │ varchar │ double │
├───────┼─────────┼────────┤
│     1 │ Trey    │   43.0 │
└───────┴─────────┴────────┘
'''