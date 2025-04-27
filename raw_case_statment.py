import duckdb
import pandas as pd


data = {
    'id': [1, 1, 3, 4],
    'name': ['Alice', 'Trey', 'Charlie', 'David'],
    'age': [9,43,90,None]
}

cast_dict = {"age": "%Y%m%d"}

df = pd.DataFrame(data)

duck_connection = duckdb.connect()

relation = duck_connection.from_df(df)


result = relation.select("""
    id, 
    name, 
    age, 
    CASE 
        WHEN age < 30 THEN 'Young'
        WHEN age BETWEEN 30 AND 50 THEN 'Middle-aged'
        WHEN age > 50 THEN 'Older'
        ELSE 'Unknown'
    END AS age_category
""")

result.show()

'''
┌───────┬─────────┬────────┬──────────────┐
│  id   │  name   │  age   │ age_category │
│ int64 │ varchar │ double │   varchar    │
├───────┼─────────┼────────┼──────────────┤
│     1 │ Alice   │    9.0 │ Young        │
│     1 │ Trey    │   43.0 │ Middle-aged  │
│     3 │ Charlie │   90.0 │ Older        │
│     4 │ David   │   NULL │ Unknown      │
└───────┴─────────┴────────┴──────────────┘

'''