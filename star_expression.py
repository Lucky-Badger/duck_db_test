import duckdb
import pandas as pd

df = pd.DataFrame({
    'a': [1, 2, 3, 4],
    'b': [True, None, False, True],
    'c': [42, 21, 13, 14]
})

star = duckdb.StarExpression(exclude = ['b'])
duck_connection = duckdb.connect()
rel = duck_connection.from_df(df)
res = rel.select(star).fetchall()
print(res)

# OR 
# cols_to_exclude = ['b']
# star = duckdb.StarExpression(exclude = [*cols_to_exclude])
# duck_connection = duckdb.connect()
# rel = duck_connection.from_df(df)
# res = rel.select(star).fetchall()
# print(res)