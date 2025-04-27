import duckdb
import pandas as pd


data = {
    'id': [1, 2, 3, 4],
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'age': ["20241017", "20241003", "20240910", "20240914"]
}

cast_dict = {"age": "%Y%m%d"}

df = pd.DataFrame(data)

# Connect to DuckDB and register the pandas DataFrame
duck_connection = duckdb.connect()

relation = duck_connection.from_df(df)


# Dynamically build the SELECT statement
select_columns = []
for column, date_format in cast_dict.items():
    select_columns.append(f"CAST(strptime({column}, '{date_format}') AS DATE) AS {column}")

# Add other columns that  remain unchanged
for column in df.columns:
    if column not in cast_dict:
        select_columns.append(column)
print(select_columns)

query = "SELECT " + ", ".join(select_columns) + " FROM relation"


result = duck_connection.sql(query)
result.show()
