import duckdb
import pandas as pd


# data = {
#     'id': [1, 1, 3, 4],
#     'name': ['Alice', 'Trey', 'Charlie', 'David'],
#     'age': [9,43,90,None]
# }

# cast_dict = {"age": "%Y%m%d"}

# df = pd.DataFrame(data)

# # Connect to DuckDB and register the pandas DataFrame
# duck_connection = duckdb.connect()

# relation = duck_connection.from_df(df)


# Creating the relation with decimal columns
rel = duckdb.sql("""
    SELECT 
        CAST(100.12345678 AS DECIMAL(38, 8)) AS col1,
        CAST(200.87654321 AS DECIMAL(38, 8)) AS col2
""")

# Adding the two decimal columns and casting the result to DECIMAL(38, 8)
result = rel.select("""
    col1, 
    col2, 
    CAST(col1 + col2 AS DECIMAL(38, 8)) AS sum_col
""")

# Displaying the result
result.show()