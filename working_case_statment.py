import duckdb
import pandas as pd


data = {
    'id': [1, 1, 3, 4],
    'name': ['Alice', 'Trey', 'Charlie', 'David'],
    'age': [9,43,90,None]
}

cast_dict = {"age": "%Y%m%d"}

df = pd.DataFrame(data)

# Connect to DuckDB and register the pandas DataFrame
duck_connection = duckdb.connect()

relation = duck_connection.from_df(df)


# Using CASE statement to categorize age
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

# Displaying the result
result.show()