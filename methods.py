import duckdb
from duckdb import DuckDBPyRelation
import pandas as pd

# Step 1: Connect
data = {
    'id': [1, 1, 3, 4],
    'name': ['Alice', 'Trey', 'bob', 'David'],
    'age': [9,43,90,None]
}

cast_dict = {"age": "%Y%m%d"}

df = pd.DataFrame(data)

duck_connection = duckdb.connect(database=':memory:', read_only=False)



# Step 3: Define a function that filters a relation
def filter_names(relation):
    # Using relation methods (not SQL)
    return relation.filter("name != 'bob'")

# Step 4: Define another function that uses .sql() inside
def uppercase_names(relation: DuckDBPyRelation, con):
    # Using connection.sql(), returns another relation
    uppcase_relation = relation.project('id, UPPER(name) AS name_upper, age')
    # finaL_relation = con.sql('SELECT name_upper, LOWER(name_upper) AS name_lower FROM uppcase_relation')
    finaL_relation = uppcase_relation.select('name_upper, LOWER(name_upper) AS name_lower')
    return finaL_relation

# Step 5: Use the functions
relation = duck_connection.from_df(df)
rel_filtered = filter_names(relation)
rel_2 = uppercase_names(rel_filtered, duck_connection)

# Step 6: Show results
# print("Filtered relation (no 'bob'):")
rel_filtered.show()

# print("\nUppercased names relation:")
rel_2.show()
