import duckdb

# Connect to an in-memory DuckDB
duck_conn = duckdb.connect(database=':memory:', read_only=False)

# Create two datasets with similar column names (in this case, 'id' and 'name')
duck_conn.execute("""
    CREATE TABLE users_1 (
        id INTEGER,
        name VARCHAR,
        age INTEGER
    )
""")
duck_conn.execute("""
    INSERT INTO users_1 (id, name, age) VALUES
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35)
""")

duck_conn.execute("""
    CREATE TABLE users_2 (
        id INTEGER,
        name VARCHAR,
        country VARCHAR
    )
""")
duck_conn.execute("""
    INSERT INTO users_2 (id, name, country) VALUES
    (1, 'Alice', 'USA'),
    (2, 'Bob', 'UK'),
    (4, 'David', 'Canada')
""")

# Load the datasets as relations
rel1 = duck_conn.table('users_1').set_alias("r1")
rel2 = duck_conn.table('users_2').set_alias("r2")

# Step 1: Perform an inner join between the two relations on the 'id' column
condition = 'r1.id=r2.id AND r1.name=r2.name'
joined_rel = rel1.join(rel2, condition=condition, how='inner').select("r1.id, r2.country")


joined_rel.show()

# # Step 2: Execute the query and get the results
# result = joined_rel.execute().fetchall()

# # Step 3: Print the results
# print(result)

# # Step 4: Clean up and close the connection
# duck_conn.close()