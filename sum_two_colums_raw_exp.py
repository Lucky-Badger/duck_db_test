import duckdb


rel = duckdb.sql("""
    SELECT 
        CAST(100.12345678 AS DECIMAL(38, 8)) AS col1,
        CAST(200.87654321 AS DECIMAL(38, 8)) AS col2
""")

result = rel.select("""
    col1, 
    col2, 
    CAST(col1 + col2 AS DECIMAL(38, 8)) AS sum_col
""")

result.show()

'''
┌───────────────┬───────────────┬───────────────┐
│     col1      │     col2      │    sum_col    │
│ decimal(38,8) │ decimal(38,8) │ decimal(38,8) │
├───────────────┼───────────────┼───────────────┤
│  100.12345678 │  200.87654321 │  300.99999999 │
└───────────────┴───────────────┴───────────────┘

'''