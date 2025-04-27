import duckdb
import pandas as pd
from  duckdb import (
    ConstantExpression as const,
    ColumnExpression as col,
    StarExpression as star
)

def expand_cols_helper(relation, alias):
    return [f"{alias}.{col}" for col in relation.columns]

df1 = pd.DataFrame({
        'id': ['jim', 'jones', 'Bob', 'Shirley', 'pip'],
        'amount1': [12345.12345678, 98765.87654321, 45678.98765432, 123456.12345678, 99999.99999999],
        'amount2': [2345.23456789, 8765.76543210, 5678.87654321, 23456.23456789, 10000.00000001]
    })

df2 = pd.DataFrame({
        'id': ['jim', 'jones', 'Bob', 'Shirley', 'pip'],
        'amount1': [12345.12345678, 98765.87654321, 45678.98765432, 123456.12345678, 99999.99999999],
        'amount2': [2345.23456789, 8765.76543210, 5678.87654321, 23456.23456789, 10000.00000001]
    })


con = duckdb.connect()

relation_1 = con.from_df(df1).set_alias("r1")

relation_2 = con.from_df(df2).set_alias("r2")


relation_result = (
    relation_1
    .join(relation_2, "r1.id = r2.id", "inner")
    .select(
        *expand_cols_helper(relation_1, "r1"), # Either use straight strings, or expressions, thus we can't use r1.*
        (col('r1.amount1') + col('r2.amount2')).cast('DECIMAL(38,8)').alias('calc_col')
    )
)

relation_result.show()

# OR
 
relation_result2 = (
    relation_1
    .join(relation_2, "r1.id = r2.id", "inner")
    .select(
        f"r1.*, \
        {((col('r1.amount1') + col('r2.amount2')).cast('DECIMAL(38,8)'))} AS calc_col" # Alias not working as intended so just rename
    )
)

relation_result2.show()

'''

'''