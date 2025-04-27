import pytest
import duckdb
import pandas as pd

def add_decimal_columns(df):
    con = duckdb.connect()

    relation = con.from_df(df)
    
    result_relation = relation.select(
        "id, CAST(amount1 AS DECIMAL(16, 8)) + CAST(amount2 AS DECIMAL(16, 8)) AS total"
    )
    
    result_df = result_relation.df()
    return result_df


@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'id': ['jim', 'jones', 'Bob', 'Shirley', 'jewn'],
        'amount1': [12345.12345678, 98765.87654321, 45678.98765432, 123456.12345678, 99999.99999999],
        'amount2': [2345.23456789, 8765.76543210, 5678.87654321, 23456.23456789, 10000.00000001]
    })


def test_add_decimal_columns(sample_data):
    result = add_decimal_columns(sample_data)
    
    expected_result = pd.DataFrame({
        'id': ['jim', 'jones', 'Bob', 'Shirley', 'jewn'],
        'total': [
            14690.35802467,   # 12345.12345678 + 2345.23456789
            107531.64297531,  # 98765.87654321 + 8765.76543210
            51357.86419753,   # 45678.98765432 + 5678.87654321
            146912.35802467,  # 123456.12345678 + 23456.23456789
            109999.99999900   # 99999.99999999 + 10000.00000001
        ]
    })
    
    pd.testing.assert_frame_equal(result, expected_result)
