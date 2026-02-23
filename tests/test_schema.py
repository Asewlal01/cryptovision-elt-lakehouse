import polars as pl
from cryptovision.silver_processing.schema import enforce_trades_schema, CANONICAL_COLUMNS

def test_valid_schema_output():
    """
    Test whether schema enforcement returns dataframe with correct columns, valid types and no rows are dropped or added
    """    
    df_raw = pl.DataFrame(
        {
            "column_0": [1, 2, 3],
            "column_1": ["100.5", "101.0", "99.75"],
            "column_2": ["0.25", "0.10", "1.00"],
            "column_3": ["25.125", "10.10", "99.75"],
            "column_4": [1609459200000, 1609459260000, 1609459320000],
            "column_5": [True, False, True],
            "column_6": [True, True, False],
        }
    )

    df = enforce_trades_schema(df_raw)

    # Column checking
    assert df.columns == CANONICAL_COLUMNS

    # Type checking
    df_types = df.dtypes
    expected_types = [pl.Int64, pl.Datetime('ms'), pl.Float64, pl.Float64]
    for df_type, expected_type in zip(df_types, expected_types):
        assert df_type == expected_type
    
    # Number of rows checking
    assert df_raw.height == df.height