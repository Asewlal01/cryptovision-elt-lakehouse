import polars as pl

# Columns given in the Binance Vision trade CSV
RAW_COLUMNS = [
    'trade_id',
    'price',
    'qty',
    'quoteQty',
    'time',
    'isBuyerMaker',
    'isBestMatch',
]

CANONICAL_COLUMNS = [
    'trade_id',
    'ts_utc',
    'price',
    'qty',
]

def enforce_trades_schema(trades_df: pl.DataFrame) -> pl.DataFrame:
    """_summary_

    Args:
        trades_df (pl.DataFrame): _description_

    Returns:
        pl.DataFrame: _description_
    """
    # Number of columns checking
    expected_num_columns = len(RAW_COLUMNS)
    given_num_columns = len(trades_df.columns)
    if expected_num_columns != given_num_columns:
        raise ValueError(f'Expected {expected_num_columns} columns, got {given_num_columns}')

    trades_df.columns = RAW_COLUMNS

    trades_df = trades_df.with_columns(
        [
            pl.col('trade_id').cast(pl.Int64),
            pl.col('price').cast(pl.Float64),
            pl.col('qty').cast(pl.Float64),
            pl.col('time').cast(pl.Int64)
        ]
    )

    trades_df = trades_df.with_columns(
        pl.from_epoch(pl.col('time'), time_unit='ms').alias('ts_utc')
    )

    canonical_trades_df = trades_df.select(CANONICAL_COLUMNS)
    return canonical_trades_df

    


