import pathlib
import datetime

class InvalidSymbolError(Exception):
    def __init__(self):
        super().__init__('Symbol is invalid')

def build_partition_path(root: pathlib.Path,
                        symbol: str,
                        date: datetime.date
                        ) -> pathlib.Path:
    """
    Build path to parent directory based on symbol and date

    Args:
        root (pathlib.Path): Directory of root
        symbol (str): Cryptocurrency pair
        date (datetime.date): Date of interest

    Returns:
        pathlib.Path: Parent directory of saved file
    """
    if not symbol:
        raise InvalidSymbolError()

    if type(date) is not datetime.date:
        raise TypeError('Date is not given as date object')

    date_str = date.isoformat()
    partition_path = root.joinpath(f'symbol={symbol}', f'date={date_str}')
    return partition_path

def build_bronze_zip_path(bronze_path: pathlib.Path,
                          symbol: str,
                          date: datetime.date
                          ) -> pathlib.Path:
    """
    Build path to the zip trades file from the bronze layer

    Args:
        bronze_path (pathlib.Path): Path to the bronze layer directory
        symbol (str): Cryptocurrency pair
        date (datetime.date): Date of interest

    Returns:
        pathlib.Path: Path to saved file
    """
    symbol = symbol.strip().upper()
    partition_path = build_partition_path(bronze_path, symbol, date)
    
    date_str = date.isoformat()
    file_name = f'{symbol}-trades-{date_str}.zip'
    bronze_zip_path = partition_path.joinpath(file_name)

    return bronze_zip_path

def build_silver_parquet_path(silver_path: pathlib.Path,
                          symbol: str,
                          date: datetime.date
                          ) -> pathlib.Path:
    """
    Build path to the parquet trades file from the silver layer

    Args:
        silver_path (pathlib.Path): Path to the silver layer directory
        symbol (str): Cryptocurrency pair
        date (datetime.date): Date of interest

    Returns:
        pathlib.Path: Path to saved file
    """
    symbol = symbol.strip().upper()
    partition_path = build_partition_path(silver_path, symbol, date)

    file_name = 'trades.parquet'
    silver_parquet_path = partition_path.joinpath(file_name)

    return silver_parquet_path

