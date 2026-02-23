import zipfile
import polars as pl
import pathlib


class NoFilesError(Exception):
    def __init__(self):
        super().__init__('No CSV files located in the ZIP')

class MultipleFilesError(Exception):
    def __init__(self):
        super().__init__('Multiple CSV files located in the ZIP')

def load_bronze_trades_zip(zip_path: pathlib.Path) -> pl.DataFrame:
    """
    Load bronze zip file and return the csv as a dataframe

    Args:
        zip_path (pathlib.Path): Path to zip file

    Returns:
        pl.DataFrame: Dataframe with trade data
    """    

    with zipfile.ZipFile(zip_path) as zf:
        csv_files = [
            name for name in zf.namelist() 
            if name.lower().endswith('.csv') and not name.endswith("/")
        ]
        if len(csv_files) == 0:
            raise NoFilesError()
        if len(csv_files) > 1:
            raise MultipleFilesError()
        
        csv_file = csv_files[0]
        with zf.open(csv_file, "r") as f:
            return pl.read_csv(f, has_header=False)

def write_silver_trades_parquet(enforced_trades_df: pl.DataFrame,
                                silver_file_path: pathlib.Path
                                ) -> int:
    """
    Write trades dataframe with enforces scheme to path

    Args:
        enforced_trades_df (pl.DataFrame): Dataframe with enforced scheme
        silver_file_path (pathlib.Path): Path to write parquet file to
        
    Returns:
        int: Number of bytes written
    """
    silver_file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_file = silver_file_path.with_name(silver_file_path.name + '.tmp')

    # Attempt writing of dataframe
    try: 
        enforced_trades_df.write_parquet(temp_file)
        temp_file.replace(silver_file_path)
    finally:
        if temp_file.exists():
            temp_file.unlink()
    
    bytes_written = silver_file_path.stat().st_size
    return bytes_written

