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

