from cryptovision.silver_processing.io import *
import zipfile
from pathlib import Path
import pytest
from datetime import datetime

def create_zip_with_csv(zip_path: Path, file_dict: dict[str, str]):
    """
    Generate a zip file with files in it

    Args:
        zip_path (Path): location to save csv
        file_dict (dict[str, str]): name of files as key and value representing content
    """    
    with zipfile.ZipFile(zip_path, "w") as zf:
        for name, content in file_dict.items():
            zf.writestr(name, content)

def generate_test_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "int_col": pl.Series([1, 2, 3], dtype=pl.Int64),
            "float_col": pl.Series([0.1, 0.2, 0.3], dtype=pl.Float64),
            "bool_col": pl.Series([True, False, True], dtype=pl.Boolean),
            "str_col": pl.Series(["a", "b", "c"], dtype=pl.Utf8),
            "dt_col": pl.Series(
                [
                    datetime(2026, 1, 1),
                    datetime(2026, 1, 2),
                    datetime(2026, 1, 3),
                ],
                dtype=pl.Datetime("ms"),
            ),
        }
    )

def test_load_valid_zip(tmp_path):
    """
    Test whether zip can be loaded and returns correct DataFrame
    """    
    zip_path = tmp_path / "test.zip"

    create_zip_with_csv(
        zip_path,
        {"trades.csv": "1,100.5,0.25,25.125,1609459200000,true,true\n"}
        )
    
    df = load_bronze_trades_zip(zip_path)

    assert df.height == 1
    assert df.width == 7


def test_zip_with_multiple_csv_raises(tmp_path):
    """
    Test whether zip with multiple files raises MultipleFilesError
    """    
    zip_path = tmp_path / "test.zip"

    create_zip_with_csv(
        zip_path,
        {"trades.csv": "1,100.5,0.25,25.125,1609459200000,true,true\n",
         "trades2.csv": "1,100.5,0.25,25.125,1609459200000,true,true\n",
         "trades3.csv": "1,100.5,0.25,25.125,1609459200000,true,true\n"}
        )
    
    with pytest.raises(MultipleFilesError):
        load_bronze_trades_zip(zip_path)

def test_zip_with_zero_csv_raises(tmp_path):
    """
    Test whether zip with no files raises NoFilesError
    """    
    zip_path = tmp_path / 'test.zip'

    create_zip_with_csv(
        zip_path,
        {}
        )
    
    with pytest.raises(NoFilesError):
        load_bronze_trades_zip(zip_path)

def test_write_parquet_creates_file(tmp_path):
    """
    Test whether writing to parquet works and stays similar to original
    """  
    path = tmp_path / 'test.parquet'
    df = generate_test_df()

    bytes_written = write_silver_trades_parquet(df, path)
    assert path.exists()
    assert bytes_written > 0

    df_read = pl.read_parquet(path)
    assert df_read.height == df.height
    assert df_read.schema == df.schema
