from cryptovision.silver_processing.io import *
import zipfile
from pathlib import Path

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

