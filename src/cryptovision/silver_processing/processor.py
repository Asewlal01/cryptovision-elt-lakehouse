import datetime
import pathlib
from typing import Callable
from ..utils.paths import build_silver_parquet_path, build_bronze_zip_path
from typing import TypedDict, Optional, Literal, Callable

def utcnow() -> datetime.datetime:
    timezone = datetime.timezone.utc
    return datetime.datetime.now(timezone)

class SilverMetadata(TypedDict):
    status: Literal["written", "skipped", "missing_bronze"]
    symbol: str
    date: str
    bronze_path: str
    silver_path: str
    ingestion_ts: str
    rows: Optional[int]
    output_bytes: Optional[int]
    min_ts: Optional[str]
    max_ts: Optional[str]

class SilverTradesProcessor:
    def __init__(self,
                 bronze_path: pathlib.Path,
                 silver_path: pathlib.Path,
                 overwrite: bool = False,
                 now_fn: Callable[[], datetime.datetime]=utcnow
                 ) -> None:
        """
        Instantiate the silver trades processor

        Args:
            bronze_path (pathlib.Path): Path to bronze data layer
            silver_path (pathlib.Path): Path to silver data layer
            overwrite (bool, optional): Boolean indicating whether to overwrite already existing files. Defaults to False.
            now_fn (_type_, optional): Function to compute the current time. Defaults to lambda:datetime.datetime.now(datetime.timezone.utc).
        """
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.overwrite = overwrite
        self.now_fn = now_fn

    def process(self,
                symbol: str,
                date: datetime.date) -> SilverMetadata:
        """
        Process the file from a given symbol and date

        Args:
            symbol (str): Cryptocurrency pair
            date (datetime.date): Date of interest

        Returns:
            dict: Metadata 
        """
        bronze_file = build_bronze_zip_path(self.bronze_path, symbol, date)         
        silver_file = build_silver_parquet_path(self.silver_path, symbol, date)
        meta: SilverMetadata = {
            'status': 'written',
            'symbol': symbol,
            'date': date.isoformat(),
            'bronze_path': str(bronze_file),
            'silver_path': str(silver_file),
            'ingestion_ts': utcnow().isoformat(),
            'rows': None,
            'output_bytes': None,
            'min_ts': None,
            'max_ts': None
        }

        # File already exists and we do not overwrite
        if self._should_skip(silver_file):
            meta['status'] = 'skipped'
            return meta
        
        if not bronze_file.exists():
            meta['status'] = 'missing_bronze'
            return meta
        
        # TODO: add loading and processing

        return meta
    
    def _should_skip(self,
                     silver_file: pathlib.Path
                     ) -> bool:
        """
        Check whether the current file should be skipped

        Args:
            silver_file (pathlib.Path): Path to silver file

        Returns:
            bool: Boolean indicating whether to skip
        """
        return silver_file.exists() and not self.overwrite
