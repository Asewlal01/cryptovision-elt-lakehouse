from typing import TypedDict, Optional, Literal, Callable, Any
import datetime
import pathlib
import requests
import hashlib
import os
from ..utils.paths import build_bronze_zip_path
from .naming import build_file_name

class DownloadMetadata(TypedDict):
    symbol: str
    date: str
    url: str
    path: str
    status: Literal["downloaded", "skipped", "not_found"]
    http_status: Optional[int]
    download_started_ts: datetime.datetime
    bytes_written: int
    sha256: Optional[str]

class NotFoundError(Exception):
    def __init__(self, url: str):
        super().__init__(f'Resource not found: {url}')
        self.url = url

class ZeroByteError(Exception):
    def __init__(self):
        super().__init__('Zero bytes written')

class WritingError(Exception):
    def __init__(self):
        super().__init__('Error occured while writing')        

class BinanceVisionClient:
    BASE_URL = "https://data.binance.vision"
    DATA_PATH = "data/spot/daily/trades"
    def __init__(self,
                 bronze_path: pathlib.Path,
                 http_get: Callable[..., Any] = requests.get
                 ) -> None:
        """
        Initialize the Binance Vision Client

        Args:
            bronze_path (Path): Path to the bronze layer
            http_get (Callable[..., Any]): Function used to perform the HTTP GET request. Defaults to requests.get
        """
        self.bronze_path = bronze_path
        self.http_get = http_get
    
    def download(self,
                 symbol: str,
                 date: datetime.date,
                 ) -> DownloadMetadata: 
        """
        Download data from symbol at a given date

        Args:
            symbol (str): Cryptocurrency pair of interest
            date (datetime): Date of interest

        Returns:
            dict: metadata
        """        
        url = self._build_url(symbol, date)
        save_path = self._build_file_path(symbol, date)

        ts = datetime.datetime.now(datetime.timezone.utc)
        meta: DownloadMetadata = {
            'symbol': symbol,
            'date': date.isoformat(),
            'url': url,
            'path': str(save_path),
            'status': 'skipped',
            'http_status': None,
            'download_started_ts': ts,
            'bytes_written': 0,
            'sha256': None,
        }

        # When file exists and has a size greater than zero
        if save_path.is_file():
            if save_path.stat().st_size > 0:
                return meta
        
        try: 
            bytes_written, sha256 = self._stream_download(url, save_path)
        except NotFoundError:
            meta['status'] = 'not_found'
            meta['http_status'] = 404
            return meta
        
        meta['status'] = 'downloaded'
        meta['http_status'] = 200
        meta['bytes_written'] = bytes_written
        meta['sha256'] = sha256
        return meta
    
    def _build_url(self,
                   symbol: str,
                   date: datetime.date
                   ) -> str:
        """
        Construct url to download data from symbol and date

        Args:
            symbol (str): Cryptocurrency pair of interest
            date (datetime): Date of interest

        Returns:
            str: Url to downloadable data
        """
        file_name = build_file_name(symbol, date)
        return f'{self.BASE_URL}/{self.DATA_PATH}/{symbol}/{file_name}'
    
    def _build_file_path(self,
                    symbol: str,
                    date: datetime.date
                    ) -> pathlib.Path:
        """
        Create and return path to saving location of data

        Args:
            symbol (str): Cryptocurrency pair of interest
            date (datetime): Date of interest

        Returns:
            pathlib.Path: Folder to save
        """        
        save_path = build_bronze_zip_path(self.bronze_path, symbol, date)
        return save_path

    def _stream_download(self,
                         url: str,
                         save_path: pathlib.Path
                         ) -> tuple[int, str]:
        """
        Download file from a given url and save

        Args:
            url (str): link to file
            save_path (pathlib.Path): Path to save file

        Returns:
            tuple[int, str]: tuple with the bytes written and sha256
        """
        temp_path = save_path.with_name(save_path.name + ".tmp")

        with self.http_get(url, stream=True, timeout=(10, 60)) as response:
            if response.status_code == 404:
                raise NotFoundError(url)
            response.raise_for_status()

            save_path.parent.mkdir(parents=True, exist_ok=True)

            # Keeping track of the bytes and hash
            bytes_written = 0
            hasher = hashlib.sha256()

            try: 
                with open(temp_path, 'wb') as f:
                    # 1 MB chunks
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue

                        f.write(chunk)
                        hasher.update(chunk)
                        bytes_written += len(chunk)

                    f.flush()
                    os.fsync(f.fileno())

            # Error when trying to write
            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                raise WritingError() from e

        # Empty file likely implies something went wrong during writing
        if bytes_written == 0:
            if temp_path.exists():
                    temp_path.unlink()
            raise ZeroByteError()

        # Replace correct file name
        temp_path.replace(save_path)
        
        sha256 = hasher.hexdigest()
        return bytes_written, sha256

