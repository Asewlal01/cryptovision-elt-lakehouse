import datetime
import pathlib
from typing import Callable

def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

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
                date: datetime.date) -> dict:
        """
        Process the file from a given symbol and date

        Args:
            symbol (str): Cryptocurrency pair
            date (datetime.date): Date of interest

        Returns:
            dict: Metadata 
        """        

        raise NotImplementedError