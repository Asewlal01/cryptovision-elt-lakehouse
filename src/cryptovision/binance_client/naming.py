import datetime

def build_file_name(symbol: str,
                date: datetime.date
                ) -> str:
    """
    Construct the file name that is used by Binance vision

    Args:
        symbol (str): Cryptocurrency pair
        date (datetime): Date of interest

    Returns:
        str: Filename 
    """
    symbol = normalize_symbol(symbol)
    date_str = date.isoformat()
    return f'{symbol}-trades-{date_str}.zip'

def normalize_symbol(symbol: str):
    """
    Normalize the symbol by removing all whitespace and making letters capitalised

    Args:
        symbol (str): Cryptocurrency pair
    """    
    return symbol.strip().capitalize()