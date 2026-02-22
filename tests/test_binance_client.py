from typing import Callable
from cryptovision import BinanceVisionClient
from cryptovision.binance_client.client import NotFoundError
import datetime
import hashlib
import pathlib
import pytest

class FakeReponse:
    def __init__(self, 
                status_code: int,
                content: bytes,
                raise_midstream: bool
                ) -> None:
        """
        Instantiate the fake response

        Args:
            status_code (int): Status code of the response
            content (bytes): Content of codes encoded as bytes
            raise_midstream (bool): Boolean indicating whether to raise an exception midstream
        """        
        self.status_code = status_code
        self.content = content
        self.raise_midstream = raise_midstream

    def __enter__(self):
        return self
    
    def __exit__(self, *_):
        return None
    
    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception("HTTP error")
        
    def iter_content(self, chunk_size):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i:i+chunk_size]
            if self.raise_midstream:
                raise Exception("Midstream error")

def make_fake_http_get(status_code: int,
                       content: bytes,
                       raise_midstream: bool
                       ) -> Callable:
    """
    Generate a function that acts like an HTTP get request but with a fake response

        Args:
            status_code (int): Status code of the response
            content (bytes): Content of codes encoded as bytes
            raise_midstream (bool): Boolean indicating whether to raise an exception midstream

    Returns:
        Callable: Fake HTTP get request function
    """    
    def fake_http_get(*args, **kwargs):
        return FakeReponse(status_code, content, raise_midstream)
    return fake_http_get

def test_successful_write(tmp_path):
    """
    Test whether the client can succesfully download, write and return metadata
    """
    status_code = 200
    content = b'this is a streaming test'
    raise_midstream = False
    http_get = make_fake_http_get(status_code, content, raise_midstream)
    client = BinanceVisionClient(tmp_path, http_get)
    
    symbol = 'TEST'
    date = datetime.date(2026, 1, 1)
    meta = client.download(symbol, date)

    # Metadata checking
    assert meta['status'] == 'downloaded'
    assert meta['http_status'] == 200
    assert meta['bytes_written'] == len(content)
    assert meta['sha256'] == hashlib.sha256(content).hexdigest()

    # Final file checking
    file_path = pathlib.Path(meta['path'])
    assert file_path.exists()
    assert file_path.read_bytes() == content

    # Check for no temp file
    temp_path = file_path.with_name(file_path.name + '.tmp')
    assert not temp_path.exists()
    
def test_404_no_found(tmp_path):
    """
    Test whether the client can handle a 404 error (no file found), and make sure that it doesn't writes anything
    """
    status_code = 404
    content = b''
    raise_midstream = False
    http_get = make_fake_http_get(status_code, content, raise_midstream)
    client = BinanceVisionClient(tmp_path, http_get)

    symbol = 'TEST'
    date = datetime.date(2026, 1, 1)
    meta = client.download(symbol, date)

    # Metadata checking
    assert meta['status'] == 'not_found'
    assert meta["http_status"] == 404
    assert meta['bytes_written'] == 0
    
    # Check whether there are no files
    file_path = pathlib.Path(meta['path'])
    temp_path = file_path.with_name(file_path.name + '.tmp')
    assert not file_path.exists()
    assert not temp_path.exists()

    # Check whether directory does not exist
    assert not file_path.parent.exists()
        

def test_zero_write_response():
    """
    Test whether the client can handle a response with no content (zero-bytes written) and succesfully cleans up any leftover files
    """
    pass

def test_midstream_failure():
    """
    Test whether the client can handle midstream failures and clean leftover files
    """    
    pass

def test_skip_if_alread_exists():
    """
    Test whether the client can handle skipping files when it already exists
    """    
    pass