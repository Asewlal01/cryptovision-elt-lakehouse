from typing import List

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


def test_successful_write():
    """
    Testing whether the client can succesfully download, write and return metadata
    """    
    pass

def test_404_no_found():
    """
    Test whether the client can handle a 404 error (no file found), and make sure that it doesn't writes anything
    """
    pass

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