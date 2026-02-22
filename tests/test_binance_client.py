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