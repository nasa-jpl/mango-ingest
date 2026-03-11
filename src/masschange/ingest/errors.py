class ZeroByteFileError(ValueError):
    """
    Raised when a zero-byte file is encountered.  This is distinct from the valid case where a primary product file with
    zero data rows (but with the correct header) is encountered.
    """