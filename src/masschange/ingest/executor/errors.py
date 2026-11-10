class EmptyProductException(Exception):
    """Exception to throw when a data file does not have any data"""
    pass
class TimestampOutliersException(Exception):
    """Exception to throw when some timestamps in the files are outside the expected range"""
    pass