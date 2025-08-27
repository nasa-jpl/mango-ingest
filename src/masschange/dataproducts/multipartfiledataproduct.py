
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class MultipartFileDataProduct(DataProduct):
    '''
    A MultipartFileDataProduct is a data product composed of multiple files, potentially exhibiting
    overlapping time ranges.
    The ingestion strategy for this type of product should differ from standard approaches in its handling of duplicate data.
    Instead of relying on temporal spans for duplicate removal, the strategy should use the source file name to
    identify and eliminate redundant entries.
    '''

    SOURCE_FILE_COLUMN_NAME = 'source_file_name'  # Name of a column that holds source file name

class TimeSeriesMultipartFileDataProduct(TimeSeriesDataProduct, MultipartFileDataProduct):
    '''
    A TimeSeriesMultipartFileDataProduct is a time series data product composed of multiple files, potentially exhibiting
    overlapping time ranges, such as the OPFFRED data product.
    The ingestion strategy for this type of product should differ from standard approaches in its handling of duplicate data.
    Instead of relying on temporal spans for duplicate removal, the strategy should use the source file name to
    identify and eliminate redundant entries.
    '''
    pass