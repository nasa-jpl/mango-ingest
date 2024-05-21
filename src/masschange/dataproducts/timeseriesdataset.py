from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion


class TimeSeriesDataset:
    product: TimeSeriesDataProduct
    version: TimeSeriesDatasetVersion
    stream_id: str

    def __init__(self, product: TimeSeriesDataProduct, version: TimeSeriesDatasetVersion, stream_id: str):
        self.product = product
        self.version = version
        self.stream_id = stream_id


    # TODO: extract all methods of TimeSeriesDataProduct which have version/stream in signature, and move them here