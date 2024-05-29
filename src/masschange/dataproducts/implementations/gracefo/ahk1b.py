from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoahk1b import GraceFOAhk1BDataFileReader
from masschange.dataproducts.implementations.gracefo.ahk_base import GraceFOAhkBaseDataProduct


class GraceFOAhk1BDataProduct(GraceFOAhkBaseDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAhk1BDataFileReader()

    id_suffix = 'AHK1B'
    processing_level = '1B'

