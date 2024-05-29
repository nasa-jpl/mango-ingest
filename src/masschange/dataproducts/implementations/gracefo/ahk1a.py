from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoahk1a import GraceFOAhk1ADataFileReader
from masschange.dataproducts.implementations.gracefo.ahk_base import GraceFOAhkBaseDataProduct


class GraceFOAhk1ADataProduct(GraceFOAhkBaseDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAhk1ADataFileReader()

    id_suffix = 'AHK1A'
    processing_level = '1A'

