from datetime import datetime

from masschange.ingest.executor.datafilereaders.base_offred import OffredFileReader

class GraceFOOffredDataFileReader(OffredFileReader):


    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^(?P<instrument_id>GF[12])_CX_[A-Z0-9]+_[A-Z]{3}_[4D]_\d+_\d{4}_\d{11}_\d{11}.out'


    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return '^(?P<instrument_id>GF[12])_CX_[A-Z]{3}_[4D]_\d{9}_\d{4}.zip'



