from datetime import datetime, timedelta
from typing import Sequence, Dict, Any

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader

# 0.1-Hz GPS-derived onboard navigation measurements
class GraceFOPci1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^PCI1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_04\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Sequence[Dict]:
        return [
            {'index': 0, 'label': 'gps_time', 'type': np.ulonglong}, # Seconds past 12:00:00 noon of January 1, 2000 in GPS Time
            {'index': 1, 'label': 'GRACEFO_id', 'type': 'U1'},
            {'index': 2, 'label': 'ant_centr_corr', 'type': np.double},
            {'index': 3, 'label': 'ant_centr_rate', 'type': np.double},
            {'index': 4, 'label': 'ant_centr_accl', 'type': np.double},
            {'index': 5, 'label': 'qualflg', 'type': 'U8'}
        ]

    @classmethod
    def get_const_column_expected_values(cls) -> Dict[str, Any]:
        return {}

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.gps_time)