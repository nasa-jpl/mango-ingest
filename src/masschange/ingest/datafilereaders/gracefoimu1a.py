from collections.abc import Collection
from datetime import datetime, timedelta
from typing import Dict, Any

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


# 8-Hz IMU measurements
class GraceFOImu1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^IMU1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_04\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        legacy_column_defs = [
            {'index': 0, 'label': 'rcvtime_intg', 'type': np.ulonglong},
            {'index': 1, 'label': 'rcvtime_frac', 'type': np.uint},
            {'index': 2, 'label': 'time_ref', 'type': 'U1'},
            {'index': 3, 'label': 'GRACEFO_id', 'type': 'U1'},
            {'index': 4, 'label': 'gyro_id', 'type': np.ubyte}, # valid_range: 1, 4
            {'index': 5, 'label': 'FiltAng', 'type': np.double},
            {'index': 6, 'label': 'qualflg', 'type': 'U8'},
        ]

        return [AsciiDataFileReaderColumn.from_legacy_definition(col) for col in legacy_column_defs]

    @classmethod
    def get_const_column_expected_values(cls) -> Dict[str, Any]:
        return {
            'time_ref': 'R'
        }

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)
