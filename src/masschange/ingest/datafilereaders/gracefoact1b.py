from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


class GraceFOAct1BDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ACT1B_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_04\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        legacy_column_defs = [
            {'index': 0, 'label': 'gps_time', 'type': np.ulonglong},
            {'index': 1, 'label': 'GRACEFO_id', 'type': 'U1'},
            {'index': 2, 'label': 'lin_accl_x', 'type': np.double},
            {'index': 3, 'label': 'lin_accl_y', 'type': np.double},
            {'index': 4, 'label': 'lin_accl_z', 'type': np.double},
            {'index': 5, 'label': 'ang_accl_x', 'type': np.double, 'const_value': 0},  # 0 if ACT1B according to the data book
            {'index': 6, 'label': 'ang_accl_y', 'type': np.double, 'const_value': 0},  # 0 if ACT1B according to the data book
            {'index': 7, 'label': 'ang_accl_z', 'type': np.double, 'const_value': 0},  # 0 if ACT1B according to the data book
            {'index': 8, 'label': 'acl_x_res', 'type': np.double},
            {'index': 9, 'label': 'acl_y_res', 'type': np.double},
            {'index': 10, 'label': 'acl_z_res', 'type': np.double},
            {'index': 11, 'label': 'qualflg', 'type': 'U8'}
        ]

        return [AsciiDataFileReaderColumn.from_legacy_definition(col) for col in legacy_column_defs]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.gps_time)
