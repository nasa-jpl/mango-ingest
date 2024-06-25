from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


# 8-Hz IMU measurements
class GraceFOImu1BDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^IMU1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcvtime_intg', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='rcvtime_frac', np_type=np.uint, unit='microsecond'),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', unit=None, const_value='G'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=4, name='gyro_id', np_type=np.ubyte, unit=None),  # valid_range: 1, 4
            AsciiDataFileReaderColumn(index=5, name='FiltAng', np_type=np.double, unit='degree',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=6, name='qualflg', np_type='U8', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)
