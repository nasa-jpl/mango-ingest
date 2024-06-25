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
        return '^ACT1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='gps_time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=2, name='lin_accl_x', np_type=np.double, unit=None,
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=3, name='lin_accl_y', np_type=np.double, unit='m/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=4, name='lin_accl_z', np_type=np.double, unit='m/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=5, name='ang_accl_x', np_type=np.double, unit='rad/s2',
                                      const_value=0),
            AsciiDataFileReaderColumn(index=6, name='ang_accl_y', np_type=np.double, unit='rad/s2',
                                      const_value=0),
            AsciiDataFileReaderColumn(index=7, name='ang_accl_z', np_type=np.double, unit='rad/s2',
                                      const_value=0),
            AsciiDataFileReaderColumn(index=8, name='acl_x_res', np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=9, name='acl_y_res', np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=10, name='acl_z_res', np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=11, name='qualflg', np_type='U8', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.gps_time)
