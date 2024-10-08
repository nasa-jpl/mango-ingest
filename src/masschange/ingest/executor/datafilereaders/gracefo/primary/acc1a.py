from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


class GraceFOAcc1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ACC1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcvtime_intg', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='rcvtime_frac', np_type=np.uint, unit='microsecond'),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', unit=None, const_value='R'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=4, name='qualflg', np_type='U8', unit=None),
            AsciiDataFileReaderColumn(index=5, name='prod_flag', np_type='U32', unit=None,
                                      const_value='00000100000000000000000000111111'),
            # TODO: prod_flag should be a bit array - need to work out how to convert on load
            AsciiDataFileReaderColumn(index=6, name='lin_accl_x', np_type=np.double, unit='m/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=7, name='lin_accl_y', np_type=np.double, unit='m/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=8, name='lin_accl_z', np_type=np.double, unit='m/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=9, name='ang_accl_x', np_type=np.double, unit='rad/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=10, name='ang_accl_y', np_type=np.double, unit='rad/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=11, name='ang_accl_z', np_type=np.double, unit='rad/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=12, name='icu_blk_nr', np_type=int, unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)
