from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


class GraceFOAct1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ACT1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcvtime_intg', np_type=np.ulonglong),
            AsciiDataFileReaderColumn(index=1, name='rcvtime_frac', np_type=np.uint),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', const_value='R'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1'),
            AsciiDataFileReaderColumn(index=4, name='qualflg', np_type='U8'),
            AsciiDataFileReaderColumn(index=5, name='prod_flag', np_type='U32',
                                      const_value='00000100000000000000000000111111'),
            # TODO: prod_flag should be a bit array - need to work out how to convert on load
            AsciiDataFileReaderColumn(index=6, name='lin_accl_x', np_type=np.double, aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=7, name='lin_accl_y', np_type=np.double, aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=8, name='lin_accl_z', np_type=np.double, aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=9, name='ang_accl_x', np_type=np.double, aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=10, name='ang_accl_y', np_type=np.double, aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=11, name='ang_accl_z', np_type=np.double, aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=12, name='icu_blk_nr', np_type=int)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)
