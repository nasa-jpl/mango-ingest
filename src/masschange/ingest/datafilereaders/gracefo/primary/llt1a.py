from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn

class GraceFOLlt1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^LLT1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[Y])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='gps_time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='rcv_id', np_type='U1', unit=None, is_time_series_id_column=True),
            AsciiDataFileReaderColumn(index=2, name='trx_id', np_type='U1', unit=None, is_time_series_id_column=True),

            AsciiDataFileReaderColumn(index=3, name='tau', np_type=np.double, unit='s',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=4, name='xpos', np_type=np.double, unit='m',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=5, name='ypos', np_type=np.double, unit='m',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=6, name='zpos', np_type=np.double, unit='m',
                                      aggregations=['min', 'max']),

            AsciiDataFileReaderColumn(index=7, name='xvel', np_type=np.double, unit='m/s',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=8, name='yvel', np_type=np.double, unit='m/s',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=9, name='zvel', np_type=np.double, unit='m/s',
                                      aggregations=['min', 'max']),

            AsciiDataFileReaderColumn(index=10, name='qualflg', np_type='U8', unit=None),
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.gps_time)

