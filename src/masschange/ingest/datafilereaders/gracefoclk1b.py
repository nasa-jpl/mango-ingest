from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


class GraceFOClk1BDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^CLK1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcv_time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=2, name='clock_id', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=3, name='eps_time', np_type=np.double, unit='s',
                                      aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=4, name='eps_err', np_type=np.double, unit='s',
                                      aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=5, name='eps_drift', np_type=np.double, unit='s/s',
                                      aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=6, name='drift_err', np_type=np.double, unit='s/s',
                                      aggregations=['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=7, name='qualflg', np_type='U8', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcv_time)
