from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


class GraceFOIhk1BDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^IHK1B_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

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
            AsciiDataFileReaderColumn(index=4, name='qualflg', np_type='U8', unit=None),
            AsciiDataFileReaderColumn(index=5, name='sensortype', np_type='U1', unit=None),
            # TODO: unit for sensorvalue depends on sensor type, could be value for voltage, temperature or current
            AsciiDataFileReaderColumn(index=6, name='sensorvalue', np_type=np.double, unit='implement_me'),
            AsciiDataFileReaderColumn(index=7, name='sensorname', np_type='U2', unit=None),
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)
