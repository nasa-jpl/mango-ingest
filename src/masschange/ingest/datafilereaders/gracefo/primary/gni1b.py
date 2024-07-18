from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np
from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn, DerivedAsciiDataFileReaderColumn

class GraceFOGni1BDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^GNI1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL04\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='gps_time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='GRACEFO_id', np_type='U1', unit=None),

            AsciiDataFileReaderColumn(index=2, name='coord_ref', np_type='U1', unit=None),

            AsciiDataFileReaderColumn(index=3, name='xpos', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=4, name='ypos', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=5, name='zpos', np_type=np.double, unit='m'),

            AsciiDataFileReaderColumn(index=6, name='xpos_err', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=7, name='ypos_err', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=8, name='zpos_err', np_type=np.double, unit='m'),

            AsciiDataFileReaderColumn(index=9, name='xvel', np_type=np.double, unit='m/s'),
            AsciiDataFileReaderColumn(index=10, name='yvel', np_type=np.double, unit='m/s'),
            AsciiDataFileReaderColumn(index=11, name='zvel', np_type=np.double, unit='m/s'),

            AsciiDataFileReaderColumn(index=12, name='xvel_err', np_type=np.double, unit='m/s'),
            AsciiDataFileReaderColumn(index=13, name='yvel_err', np_type=np.double, unit='m/s'),
            AsciiDataFileReaderColumn(index=14, name='zvel_err', np_type=np.double, unit='m/s'),

            AsciiDataFileReaderColumn(index=15, name='qualflg', np_type='U8', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.gps_time)
