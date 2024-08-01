from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn

class GraceFOLhk1BDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^LHK1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:

        return [
            AsciiDataFileReaderColumn(index=0, name='time_intg', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='time_frac', np_type=np.uint, unit='ns'),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', unit=None, const_value='G'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=4, name='qualflg', np_type='U8', unit=None),
            AsciiDataFileReaderColumn(index=5, name='sensortype', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=6, name='sensorvalue', np_type=np.ulonglong, unit=None),
            AsciiDataFileReaderColumn(index=7, name='sensorname', np_type='U1000', unit=None, is_time_series_id_column=True)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        # TODO: Pandas has timedelta that supports nanoseconds, but
        # Postgres does not supports nanoseconds timestamp, so the timestamps will have microseconds precision
        return cls.get_reference_epoch() + timedelta(seconds=row.time_intg, microseconds=row.time_frac/1000)

