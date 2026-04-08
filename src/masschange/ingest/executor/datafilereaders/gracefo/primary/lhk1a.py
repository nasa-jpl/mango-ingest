from collections.abc import Collection
from datetime import datetime, timedelta

from typing import Union

import numpy as np
import pandas as pd

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn, \
    ArrayLikeAsciiDataFileReaderColumn, DerivedAsciiDataFileReaderColumn
from masschange.ingest.executor.datafilereaders.filter import DataFilter
from masschange.ingest.utils.populate_dynamic_unit import populate_dynamic_unit


class GraceFOLhk1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^LHK1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:

        return [
            AsciiDataFileReaderColumn(index=0, name='time_intg', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='time_frac', np_type=np.uint, unit='ns'),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1', unit=None),
            ArrayLikeAsciiDataFileReaderColumn(index=4, name='qualflg', np_type='U8', array_size=8),
            AsciiDataFileReaderColumn(index=5, name='sensortype', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=6, name='sensorvalue', np_type=np.ulonglong, unit=None),
            AsciiDataFileReaderColumn(index=7, name='sensorname', np_type='U1000', unit=None, is_channel_id_column=True),
            DerivedAsciiDataFileReaderColumn(name='unit', np_type='U4', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        # TODO: Pandas has timedelta that supports nanoseconds, but
        # Postgres does not supports nanoseconds timestamp, so the timestamps will have microseconds precision
        return cls.get_reference_epoch() + timedelta(seconds=row.time_intg, microseconds=row.time_frac/1000)

    @classmethod
    def load_data_from_file(cls, filepath: str, filters: Union[list[DataFilter], None] = None) -> pd.DataFrame:
        # Overwrite the parent's method to add 'unit' column
        df = super().load_data_from_file(filepath, filters=filters)

        # insert unit column after "sensorname" column
        df.insert(loc=df.columns.get_loc("sensorname") + 1, column="unit",
                  value=df.apply(populate_dynamic_unit, axis=1))
        return df