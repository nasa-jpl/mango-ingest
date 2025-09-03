from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn


class GraceFOAcc1bAngXDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ACC1B-angX_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'


    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='x', np_type=np.double, unit='micro-radians/s^2')

        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time)

    @classmethod
    def get_header_line_count(cls, filename: str) -> int:
        # No header in the file
        return 0
