from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn


class GraceFOResidsDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^RESIDS_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[Y])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'

    """
    From David's e-mail: 
    File format is two columns: [time KBR-GPS_residual(cm)].
    """
    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='kbr_gps_residual', np_type=np.double, unit='cm',
                                      aggregations=['min', 'max'])
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time)

    @classmethod
    def get_header_line_count(cls, filename: str) -> int:
        # No header in the file
        return 0
