from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


class GraceFOQcp1BDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^QCP1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='gps_time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='GRACEFO_id', np_type='U1', unit=None),

            AsciiDataFileReaderColumn(index=2, name='sca_id', np_type=int, unit=None,
                                      const_value=0),
            AsciiDataFileReaderColumn(index=3, name='quatangle', np_type=np.double, unit=None,
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=4, name='quaticoeff', np_type=np.double, unit=None,
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=5, name='quatjcoeff', np_type=np.double, unit=None,
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=6, name='quatkcoeff', np_type=np.double, unit=None,
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=7, name='qual_rss', np_type=np.double, unit=None,
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=8, name='qualflg', np_type='U8', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.gps_time)
