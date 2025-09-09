from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn


class GraceFOClkDdDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^Clk_DD_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[Y])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'

    """
    From David's e-mail: 
    You will need to extract:
    Col 1: time tag
    Col 9: Difference in clock solution for GRACE-C from previous day and current day in the overlap period. (units: picoseconds)
    Col 10: Difference in clock solution for GRACE-D from previous day and current day in the overlap period. (units: picoseconds)
    Col 11: Double Difference (Column 9 – Column 10). (units: picoseconds).
    Note: the time period will be 22:00 prev day to 2:00 current day in the existing clk_dd.txt files.
    
    Also note that the statistics displayed in the performance table come from Column 11, however, Column 9 and 10 are 
    useful for debugging purposes as they give an extra layer of information.

    """
    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=8, name='clc_diff_c', np_type=np.double, unit='ps'),
            AsciiDataFileReaderColumn(index=9, name='clc_diff_d', np_type=np.double, unit='ps'),
            AsciiDataFileReaderColumn(index=10, name='clc_dd', np_type=np.double, unit='ps')
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time)

    @classmethod
    def get_header_line_count(cls, filename: str) -> int:
        # No header in the file
        return 0
