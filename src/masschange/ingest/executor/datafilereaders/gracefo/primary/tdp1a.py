from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from masschange.ingest.executor.datafilereaders.base import  AsciiDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn



class GraceFOTdp1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^TDP1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.dat$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'

    '''
    Data file format:
        1. time continuous seconds past Jan. 1, 2000 11:59:47 UTC. GCORE standard for time
        2. nominal_value a nominal value for the parameter
        3. value value at the given time
        4. sigma standard deviation for the parameter
        5. name An arbitrary sequence of letters [A-Z,a-z], digits[0-9], and "." without spaces.
    Example:
    375958800.0000  0.00000000000000  -110937.208992753  0.0368   .Satellite.GPS23.Clk.Bias  
    '''

    #TODO: units depend on value of 'name'. Need to figure out how to assign units

    @classmethod
    # Use for reading row data
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='nominal_value', np_type=np.double, unit='None'),
            AsciiDataFileReaderColumn(index=2, name='value', np_type=np.double, unit='None',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=3, name='sigma', np_type=np.double, unit='None',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=4, name='name', np_type='U512', unit=None, is_channel_id_column=True)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time)

    @classmethod
    def get_reference_epoch(cls) -> datetime:
        # TODO: for other data, epoch is datetime(2000, 1, 1, 12). Make sure that this data has different epoch
        return datetime(2000, 1, 1, 11, 59, 47)

    @classmethod
    def get_header_line_count(cls, filename: str) -> int:
        # No header in the file
        return 0