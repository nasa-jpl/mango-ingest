from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn

class GraceFOLri1BDataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^LRI1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[Y])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.LRI\.tgz'

    @classmethod
    # Use for reading row data
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='gps_time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='biased_range', np_type=np.double, unit='m',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=2, name='range_rate', np_type=np.double, unit='m/s',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=3, name='range_accl', np_type=np.double, unit='m/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=4, name='iono_corr', np_type=np.double, unit=None,
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=5, name='lighttime_corr', np_type=np.double, unit = 'm',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=6, name='lighttime_rate', np_type=np.double, unit='m/s',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=7, name='lighttime_accl', np_type=np.double, unit='m/s2',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=8, name='ant_centr_corr', np_type=np.double, unit='m',
                                      const_value=0.0),
            AsciiDataFileReaderColumn(index=9, name='ant_centr_rate', np_type=np.double, unit='m/s',
                                      const_value=0.0),
            AsciiDataFileReaderColumn(index=10, name='ant_centr_accl', np_type=np.double, unit='m/s2',
                                      const_value=0.0),
            AsciiDataFileReaderColumn(index=11, name='K_A_SNR', np_type=np.double, unit='db-Hz',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=12, name='Ka_A_SNR', np_type=np.double, unit='db-Hz',
                                      const_value=0.0),
            AsciiDataFileReaderColumn(index=13, name='K_B_SNR', np_type=np.double, unit='db-Hz',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=14, name='Ka_B_SNR', np_type=np.double, unit='db-Hz',
                                      const_value=0.0),
            AsciiDataFileReaderColumn(index=15, name='qualflg', np_type='U8', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.gps_time)

