from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np
from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn, DerivedAsciiDataFileReaderColumn
from masschange.db.data.aggregations import NestedAggregation
from masschange.ingest.executor.utils.geolocation import Geolocation

class GraceFOGnv1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^GNV1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcv_time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='n_prns', np_type=np.uint, unit=None),
            AsciiDataFileReaderColumn(index=2, name='GRACEFO_id', np_type='U1', unit=None),

            AsciiDataFileReaderColumn(index=3, name='chisq', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=4, name='cov_mult', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=5, name='voltage', np_type=np.double, unit='counts'),

            AsciiDataFileReaderColumn(index=6, name='xpos', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=7, name='ypos', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=8, name='zpos', np_type=np.double, unit='m'),

            AsciiDataFileReaderColumn(index=9, name='xpos_err', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=10, name='ypos_err', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=11, name='zpos_err', np_type=np.double, unit='m'),

            AsciiDataFileReaderColumn(index=12, name='xvel', np_type=np.double, unit='m/s'),
            AsciiDataFileReaderColumn(index=13, name='yvel', np_type=np.double, unit='m/s'),
            AsciiDataFileReaderColumn(index=14, name='zvel', np_type=np.double, unit='m/s'),

            AsciiDataFileReaderColumn(index=15, name='xvel_err', np_type=np.double, unit='m/s'),
            AsciiDataFileReaderColumn(index=16, name='yvel_err', np_type=np.double, unit='m/s'),
            AsciiDataFileReaderColumn(index=17, name='zvel_err', np_type=np.double, unit='m/s'),

            AsciiDataFileReaderColumn(index=18, name='timer_offset', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=19, name='time_offset_err', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=20, name='time_drift', np_type=np.double, unit='s/s'),
            AsciiDataFileReaderColumn(index=21, name='err_drift', np_type=np.double, unit='s/s'),
            AsciiDataFileReaderColumn(index=22, name='qualflg', np_type='U8', unit=None),

            DerivedAsciiDataFileReaderColumn(name='location', np_type='U64', unit=None, aggregations=[
                NestedAggregation('centroid', ['st_collect', 'st_centroid'])]),
            DerivedAsciiDataFileReaderColumn(name='orbit_direction', np_type='U1', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcv_time)

    @classmethod
    def append_derived_fields(cls, df):
        Geolocation.append_location_fields(df)

