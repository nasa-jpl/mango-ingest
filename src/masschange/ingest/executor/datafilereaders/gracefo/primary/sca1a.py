from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


# Star Camera Assembly data
# 2-Hz SCA attitude measurements in the form of quaternions expressed in each of the three SCFs
class GraceFOSca1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^SCA1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        # NOTE: THe product has 2 unusual columns:
        # - sca_null1
        # - sca_null1
        # which are not defined in the data book.
        # The values for the columns seems to always be '0'
        # Definitions in the header file:
        # - sca_null1:
        #    comment: 13th column
        #    long_name: Not defined
        #
        # - sca_null2:
        #    comment: 14th column
        #    long_name: Not defined
        #
        # For now, assume that these are constant columns
        return [
            AsciiDataFileReaderColumn(index=0, name='rcvtime_intg', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='rcvtime_frac', np_type=np.uint, unit='microsecond'),
            AsciiDataFileReaderColumn(index=2, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=3, name='sca_id', np_type=np.ubyte, unit=None, is_time_series_id_column=True),
            AsciiDataFileReaderColumn(index=4, name='sca_desig', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=5, name='quatangle', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=6, name='quaticoeff', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=7, name='quatjcoeff', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=8, name='quatkcoeff', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=9, name='nlocks', np_type=np.uint, unit=None),
            AsciiDataFileReaderColumn(index=10, name='nstars', np_type=np.uint, unit=None),
            AsciiDataFileReaderColumn(index=11, name='sca_confid', np_type=np.ubyte, unit=None),
            AsciiDataFileReaderColumn(index=12, name='sca_null1', np_type=int, unit=None, const_value=0),
            AsciiDataFileReaderColumn(index=13, name='sca_null2', np_type=int, unit=None, const_value=0),
            AsciiDataFileReaderColumn(index=14, name='sca_mode', np_type='U8', unit=None),
            AsciiDataFileReaderColumn(index=15, name='qualflg', np_type='U8', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)
