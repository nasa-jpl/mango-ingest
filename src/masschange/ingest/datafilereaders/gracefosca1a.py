from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


# Star Camera Assembly data
# 2-Hz SCA attitude measurements in the form of quaternions expressed in each of the three SCFs
class GraceFOSca1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^SCA1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_04\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        #NOTE: THe product has 2 unusual columns:
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

        legacy_column_defs = [
            {'index': 0, 'label': 'rcvtime_intg', 'type': np.ulonglong},
            {'index': 1, 'label': 'rcvtime_frac', 'type': np.uint},
            {'index': 2, 'label': 'GRACEFO_id', 'type': 'U1'},
            {'index': 3, 'label': 'sca_id', 'type': np.ubyte},  # valid_range: 1, 3
            {'index': 4, 'label': 'sca_desig', 'type': 'U1'},
            {'index': 5, 'label': 'quatangle', 'type': np.double},
            {'index': 6, 'label': 'quaticoeff', 'type': np.double},
            {'index': 7, 'label': 'quatjcoeff', 'type': np.double},
            {'index': 8, 'label': 'quatkcoeff', 'type': np.double},
            {'index': 9, 'label': 'nlocks', 'type': np.uint},
            {'index': 10, 'label': 'nstars', 'type': np.uint},
            {'index': 11, 'label': 'sca_confid', 'type': np.ubyte}, # valid_range: 0, 255
            {'index': 12, 'label': 'sca_null1', 'type': int, 'const_value': 0},
            {'index': 13, 'label': 'sca_null2', 'type': int, 'const_value': 0},
            {'index': 14, 'label': 'sca_mode', 'type': 'U8'},
            {'index': 15, 'label': 'qualflg', 'type': 'U8'}
        ]

        return [AsciiDataFileReaderColumn.from_legacy_definition(col) for col in legacy_column_defs]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)