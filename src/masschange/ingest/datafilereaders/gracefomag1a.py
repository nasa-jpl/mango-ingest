from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


# GRACE-FO Level-1A Magnetometer and Torque Rod Data
class GraceFOMag1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^MAG1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        # Note: the header is incorrect for MAG1A data file.
        # It says 'time_ref' is column 3, 'GRACEFO_id' is culmmn 4,
        # but in the file body 'time_ref' is column 4, 'GRACEFO_id' is column 3
        #  - time_ref:
        #         comment: 3rd column
        #         value_meanings:
        #           - R = Receiver, OBC, or LRI time
        #           - G = GPS time
        #     - GRACEFO_id:
        #         comment: 4th column
        #         valid_range: C,D
        #
        return [
            AsciiDataFileReaderColumn(index=0, name='time_intg', np_type=np.ulonglong),
            AsciiDataFileReaderColumn(index=1, name='time_frac', np_type=np.uint),
            AsciiDataFileReaderColumn(index=2, name='GRACEFO_id', np_type='U1'),
            AsciiDataFileReaderColumn(index=3, name='time_ref', np_type='U1', const_value='R'),
            AsciiDataFileReaderColumn(index=4, name='MfvX_RAW', np_type=np.double),
            AsciiDataFileReaderColumn(index=5, name='MfvY_RAW', np_type=np.double),
            AsciiDataFileReaderColumn(index=6, name='MfvZ_RAW', np_type=np.double),
            AsciiDataFileReaderColumn(index=7, name='torque1A', np_type=np.double),
            AsciiDataFileReaderColumn(index=8, name='torque2A', np_type=np.double),
            AsciiDataFileReaderColumn(index=9, name='torque3A', np_type=np.double),
            AsciiDataFileReaderColumn(index=10, name='torque1B', np_type=np.double),
            AsciiDataFileReaderColumn(index=11, name='torque2B', np_type=np.double),
            AsciiDataFileReaderColumn(index=12, name='torque3B', np_type=np.double),
            AsciiDataFileReaderColumn(index=13, name='MF_BCalX', np_type=np.double),
            AsciiDataFileReaderColumn(index=14, name='MF_BCalY', np_type=np.double),
            AsciiDataFileReaderColumn(index=15, name='MF_BCalZ', np_type=np.double),
            AsciiDataFileReaderColumn(index=16, name='torque_cal', np_type=np.double),
            AsciiDataFileReaderColumn(index=17, name='qualflg', np_type='U8'),
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time_intg, microseconds=row.time_frac)
