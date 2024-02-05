from datetime import datetime
from typing import Sequence, Dict

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader
from masschange.utils.timespan import TimeSpan

# GRACE-FO Level-1A Magnetometer and Torque Rod Data
class GraceFOMag1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^MAG1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_04\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Sequence[Dict]:
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
            {'index': 0, 'label': 'time_intg', 'type': np.ulonglong},
            {'index': 1, 'label': 'time_frac', 'type': np.uint},
            {'index': 2, 'label': 'GRACEFO_id', 'type': 'U1'},
            {'index': 3, 'label': 'time_ref', 'type': 'U1'},
            {'index': 4, 'label': 'MfvX_RAW', 'type': np.double},
            {'index': 5, 'label': 'MfvY_RAW', 'type': np.double},
            {'index': 6, 'label': 'MfvZ_RAW', 'type': np.double},
            {'index': 7, 'label': 'torque1A', 'type': np.double},
            {'index': 8, 'label': 'torque2A', 'type': np.double},
            {'index': 9, 'label': 'torque3A', 'type': np.double},
            {'index': 10, 'label': 'torque1B', 'type': np.double},
            {'index': 11, 'label': 'torque2B', 'type': np.double},
            {'index': 12, 'label': 'torque3B', 'type': np.double},
            {'index': 13, 'label': 'MF_BCalX', 'type': np.double},
            {'index': 14, 'label': 'MF_BCalY', 'type': np.double},
            {'index': 15, 'label': 'MF_BCalZ', 'type': np.double},
            {'index': 16, 'label': 'torque_cal', 'type': np.double},
            {'index': 17, 'label': 'qualflg', 'type': 'U8'},
        ]

    @classmethod
    def get_const_column_expected_values(cls) -> Sequence[Dict]:
        return {
            'time_ref': 'R'
        }

    @classmethod
    def get_time_column_labels(cls) -> Dict:
        return {'time_sec': 'time_intg', 'time_frac': 'time_frac'}