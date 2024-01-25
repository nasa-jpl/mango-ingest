from datetime import datetime
from typing import Sequence, Dict, Any

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader
from masschange.utils.timespan import TimeSpan


class GraceFOAcc1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ACC1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_04\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Sequence[Dict]:
        return [
            {'index': 0, 'label': 'rcvtime_intg', 'type': np.ulonglong},
            {'index': 1, 'label': 'rcvtime_frac', 'type': np.uint},
            {'index': 2, 'label': 'time_ref', 'type': 'U1'},
            {'index': 3, 'label': 'GRACEFO_id', 'type': 'U1'},
            {'index': 4, 'label': 'qualflg', 'type': 'U8'},
            {'index': 5, 'label': 'prod_flag', 'type': 'U32'},  # TODO: this should be a bit array - need to work out how to convert on load
            {'index': 6, 'label': 'lin_accl_x', 'type': np.double},
            {'index': 7, 'label': 'lin_accl_y', 'type': np.double},
            {'index': 8, 'label': 'lin_accl_z', 'type': np.double},
            {'index': 9, 'label': 'ang_accl_x', 'type': np.double},
            {'index': 10, 'label': 'ang_accl_y', 'type': np.double},
            {'index': 11, 'label': 'ang_accl_z', 'type': np.double},
            {'index': 12, 'label': 'icu_blk_nr', 'type': int}
        ]

    @classmethod
    def get_const_column_expected_values(cls) -> Dict[str, Any]:
        return {
            'time_ref': 'R',
            'prod_flag': '00000100000000000000000000111111'
        }