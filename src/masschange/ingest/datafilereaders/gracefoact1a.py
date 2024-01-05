from datetime import datetime
from typing import Sequence, Dict

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader
from masschange.utils.timespan import TimeSpan


class GraceFOAct1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def parse_data_span(cls, filepath: str) -> TimeSpan:
        # TODO: implement
        raise NotImplementedError()

    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ACT1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_04\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    def get_desired_column_defs(cls) -> Sequence[Dict]:
        float_value_dtype = np.double
        return [
            {'index': 0, 'label': 'rcvtime_intg', 'type': np.ulonglong},
            {'index': 1, 'label': 'rcvtime_frac', 'type': np.uint},
            {'index': 6, 'label': 'lin_accl_x', 'type': float_value_dtype},
            {'index': 7, 'label': 'lin_accl_y', 'type': float_value_dtype},
            {'index': 8, 'label': 'lin_accl_z', 'type': float_value_dtype},
            {'index': 9, 'label': 'ang_accl_x', 'type': float_value_dtype},
            {'index': 10, 'label': 'ang_accl_y', 'type': float_value_dtype},
            {'index': 11, 'label': 'ang_accl_z', 'type': float_value_dtype}
        ]
