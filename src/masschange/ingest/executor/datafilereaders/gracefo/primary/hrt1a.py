from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


class GraceFOHrt1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^HRT1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time_intg', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='time_frac', np_type=np.uint, unit='microsecond'),
            AsciiDataFileReaderColumn(index=2, name='time_ref',  np_type='U1', unit=None, const_value='R'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=4, name='TEMP_MEP_neg_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=5, name='TEMP_MEP_pos_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=6, name='TEMP_MEPm', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=7, name='TEMP_ICU', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=8, name='TEMP_ICU_red', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=9, name='TEMP_ACC_neg_z', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=10, name='TEMP_ACC_pos_z', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=11, name='TEMP_CFRP_pos_x', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=12, name='TEMP_CFRP_pos_x_red', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=13, name='TEMP_CFRP_neg_x', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=14, name='TEMP_CFRP_neg_x_red', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=15, name='TEMP_CFRP_neg_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=16, name='TEMP_CFRP_neg_y_red', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=17, name='TEMP_ACCSen', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=18, name='TEMP_ICU_spec', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=19, name='TEMP_MWA_neg_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=20, name='TEMP_MWA_neg_yoff', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=21, name='TEMP_MWA_pos_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=22, name='TEMP_MWA_pos_yoff', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=23, name='TEMP_Horn_pos_x', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=24, name='TEMP_Horn_pos_x_red', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=25, name='TEMP_HornPl', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=26, name='TEMP_HornPl_red', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=27, name='TEMP_HMWA_neg_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=28, name='TEMP_HMWA_pos_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=29, name='TEMP_RFSamp', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=30, name='TEMP_USO_neg_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=31, name='TEMP_USO_neg_y_red', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=32, name='TEMP_USO_pos_y', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=33, name='TEMP_USO_pos_y_red', np_type=np.double, unit='degrees C',
                                      aggregations=['min', 'max']),
            AsciiDataFileReaderColumn(index=34, name='qualflg', np_type='U8', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time_intg, microseconds=row.time_frac)
