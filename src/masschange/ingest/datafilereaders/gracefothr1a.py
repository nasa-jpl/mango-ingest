from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn


class GraceFOThr1ADataFileReader(AsciiDataFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^THR1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time_intg', np_type=np.ulonglong),
            AsciiDataFileReaderColumn(index=1, name='time_frac', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', const_value='R'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1'),
            # thust_count_* increases with time, resets after 4294967295
            AsciiDataFileReaderColumn(index=4, name='thrust_count_att_ctrl_1_1', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=5, name='thrust_count_att_ctrl_1_2', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=6, name='thrust_count_att_ctrl_1_3', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=7, name='thrust_count_att_ctrl_1_4', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=8, name='thrust_count_att_ctrl_1_5', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=9, name='thrust_count_att_ctrl_1_6', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=10, name='thrust_count_att_ctrl_2_1', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=11, name='thrust_count_att_ctrl_2_2', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=12, name='thrust_count_att_ctrl_2_3', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=13, name='thrust_count_att_ctrl_2_4', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=14, name='thrust_count_att_ctrl_2_5', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=15, name='thrust_count_att_ctrl_2_6', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=16, name='thrust_count_undef_1', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=17, name='thrust_count_undef_2', np_type = np.uint, const_value = 0),

            AsciiDataFileReaderColumn(index=18, name='on_time_att_ctrl_1_1', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=19, name='on_time_att_ctrl_1_2', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=20, name='on_time_att_ctrl_1_3', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=21, name='on_time_att_ctrl_1_4', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=22, name='on_time_att_ctrl_1_5', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=23, name='on_time_att_ctrl_1_6', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=24, name='on_time_att_ctrl_2_1', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=25, name='on_time_att_ctrl_2_2', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=26, name='on_time_att_ctrl_2_3', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=27, name='on_time_att_ctrl_2_4', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=28, name='on_time_att_ctrl_2_5', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=29, name='on_time_att_ctrl_2_6', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=30, name='on_time_orb_ctrl_1', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            AsciiDataFileReaderColumn(index=31, name='on_time_orb_ctrl_2', np_type = np.uint, aggregations = ['MIN', 'MAX']),
            # accum_dur_* increases with time, resets after 4294967295
            AsciiDataFileReaderColumn(index=32, name='accum_dur_att_ctrl', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=33, name='accum_dur_undef_1', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=34, name='accum_dur_undef_2', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=35, name='accum_dur_undef_3', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=36, name='accum_dur_undef_4', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=37, name='accum_dur_undef_5', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=38, name='accum_dur_undef_6', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=39, name='accum_dur_undef_7', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=40, name='accum_dur_undef_8', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=41, name='accum_dur_undef_9', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=42, name='accum_dur_undef_10', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=43, name='accum_dur_undef_11', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=44, name='accum_dur_orb_ctrl', np_type = np.uint, aggregations = ['AVG']),
            AsciiDataFileReaderColumn(index=45, name='accum_dur_undef_12', np_type = np.uint, const_value = 0),
            AsciiDataFileReaderColumn(index=46, name='qualflg', np_type='U8')
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time_intg, microseconds=row.time_frac)
