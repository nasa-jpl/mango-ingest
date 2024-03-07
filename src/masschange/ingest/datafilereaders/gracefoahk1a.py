from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import  DataFileWithProdFlagReader, \
    AsciiDataFileReaderColumn, ProdFlagInputColumn, ProdFlagOutputColumn


class GraceFOAhk1ADataFileReader(DataFileWithProdFlagReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^AHK1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_04\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    # Use for reading row data
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcvtime_intg', np_type=np.ulonglong),
            AsciiDataFileReaderColumn(index=1, name='rcvtime_frac', np_type=np.uint),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', const_value='R'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1'),
            AsciiDataFileReaderColumn(index=4, name='qualflg', np_type='U8'),
            AsciiDataFileReaderColumn(index=5, name='prod_flag', np_type='U32'),

            ProdFlagInputColumn(index=6, name='data01', np_type = np.double),
            ProdFlagInputColumn(index=7, name='data02', np_type = np.double),

            # looks like this column is always icu_blk_nr
            ProdFlagInputColumn(index=8, name='icu_blk_nr', np_type=int),
            # looks like this column is always PPS_source
            ProdFlagInputColumn(index=9, name='PPS_source', np_type=int),
            # looks like this column is always Sync_Qual
            ProdFlagInputColumn(index=10, name='Sync_Qual', np_type=int),
            # looks like this column is always statusflag
            ProdFlagInputColumn(index=11, name='statusflag', np_type='U32')
        ]

    @classmethod
    def get_prod_flag_output_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=0, name='TFEEU_IF', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=1, name='TFEEU_REF', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=2, name='TFEEU_X', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=3, name='TFEEU_YZ', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=4, name='analog_GND', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=5, name='plus_3_dot_3V', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=6, name='Vp', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=7, name='MES_Vd', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=8, name='MES_DET_X1', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=9, name='MES_DET_X2', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=10, name='MES_DET_X3', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=11, name='MES_DET_Y1', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=12, name='MES_DET_Y2', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=13, name='MES_DET_Z1', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=14, name='TSU_Y_plus', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=15, name='TICUN', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=16, name='TSU_Y_minus', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=17, name='TSU_Z_plus', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=18, name='TSU_Z_minus', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=19, name='plus_5V', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=20, name='TICUR', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=21, name='plus_15V', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=22, name='minus_15V', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=23, name='plus_48V', np_type = np.double),
            ProdFlagOutputColumn(index = None, prod_flag_bit_index=24, name='minus_48V', np_type = np.double),
            # prod_flag_bit_index = 25 is undefined
            ProdFlagOutputColumn(index=None, prod_flag_bit_index=26, name='icu_blk_nr', np_type=int),
            ProdFlagOutputColumn(index=None, prod_flag_bit_index=27, name='PPS_source', np_type=int),
            ProdFlagOutputColumn(index=None, prod_flag_bit_index=28, name='Sync_Qual', np_type=int),
            ProdFlagOutputColumn(index=None, prod_flag_bit_index=29, name='statusflag', np_type='U32'),
            # prod_flag_bit_index = 30 is undefined
            # prod_flag_bit_index = 31 is undefined
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)
