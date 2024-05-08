from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from masschange.ingest.datafilereaders.base import  DataFileWithProdFlagReader, \
    AsciiDataFileReaderColumn, VariableSchemaAsciiDataFileReaderColumn


class GraceFOKbr1ADataFileReader(DataFileWithProdFlagReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^KBR1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    # Use for reading row data
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcvtime_intg', np_type=np.ulonglong, unit='implement_me'),
            AsciiDataFileReaderColumn(index=1, name='rcvtime_frac', np_type=np.uint, unit='implement_me'),
            AsciiDataFileReaderColumn(index=2, name='GRACEFO_id', np_type='U1', unit='implement_me'),
            AsciiDataFileReaderColumn(index=3, name='prn_id', np_type=int, unit='implement_me'),
            AsciiDataFileReaderColumn(index=4, name='ant_id', np_type=int, unit='implement_me'),
            AsciiDataFileReaderColumn(index=5, name='prod_flag', np_type='U16', unit='implement_me'),
            AsciiDataFileReaderColumn(index=6, name='qualflg', np_type='U8', unit='implement_me'),

            # skip definitions of columns defined by 'prod_flag'
            # add definitions for VariableSchemaAsciiDataFileReaderColumns
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=0, name='CA_range', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=1, name='L1_range', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=2, name='L2_range', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),

            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=3, name='CA_phase', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=4, name='L1_phase', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=5, name='L2_phase', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),

            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=6, name='CA_SNR', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=7, name='L1_SNR', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=8, name='L2_SNR', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),

            # TODO: assume that 'channel' is an integer number. Need to confirm
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=9, name='CA_chan', np_type=pd.Int32Dtype,
                                                    unit='implement_me'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=10, name='L1_chan', np_type=pd.Int32Dtype,
                                                    unit='implement_me'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=11, name='L2_chan', np_type=pd.Int32Dtype,
                                                    unit='implement_me'),

            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=12, name='K_phase', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=13, name='Ka_phase', np_type=np.double,
                                                    unit='implement_me', aggregations=['MIN', 'MAX']),

            # Note: 'NaN' is not defined for int. Panda has a nullable int types pd.Int64Dtype
            # See:
            # https://pandas.pydata.org/pandas-docs/version/1.1/user_guide/integer_na.html#:~:text=nan%20.,values%20to%20become%20floating%20point.

            # TODO: it is not clear from the description if these are double or int,
            # but in the sample file they are represented as int
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=14, name='K_SNR', np_type=pd.Int32Dtype,
                                                    unit='implement_me'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=15, name='Ka_SNR', np_type=pd.Int32Dtype,
                                                    unit='implement_me')
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)

    @classmethod
    def _get_first_prod_flag_data_column_position(cls) -> int:
        return 7