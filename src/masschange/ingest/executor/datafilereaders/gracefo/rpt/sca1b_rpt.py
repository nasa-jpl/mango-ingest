from collections.abc import Collection
from datetime import datetime

import numpy as np

from masschange.ingest.executor.datafilereaders.base import ReportFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn


class GraceFOSca1BRptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^SCA1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [

            AsciiDataFileReaderColumn(index=19, name='nr_obs_sc1', np_type=np.double, unit=None),                                 
            AsciiDataFileReaderColumn(index=20, name='nr_obs_sc2', np_type=np.double, unit=None),                                  
            AsciiDataFileReaderColumn(index=21, name='nr_obs_sc3',  np_type=np.double, unit=None),                                  
            AsciiDataFileReaderColumn(index=22, name='nr_obs_imu',  np_type=np.double, unit=None),                                   
            AsciiDataFileReaderColumn(index=23, name='nr_obs_acc',  np_type=np.double, unit=None),  
            
            AsciiDataFileReaderColumn(index=24, name='rms_postfit_sc1_x',  np_type=np.double, unit='microrad'),
            AsciiDataFileReaderColumn(index=25, name='rms_postfit_sc1_y',  np_type=np.double, unit='microrad'),
            AsciiDataFileReaderColumn(index=26, name='rms_postfit_sc1_z',  np_type=np.double, unit='microrad'),
            AsciiDataFileReaderColumn(index=27, name='rms_postfit_sc2_x', np_type=np.double, unit='microrad'),
            AsciiDataFileReaderColumn(index=28, name='rms_postfit_sc2_y',  np_type=np.double, unit='microrad'),
            AsciiDataFileReaderColumn(index=29, name='rms_postfit_sc2_z',  np_type=np.double, unit='microrad'),
            AsciiDataFileReaderColumn(index=30, name='rms_postfit_sc3_x',  np_type=np.double, unit='microrad'),
            AsciiDataFileReaderColumn(index=31, name='rms_postfit_sc3_y',  np_type=np.double, unit='microrad'),
            AsciiDataFileReaderColumn(index=32, name='rms_postfit_sc3_z',  np_type=np.double, unit='microrad'),

            AsciiDataFileReaderColumn(index=33, name='rms_postfit_imu_x', np_type=np.double, unit='microrad/s'),
            AsciiDataFileReaderColumn(index=34, name='rms_postfit_imu_y', np_type=np.double, unit='microrad/s'),
            AsciiDataFileReaderColumn(index=35, name='rms_postfit_imu_z', np_type=np.double, unit='microrad/s'),

            AsciiDataFileReaderColumn(index=36, name='rms_postfit_acc_x', np_type=np.double, unit='microrad/s2'),
            AsciiDataFileReaderColumn(index=37, name='rms_postfit_acc_y', np_type=np.double, unit='microrad/s2'),
            AsciiDataFileReaderColumn(index=38, name='rms_postfit_acc_z', np_type=np.double, unit='microrad/s2')
        ]

