from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import VariableDataClustersPerRowReader, AsciiDataFileReaderColumn, DerivedAsciiDataFileReaderColumn


class GraceFOGnv1APrnDataFileReader(VariableDataClustersPerRowReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^GNV1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:

        return [
            AsciiDataFileReaderColumn(index=0, name='rcv_time', np_type=np.ulonglong),
            AsciiDataFileReaderColumn(index=1, name='n_prns', np_type=np.uint),
            AsciiDataFileReaderColumn(index=2, name='GRACEFO_id', np_type='U1'),
            DerivedAsciiDataFileReaderColumn(name='prn_id', np_type=np.uint),
            DerivedAsciiDataFileReaderColumn(name='el_prn', np_type=np.double),
            DerivedAsciiDataFileReaderColumn(name='az_prn', np_type=np.double)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcv_time)

    @classmethod
    def _get_first_cluster_column_position(cls) -> int:
        return 23

    @classmethod
    def _get_clusters_counter_col_name(cls) -> int:
        return 'n_prns'

    @classmethod

    def _get_num_variables_in_cluster(cls) -> int:
        """
        Return number of values in data cluster
        """
        return 3