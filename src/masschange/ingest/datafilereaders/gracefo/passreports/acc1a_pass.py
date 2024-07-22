from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import ReportFileReader, AsciiDataFileReaderColumn


class GraceFOAcc1APassDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ACC1A_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.pass$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return None

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:

        return [
            AsciiDataFileReaderColumn(index=19, name='nrec_read', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=20, name='nrec_read_used', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=21, name='nrec_written', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=22, name='nrec_nulled', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=23, name='nrec_non_incorporated', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=24, name='nrec_filled', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=25, name='nrec_consistency', np_type=int, unit=None)
        ]


