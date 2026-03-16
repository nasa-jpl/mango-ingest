from collections.abc import Collection
from datetime import datetime, timedelta

import os
import re
import numpy as np
import pandas as pd
from typing import Union

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import (AsciiDataFileReaderColumn,
                                                                     DerivedAsciiDataFileReaderColumn)
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.ingest.executor.datafilereaders.filter import DataFilter


class GraceFODdicDataFileReader(AsciiDataFileReader):
    """
    KBR Baseband Frequency
    """

    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^DDIC_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[Y])_(?P<subset_version>\d{3})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'


    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='ddic', np_type=np.double, unit='cycles',
                                      aggregations=['min', 'max']),
            DerivedAsciiDataFileReaderColumn(name='subset_version', np_type=np.uint16, unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time)

    @classmethod
    def get_header_line_count(cls, filename: str) -> int:
        # No header in the file
        return 0

    @classmethod
    def extract_dataset_version(cls, filepath: str) -> DatasetVersion:
        # no versions, use default version 00
        return DatasetVersion("00")

    @classmethod
    def extract_subset_version(cls, filepath: str) -> int:
        """Extract subset version from input file name"""
        filename = os.path.split(filepath)[-1]
        pattern = cls.get_applicable_regex_pattern(filename)
        return np.uint16(re.search(pattern, filename).group('subset_version'))

    @classmethod
    def load_data_from_file(cls, filepath: str, filters:Union[list[DataFilter],None] = None) -> pd.DataFrame:
        # Overwrite the parent's method to add 'subset_version' column
        df = super().load_data_from_file(filepath, filters = filters)

        # insert column before the last column (timestamp)
        insertion_index = len(df.columns) - 1
        subset_ver = cls.extract_subset_version(filepath)
        df.insert(loc=insertion_index, column="subset_version", value=subset_ver)

        return df
