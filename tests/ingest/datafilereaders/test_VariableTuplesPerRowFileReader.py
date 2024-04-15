import logging
import os
import unittest
from datetime import datetime
from typing import Sequence, Type, Tuple, Dict, List

import numpy as np

from masschange.ingest import ingest
from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.db import get_db_connection
from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn, DerivedAsciiDataFileReaderColumn
from masschange.ingest.datafilereaders.gracefognv1a_prn import GraceFOGnv1APrnDataFileReader

log = logging.getLogger()


class VariableTuplesPerRowReaderTestCase(unittest.TestCase):
    def test_run(self):

        reader = GraceFOGnv1APrnDataFileReader()
        filepath = '/Users/ira/MASS_CHANGE_DATA/unit_test_data/GNV1A_2023-06-01_C_04.txt'
        raw_data = reader._load_raw_data_from_file(filepath)

