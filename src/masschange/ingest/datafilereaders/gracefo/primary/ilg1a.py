from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import LogFileReader, AsciiDataFileReaderColumn, \
    DerivedAsciiDataFileReaderColumn

class GraceFOIlg1ADataFileReader(LogFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ILG1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcv_time', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='pkt_count', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=2, name='GRACEFO_id', np_type='U1', unit=None),
            DerivedAsciiDataFileReaderColumn(name='logpacket', np_type='U1000', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcv_time)

    @classmethod
    def log_msg_column_name(cls):
        return 'logpacket'

    @classmethod
    def _load_raw_data_from_file(cls, filename: str) -> np.ndarray:
        # The ILG files seems to be encoded with 'windows-1252'
        # Replace carriage return characters and decode with 'windows-1252'

        contents = ''
        with open(filename, "rb") as input_file:
            while True:
                line = input_file.readline().replace(b"\r", b"").decode('windows-1252').strip()
                if not line:
                    break
                if line[-1] != ">":  # Remove rows with empty log message
                    contents += line + '\n'

        # create a tmp file for updated input data
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w") as tmp:
            tmp.write(contents)
            tmp.seek(0)  # go back to the beginning to read data from the file
            # read from tmp file
            return super()._load_raw_data_from_file(tmp.name)
