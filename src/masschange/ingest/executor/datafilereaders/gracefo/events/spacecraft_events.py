from collections.abc import Collection
from datetime import datetime, timedelta

from masschange.ingest.executor.datafilereaders.baseevents import EventsFileReader
from masschange.ingest.executor.datafilereaders.base import DerivedAsciiDataFileReaderColumn, \
    AsciiDataFileReaderColumn

class GraceFOSpacecraftEventsDataFileReader(EventsFileReader):

    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    # TODO: re-do when the file naming convention for events file is defined
    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^events_(?P<instrument_id>[Y])_(?P<dataset_version>\d{2})\.yaml'


    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            DerivedAsciiDataFileReaderColumn(name='spacecraftevent', np_type='U100', unit=None),
            DerivedAsciiDataFileReaderColumn(name='time', np_type='U50', unit=None),
            DerivedAsciiDataFileReaderColumn(name='created', np_type='U50', unit=None),
            DerivedAsciiDataFileReaderColumn(name='createdby', np_type='U50', unit=None),
            DerivedAsciiDataFileReaderColumn(name='spacecraft', np_type='U6', unit=None),
            DerivedAsciiDataFileReaderColumn(name='meta', np_type='U100', unit=None)
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        """Converts GPS time represented as a string in format 'YYYY-MM-DD HH:MM:SS GPS'
        to TAI time (datetime object)."""
        try:
            return datetime.strptime(row.time, '%Y-%m-%d %H:%M:%S GPS')
        except ValueError:
            f"Error converting spacecraft event time: {row.time} to a time object..."

    @classmethod
    def get_event_type_filter(cls) -> str:
        return 'spacecraftevent'

