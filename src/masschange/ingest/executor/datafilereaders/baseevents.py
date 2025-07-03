from __future__ import annotations
from abc import abstractmethod
from typing import Dict, List, Union
import numpy as np
import yaml

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader
from masschange.utils.misc import flatten_nested_dict

class EventsFileReader(AsciiDataFileReader):
    """
    Data reader for events file in YAML format.
    """
    @classmethod
    def _load_raw_data_from_file(cls, filename: str) -> np.ndarray:
        with open(filename, 'r') as f:
            raw = f.read()
        content = yaml.safe_load(raw)

        # filter content to get data only for the desired event type
        filtered_content = [event for event in content if cls.get_event_type_filter() in event]
        column_defs = cls.get_input_column_defs()

        # create np.recarray to hold data for the events
        data_rec = np.recarray(len(filtered_content), dtype=np.dtype([(col.name, col.np_dtype) for col in column_defs]))

        for i, event in enumerate(filtered_content):
            data = np.array(cls._get_values_by_keys(flatten_nested_dict(event), [col.name for col in column_defs]))
            data_row = np.core.records.fromarrays(data, dtype=np.dtype([(col.name, col.np_dtype) for col in column_defs]))

            data_rec[i] = data_row

        # replace commas with semicolons, because commas break conversion to csv during ingestion
        # replace '\n' with '\\n' to store multiline strings as a single line for csv conversion
        for column_name in data_rec.dtype.names:
            col_dtype = data_rec.dtype[column_name]

            # Check if the field's data is a fixed-length Unicode strings
            if col_dtype.kind == 'U':
                data_rec[column_name] = np.char.replace(data_rec[column_name], ',', ";")
                data_rec[column_name] = np.char.replace(data_rec[column_name], '\n', '\\n')
        return data_rec

    @classmethod
    def _get_values_by_keys(cls, events_dict: Dict, keys: List[str]) -> List[Union[str, None]]:
        """
        Given a list of keys, return a list of values from the dictionary. Some values could be None.
        """
        return [events_dict.get(k) for k in keys]

    @classmethod
    @abstractmethod
    def get_event_type_filter(cls) -> str:
        """
        Event YAML file contains data for different types of events, for example, 'spacecraft_event','soe_event'.
        We want to have separate readers for each type of events.
        This method returns the event type for the reader.
        The name should correspond to the event type key in the input events YAML file
        """
        pass


