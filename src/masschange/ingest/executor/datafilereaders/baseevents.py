from __future__ import annotations
from abc import abstractmethod
from typing import Dict, List, Union
import numpy as np
import yaml

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader


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
        filtered_content = []
        for event in content:
            if cls.get_event_type_filter() in event:
                filtered_content.append(event)

        column_defs = cls.get_input_column_defs()

        # create np.recarray to hold data for the events
        data_rec = np.recarray(len(filtered_content),
                                     dtype=np.dtype([(col.name, col.np_dtype) for col in column_defs]))

        for i, event in enumerate(filtered_content):
            data = np.array(cls._get_values_by_keys(event, [col.name for col in column_defs]))
            data_row = np.core.records.fromarrays(data, dtype=np.dtype([(col.name, col.np_dtype) for col in column_defs]))
            data_rec[i-1] = data_row
        return data_rec

    @classmethod
    def _get_values_by_keys(cls, events_dict: Dict, keys: List[str]) -> List[Union[str, None]]:
        """
        Given a list of keys, return a list of values from the dictionary. Some values could be None.
        """
        data = []
        for column_name in keys:
            data.append(cls._get_value_by_key_recursively(events_dict, column_name))
        return data

    @classmethod
    def _get_value_by_key_recursively(cls, events_dict: Dict, target_key: str) -> Union[List[str], None]:

        """
        Get a string value by key from the dictionary recursively.
        Returns the value or None if the key does not exist

        If the key is not unique through all nested dictionaries (which should newer happen in the
        'event' yaml file with re-defined format), the first encountered string value will be returned
        """

        if target_key in events_dict:
            val = events_dict[target_key]
            if not isinstance(val, dict):
                return events_dict[target_key]

        for value in events_dict.values():
            if isinstance(value, dict):
                return cls._get_value_by_key_recursively(value, target_key)
        return None

    @classmethod
    @abstractmethod
    def get_event_type_filter(cls) -> str:
        """
        Event YAML file contains data for different types of events, for example, 'spacecraftevent','operator'.
        We want to have separate readers for each type of events.
        This method returns the event type for the reader.
        The name should correspond to the event type key in the input events YAML file
        """
        pass


