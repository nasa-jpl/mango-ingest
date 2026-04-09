from collections import defaultdict
from typing import List


class KeyValueQueryParameter:
    key: str
    value: str

    def __init__(self, raw_input: str):
        if len([c for c in raw_input if c == '=']) != 1:
            raise ValueError(
                f'key-value query parameter must have value of form "{{key}}={{value}}" (got "{raw_input}")')

        self.key, self.value = raw_input.split('=', maxsplit=1)

    def __lt__(self, other):
        return self.key < other.key

class KeyValueFilterSet:
    _filters: List[KeyValueQueryParameter]

    def __init__(self, filters: List[KeyValueQueryParameter]):
        self._filters = filters

    def __iter__(self):
        return iter(self.as_dict())

    def as_dict(self) -> dict[str, set[str]]:
        value_sets_by_key = defaultdict(set)
        for filter in self._filters:
            value_sets_by_key[filter.key].add(filter.value)
        return value_sets_by_key

    def add(self, filter: KeyValueQueryParameter):
        self._filters.append(filter)