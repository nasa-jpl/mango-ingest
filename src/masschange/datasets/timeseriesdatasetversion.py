from typing import Union


class TimeSeriesDatasetVersion:
    _value: str

    def __init__(self, value: Union[int, str, None]):
        """TODO: Remove None support once legacy support is no longer needed"""
        if not any(isinstance(value, accepted_type) for accepted_type in [int, str, None]):
            raise ValueError(f'arg "value" must be of type int or str (got {type(value)})')

        self._value = str(value) if value is not None else None

    @property
    def is_null(self):
        return self._value is None

    @property
    def value(self):
        return self._value

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return f'TimeSeriesDatasetVersion("{self.value}")'
