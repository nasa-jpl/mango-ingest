from __future__ import annotations
from datetime import datetime, timedelta, date
from typing import Iterable, Union


class TimeSpan:
    def __init__(self, begin: datetime, end: datetime = None, duration: timedelta = None):
        if end is not None and duration is not None:
            raise ValueError('args "end" and "duration" may not be simultaneously provided')

        if end is not None and begin > end:
            raise ValueError(f'argument "end" must not be earlier than argument "begin"')

        self._begin = begin

        if end is not None:
            self._duration = end - begin
        elif duration is not None:
            self._duration = duration
        else:
            raise ValueError('One of args "end", "duration" must be provided')

    def __eq__(self, other: TimeSpan) -> bool:
        if not isinstance(other, TimeSpan):
            return False

        return self.begin == other.begin and self.end == other.end and self.duration == other.duration

    @property
    def begin(self) -> datetime:
        return self._begin

    @property
    def end(self) -> datetime:
        return self._begin + self._duration

    @property
    def duration(self) -> timedelta:
        return self._duration

    def contains(self, dt: datetime) -> bool:
        return self.begin <= dt <= self.end

    def overlaps(self, d: date) -> bool:
        """
        :param d:
        :return: whether any portion of d overlaps this TimeSpan
        """
        dt = datetime(d.year, d.month, d.day)
        return d == self.begin.date() or d == self.end.date() or self.contains(dt)

    def get_overlapped_dates(self) -> Iterable[date]:
        """
        Yield a sorted iterable of all dates having any overlap with this TimeSpan
        :return:
        """
        date_iter = self.begin.date()
        while date_iter <= self.end.date():
            yield date_iter
            date_iter += timedelta(days=1)

    def intersection(self, other: Union[TimeSpan, None]) -> Union[TimeSpan, None]:
        """"
        Yield the intersection of this TimeSpan and another TimeSpan, returning None if there is no overlap
        """

        if other is None:
            return None

        earlier_span = self if self.begin <= other.begin else other
        later_span = other if earlier_span is self else self

        if earlier_span.end < later_span.begin:
            return None
        else:
            return TimeSpan(begin=later_span.begin, end=min(earlier_span.end, later_span.end))

    def union(self, other: Union[TimeSpan, None], allow_disconnected_spans: bool = False) -> TimeSpan:
        if self.intersection(other) is None and not allow_disconnected_spans:
            raise ValueError(f'cannot union TimeSpan {self} with non-intersection/non-adjacent TimeSpan {other} unless allow_disconnected_spans is set True')

        return TimeSpan(begin=min(self.begin, other.begin), end=max(self.end, other.end))

    def __str__(self):
        return f'TimeSpan(begin={self.begin.isoformat()}, end={self.end.isoformat()})'