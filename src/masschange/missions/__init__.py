from dataclasses import dataclass, asdict
from enum import Enum

from typing_extensions import Callable


@dataclass(frozen=True)
class Mission:
    """Contains basic mission-wide configuration used by member datasets"""
    id: str
    label: str

    def asdict(self):
        return asdict(self)


class Missions(Enum):
    GraceFO = Mission(id='GRACEFO', label='GRACE-FO')

    def __init__(self, item):
        self.id = item.id
        self.label = item.label
        self.asdict: Callable = item.asdict
