from enum import Enum
from dataclasses import dataclass, field
from typing import Callable
class LoadModes(Enum):
    overwrite='overwrite'
    append='append'


@dataclass
class DataCheck:
    check: str = field()
    op: Callable[[int, int], bool] = field()
    value: int = field()