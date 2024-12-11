from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class QueryTree:
    type: str
    val: str
    childs: List['QueryTree']
    parent: Optional['QueryTree']