from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class QueryTree:
    qtype: str
    val: str
    childs: List['QueryTree']
    parent: Optional['QueryTree']