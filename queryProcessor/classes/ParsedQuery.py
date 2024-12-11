from dataclasses import dataclass, field
from typing import List, Optional
from classes.QueryTree import QueryTree

@dataclass
class ParsedQuery:
    query_tree: QueryTree
    query: str