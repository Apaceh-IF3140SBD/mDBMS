from typing import List, Generic, TypeVar

T = TypeVar('T')

class Rows(Generic[T]):
    def __init__(self, data: List[T]):
        self.data = data
        self.rows_count = len(data)