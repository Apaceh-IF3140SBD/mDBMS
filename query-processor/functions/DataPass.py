from typing import Union, Optional, List
from storage.functions.DataWrite import DataWrite
from storage.functions.DataDeletion import DataDeletion
from classes.Rows import Rows

class DataPass:
    def __init__(
        self,
        db: str,
        table: str,
        cols: List[str],
        data: Rows,
        todo: Optional[Union['DataWrite', 'DataDeletion']]
    ):
        self.db = db
        self.table = table
        self.cols = cols
        self.data = data
        self.todo = todo