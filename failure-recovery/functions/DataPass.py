from datetime import datetime
from typing import Union, Any, Optional, Generic, List
from storage.functions.DataWrite import DataWrite
from storage.functions.DataDeletion import DataDeletion
from functions.Rows import Rows

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
