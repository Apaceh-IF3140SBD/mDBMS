from typing import Union, Optional, List
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataDeletion import DataDeletion
from queryProcessor.classes.Rows import Rows

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