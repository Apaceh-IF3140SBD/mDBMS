from datetime import datetime
from typing import Optional, List
from queryProcessor.functions.DataPass import DataPass

class ExecutionResult:
    def __init__(
        self,
        transaction_id: int,
        timestamp: datetime,
        message: str,
        data_before: Optional['DataPass'],
        data_after: Optional['DataPass'],
        query: str
    ):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data_before = data_before
        self.data_after = data_after
        self.query = query

