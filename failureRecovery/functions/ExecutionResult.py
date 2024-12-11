from datetime import datetime
from typing import Union, Any, Optional, Generic, List
from failureRecovery.functions.WALLogEntry import WALLogEntry
from failureRecovery.functions.DataPass import DataPass
from failureRecovery.functions.Rows import Rows
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataDeletion import DataDeletion

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

    ### Converts ExecutionResult to WALLogEntry
    def to_wal_log_entry(
        self,
        active_trans: Optional[List[int]] = []
    ) -> 'WALLogEntry':
        table_name = self.data_after.table if self.data_after else "Unknown"
        table_name = self.data_before.table if self.data_before else table_name
        
        if self.data_after and not self.data_before:
            operation_type = "INSERT"
        elif self.data_before and not self.data_after:
            operation_type = "DELETE"
        elif self.data_before and self.data_after:
            operation_type = "UPDATE"
        elif self.query == "START":
            operation_type = "START"
        elif self.query == "COMMIT":
            operation_type = "COMMIT"
        elif self.query == "ABORT":
            operation_type = "ABORT"
        # else:
        #     operation_type = "UNKNOWN"
        
        return WALLogEntry(
            log_sequence_number=None,
            transaction_id=self.transaction_id,
            operation_type=operation_type,
            data_before=self.data_before,
            data_after=self.data_after,
            table_name=table_name,
            active_trans=active_trans,
            timestamp=self.timestamp
        )
