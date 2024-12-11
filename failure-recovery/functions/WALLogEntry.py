from typing import Optional, List, Generic, TypeVar, Union
from datetime import datetime
from functions.Rows import Rows
from storage.functions.DataWrite import DataWrite
from storage.functions.DataDeletion import DataDeletion
from storage.functions.Condition import Condition
from functions.DataPass import DataPass

T = TypeVar('T')

from typing import Optional, Union
from datetime import datetime

class WALLogEntry:
    def __init__(
        self,
        log_sequence_number: Optional[int],
        transaction_id: int,
        operation_type: str,
        data_before: Optional['DataPass'],
        data_after: Optional['DataPass'],
        table_name: str,
        active_trans: Optional[list[int]] = None,
        timestamp: Optional[datetime] = None,
    ):
        self.log_sequence_number = log_sequence_number
        self.transaction_id = transaction_id
        self.operation_type = operation_type
        self.data_before = data_before
        self.data_after = data_after
        self.table_name = table_name
        self.active_trans = active_trans if active_trans is not None else []
        self.timestamp = timestamp or datetime.now()

    ### Converts WALLogEntry to JSON format
    def to_dict(self) -> dict:
        def serialize_data(data):
            if isinstance(data, DataWrite):
                return {
                    "type": "DataWrite",
                    "table": data.table,
                    "columns": data.columns,
                    "new_value": data.new_value,
                    "conditions": [serialize_data(cond) for cond in data.conditions] if data.conditions else [],
                }
            elif isinstance(data, DataDeletion):
                return {
                    "type": "DataDeletion",
                    "table": data.table,
                    "conditions": [serialize_data(cond) for cond in data.conditions] if data.conditions else [],
                }
            elif isinstance(data, Condition):
                return {
                    "type": "Condition",
                    "column": data.column,
                    "operation": data.operation,
                    "operand": data.operand,
                }
            elif isinstance(data, DataPass):
                return {
                    "type": "DataPass",
                    "db": data.db,
                    "table": data.table,
                    "cols": data.cols,
                    # "data": data.data.to_dict() if hasattr(data.data, "to_dict") else data.data,
                    "data": serialize_data(data.data) if data.data else None,
                    "todo": serialize_data(data.todo) if data.todo else None,
                }
            elif isinstance(data, Rows):
                return {
                    "type": "Rows",
                    "data": data.data,
                }
            return None

        return {
            "log_sequence_number": self.log_sequence_number,
            "transaction_id": self.transaction_id,
            "operation_type": self.operation_type,
            "data_before": serialize_data(self.data_before),
            "data_after": serialize_data(self.data_after),
            "table_name": self.table_name,
            "active_trans": self.active_trans,
            "timestamp": self.timestamp.isoformat(),
        }

    ### Converts from JSON to WALLogEntry format
    @classmethod
    def from_dict(cls, data: dict) -> 'WALLogEntry':
        def deserialize_data(data_entry):
            if not data_entry:
                return None
            if data_entry["type"] == "DataWrite":
                conditions = [deserialize_data(cond) for cond in data_entry["conditions"]] if data_entry.get("conditions") else []
                return DataWrite(
                    table=data_entry["table"],
                    columns=data_entry["columns"],
                    new_value=data_entry["new_value"],
                    conditions=conditions,
                )
            elif data_entry["type"] == "DataDeletion":
                conditions = [deserialize_data(cond) for cond in data_entry["conditions"]] if data_entry.get("conditions") else []
                return DataDeletion(
                    table=data_entry["table"],
                    conditions=conditions,
                )
            elif data_entry["type"] == "Condition":
                return Condition(
                    column=data_entry["column"],
                    operation=data_entry["operation"],
                    operand=data_entry["operand"]
                )
            elif data_entry["type"] == "DataPass":
                return DataPass(
                    db=data_entry["db"],
                    table=data_entry["table"],
                    cols=data_entry["cols"],
                    data=deserialize_data(data_entry["data"]) if data_entry.get("data") else None,
                    todo=deserialize_data(data_entry["todo"]) if data_entry.get("todo") else None,
                )
            elif data_entry["type"] == "Rows":
                return Rows(data_entry["data"])
            return None

        if(data.get("data_before") != None):
            data_before = deserialize_data(data.get("data_before"))
        else:
            data_before = None
        if(data.get("data_after") != None):
            data_after = deserialize_data(data.get("data_after"))
        else:
            data_after = None
        timestamp = datetime.fromisoformat(data["timestamp"])
        active_trans = data.get("active_trans", [])

        return cls(
            log_sequence_number=data["log_sequence_number"],
            transaction_id=data["transaction_id"],
            operation_type=data["operation_type"],
            data_before=data_before,
            data_after=data_after,
            table_name=data["table_name"],
            active_trans=active_trans,
            timestamp=timestamp,
        )

    ### Converts a WALLogEntry format to the recovery or "undo" version of it, changing opration type and data (before - after)
    def get_undo_entry(self) -> Optional['WALLogEntry']:
        undo_operation_map = {
            "INSERT": "DELETE",
            "DELETE": "INSERT",
            "UPDATE": "UPDATE",
            "WRITE": "WRITE"
        }
    
        reverse_operation_type = undo_operation_map.get(self.operation_type)
    
        if not reverse_operation_type:
            return None
    
        return WALLogEntry(
            log_sequence_number=self.log_sequence_number + 1 if self.log_sequence_number else None,
            transaction_id=self.transaction_id,
            operation_type=reverse_operation_type,
            # data_before=None,
            # data_after=self.data_before,  
            # data_before=self.data_before,
            # data_after=self.data_after,
            data_before=self.data_after,
            data_after=self.data_before,
            table_name=self.table_name,
            active_trans=self.active_trans,
            timestamp=datetime.now(),
        )


