from datetime import datetime
from functions.Rows import Rows
from functions.ExecutionResult import ExecutionResult
from core.FailureRecovery import FailureRecovery
from storage.functions.DataWrite import DataWrite
from storage.functions.DataDeletion import DataDeletion
from storage.core.BufferManager import BufferManager
from storage.core.StorageEngine import StorageEngine

schemas = {}
buffer_manager = BufferManager(buffer_size=5, schemas=schemas)
storage_engine = StorageEngine(buffer_manager)
recovery = FailureRecovery(storageEngine=storage_engine)

# Start Transaction
transaction_id = 1
start_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="Transaction Started",
    data_before=None,
    data_after=None,
    query="START"
)
recovery.write_log(start_log.to_wal_log_entry())

# Insert Test Format
rows_after_insert = Rows(data=["new_row1", "new_row2"])
data_after_insert = DataWrite(data=rows_after_insert)
insert_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="Insert Operation",
    data_before=None,
    data_after=data_after_insert,
    query="INSERT INTO example_table"
)
recovery.write_log(insert_log.to_wal_log_entry())

# Update Test Format
rows_before_update = Rows(data=["old_row1", "old_row2"])
rows_after_update = Rows(data=["updated_row1", "updated_row2"])
data_before_update = DataWrite(data=rows_before_update)
data_after_update = DataWrite(data=rows_after_update)
update_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="Update Operation",
    data_before=data_before_update,
    data_after=data_after_update,
    query="UPDATE example_table"
)
recovery.write_log(update_log.to_wal_log_entry())

# Delete Test Format
rows_before_delete = Rows(data=["deleted_row1", "deleted_row2"])
data_before_delete = DataDeletion(data=rows_before_delete)
delete_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="Delete Operation",
    data_before=data_before_delete,
    data_after=None,
    query="DELETE FROM example_table"
)
recovery.write_log(delete_log.to_wal_log_entry())

# Commit
end_log = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=datetime.now(),
    message="Transaction Committed",
    data_before=None,
    data_after=None,
    query="COMMIT"
)
recovery.write_log(end_log.to_wal_log_entry())

recovery.display_logs()
