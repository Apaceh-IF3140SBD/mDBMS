# save_checkpoint checking

from datetime import datetime
from storage.functions.DataWrite import DataWrite
from storage.core.BufferManager import BufferManager
from storage.core.StorageEngine import StorageEngine
from functions.WALLogEntry import WALLogEntry
from core.FailureRecovery import FailureRecovery

schemas = {}
buffer_manager = BufferManager(buffer_size=5, schemas=schemas)
storage_engine = StorageEngine(buffer_manager)
recovery = FailureRecovery(storageEngine=storage_engine)

# Start Entry 1
start_entry_1 = WALLogEntry(
    log_sequence_number=None,  
    transaction_id=1,
    operation_type="START",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(start_entry_1)

# Start Entry 2
start_entry_2 = WALLogEntry(
    log_sequence_number=None,
    transaction_id=2,
    operation_type="START",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(start_entry_2)

# Commit Entry 1
commit_entry = WALLogEntry(
    log_sequence_number=None,
    transaction_id=1,
    operation_type="COMMIT",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(commit_entry)

# Save Checkpoint
recovery.save_checkpoint()

# Start Entry 3
start_entry_3 = WALLogEntry(
    log_sequence_number=None,
    transaction_id=3,
    operation_type="START",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(start_entry_3)

# Start Entry 4
start_entry_4 = WALLogEntry(
    log_sequence_number=None,
    transaction_id=4,
    operation_type="START",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(start_entry_4)

# Commit Entry 2
commit_entry_2 = WALLogEntry(
    log_sequence_number=None,
    transaction_id=2,
    operation_type="COMMIT",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(commit_entry_2)

# Save Checkpoint
recovery.save_checkpoint()

# Commit Entry 4
commit_entry_4 = WALLogEntry(
    log_sequence_number=None,
    transaction_id=4,
    operation_type="COMMIT",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(commit_entry_4)

# Save Checkpoint
recovery.save_checkpoint()

# Display Logs
recovery.display_logs()
