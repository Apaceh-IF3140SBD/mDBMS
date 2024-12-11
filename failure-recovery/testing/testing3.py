# recover checking

from datetime import datetime
from functions.Rows import Rows
from functions.WALLogEntry import WALLogEntry
from core.FailureRecovery import FailureRecovery
from functions.RecoverCriteria import RecoverCriteria

recovery = FailureRecovery()

start_entry_1 = WALLogEntry(
    log_sequence_number=0,
    transaction_id=1,
    operation_type="START",
    data_before=None,
    data_after=None,
    table_name="users",
    timestamp=datetime.now(),
)
recovery.write_log(start_entry_1)

start_entry_2 = WALLogEntry(
    log_sequence_number=0,
    transaction_id=2,
    operation_type="START",
    data_before=None,
    data_after=None,
    table_name="orders",
    timestamp=datetime.now(),
)
recovery.write_log(start_entry_2)

insert_entry = WALLogEntry(
    log_sequence_number=0,
    transaction_id=1,
    operation_type="INSERT",
    data_before=None,
    data_after=Rows(data=[{"id": 1, "name": "Dan"}]),
    table_name="users",
    timestamp=datetime.now(),
)
recovery.write_log(insert_entry)

update_entry = WALLogEntry(
    log_sequence_number=0,
    transaction_id=1,
    operation_type="UPDATE",
    data_before=Rows(data=[{"id": 1, "name": "Dan"}]),
    data_after=Rows(data=[{"id": 1, "name": "Dan Grip"}]),
    table_name="users",
    timestamp=datetime.now(),
)
recovery.write_log(update_entry)

delete_entry = WALLogEntry(
    log_sequence_number=0,
    transaction_id=2,
    operation_type="DELETE",
    data_before=Rows(data=[{"id": 1, "name": "Bob"}]),
    data_after=None,
    table_name="orders",
    timestamp=datetime.now(),
)
recovery.write_log(delete_entry)

commit_entry_1 = WALLogEntry(
    log_sequence_number=0,
    transaction_id=1,
    operation_type="COMMIT",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(commit_entry_1)

recovery.save_checkpoint()

start_entry_3 = WALLogEntry(
    log_sequence_number=0,
    transaction_id=3,
    operation_type="START",
    data_before=None,
    data_after=None,
    table_name="products",
    timestamp=datetime.now(),
)
recovery.write_log(start_entry_3)

insert_entry_3 = WALLogEntry(
    log_sequence_number=0,
    transaction_id=3,
    operation_type="INSERT",
    data_before=None,
    data_after=Rows(data=[{"id": 3, "name": "Product A"}]),
    table_name="products",
    timestamp=datetime.now(),
)
recovery.write_log(insert_entry_3)

commit_entry_2 = WALLogEntry(
    log_sequence_number=0,
    transaction_id=2,
    operation_type="COMMIT",
    data_before=None,
    data_after=None,
    table_name="",
    timestamp=datetime.now(),
)
recovery.write_log(commit_entry_2)

recovery.save_checkpoint()

start_entry_4 = WALLogEntry(
    log_sequence_number=0,
    transaction_id=4,
    operation_type="START",
    data_before=None,
    data_after=None,
    table_name="sales",
    timestamp=datetime.now(),
)
recovery.write_log(start_entry_4)

insert_entry_4 = WALLogEntry(
    log_sequence_number=0,
    transaction_id=4,
    operation_type="INSERT",
    data_before=None,
    data_after=Rows(data=[{"id": 4, "name": "Gemus"}]),
    table_name="sales",
    timestamp=datetime.now(),
)
recovery.write_log(insert_entry_4)

recovery.save_checkpoint()

recovery.display_logs()

# recover_criteria = RecoverCriteria(transaction_id=4)
# recovery.recover(recover_criteria)
