import os
import json
import pickle
from typing import List, Union, Any
from datetime import datetime
import threading
import time

from failureRecovery.functions.ExecutionResult import ExecutionResult
from failureRecovery.functions.RecoverCriteria import RecoverCriteria
from failureRecovery.functions.WALLogEntry import WALLogEntry
from failureRecovery.functions.utils import *
from storageManager.functions.DataWrite import DataWrite
from storageManager.functions.DataDeletion import DataDeletion
from storageManager.core.StorageEngine import StorageEngine
from storageManager.functions.Condition import Condition


class FailureRecovery:
    def __init__(self, storageEngine: StorageEngine, log_file_path: str = "write_ahead_log.json"):
        self.buffer: List[Any] = []
        self.log_file_path = log_file_path
        self.write_ahead_log: List[WALLogEntry] = []
        self.log_sequence_number = 0
        self.storageEngine = storageEngine
        self._load_logs()
        self.system_recover()
        self.save_5_mins()

    def save_5_mins(self):
        def periodic_save():
            while True:
                time.sleep(300)
                self.save_checkpoint()

        save_thread = threading.Thread(target=periodic_save, daemon=True)
        save_thread.start()

    ### This method is called when commiting a certain transaction
    def commit(self, transaction_id) -> None:
        commit_log = ExecutionResult(
            transaction_id=transaction_id,
            timestamp=datetime.now(),
            message="COMMIT",
            data_before=None,
            data_after=None,
            query="COMMIT"
        )
        commit_wal_entry = commit_log.to_wal_log_entry()
        self.log_sequence_number += 1
        commit_wal_entry.log_sequence_number = self.log_sequence_number
        self.write_ahead_log.append(commit_wal_entry)

    ### This method is called when writing a log (currently the Failure Recovery component handles the storage writing)
    def write_log(self, info: ExecutionResult) -> None:
        transaction_id = info.transaction_id
    
        start_count = sum(1 for log_entry in self.write_ahead_log 
                          if log_entry.transaction_id == transaction_id and log_entry.operation_type == "START")
        end_count = sum(1 for log_entry in self.write_ahead_log 
                        if log_entry.transaction_id == transaction_id and log_entry.operation_type in ["COMMIT", "ABORT"])
        
        if start_count <= end_count:
            start_log = ExecutionResult(
                transaction_id=transaction_id,
                timestamp=datetime.now(),
                message="Transaction Started",
                data_before=None,
                data_after=None,
                query="START"
            )
            start_wal_entry = start_log.to_wal_log_entry()
            self.log_sequence_number += 1
            start_wal_entry.log_sequence_number = self.log_sequence_number
            self.write_ahead_log.append(start_wal_entry)
            # print(f"Start log written: {start_wal_entry.to_dict()}")
        
        info_wal = info.to_wal_log_entry()
        self.log_sequence_number += 1
        info_wal.log_sequence_number = self.log_sequence_number
    
        self.write_ahead_log.append(info_wal)
        self._write_to_file()
        # print(f"Log entry written: {info_wal.to_dict()}")

        ### Type 1 - using todo
        # if (info_wal.operation_type == "INSERT"):
        #     self.storageEngine.insert(info.data_after.todo)
        # elif (info_wal.operation_type == "UPDATE"):
        #     self.storageEngine.update(info.data_after.todo)
        # elif (info_wal.operation_type == "DELETE"):
        #     self.storageEngine.delete(info.data_after.todo)

        ### Type 2 - executing each row
        if info_wal.operation_type == "INSERT":
            insert_data = [
                DataWrite(
                    table=info.data_after.table,
                    columns=info.data_after.cols,
                    new_value=[row[info.data_after.cols.index(col)] for col in info.data_after.cols],
                    conditions=[]
                )
                for row in info.data_after.data.data
            ]
            for data in insert_data:
                print(data.new_value)
                self.storageEngine.insert(data)
        elif info_wal.operation_type == "UPDATE":
            delete_conditions = [
                [
                    Condition(column=col, operation='=', operand=row[info.data_before.cols.index(col)])
                    for col in info.data_before.cols
                ]
                for row in info.data_before.data.data
            ]
            for b in delete_conditions:
                for a in b:
                    print (a.column, a.operation, a.operand)
            print(info.data_before.table)

            for conditions in delete_conditions:
                delete_data = DataDeletion(
                    table=info.data_before.table,
                    conditions=conditions
                )
                self.storageEngine.delete(delete_data)

            insert_data = [
                DataWrite(
                    table=info.data_after.table,
                    columns=info.data_after.cols,
                    new_value=[row[info.data_after.cols.index(col)] for col in info.data_after.cols],
                    conditions=[]
                )
                for row in info.data_after.data.data
            ]
            for data in insert_data:
                print(data.new_value)
                self.storageEngine.insert(data)
        elif info_wal.operation_type == "DELETE":
            delete_conditions = [
                [
                    Condition(column=col, operation='=', operand=row[info.data_before.cols.index(col)])
                    for col in info.data_before.cols
                ]
                for row in info.data_before.data.data
            ]
            for b in delete_conditions:
                for a in b:
                    print (a.column, a.operation, a.operand)
            print(info.data_before.table)

            for conditions in delete_conditions:
                delete_data = DataDeletion(
                    table=info.data_before.table,
                    conditions=conditions
                )
                self.storageEngine.delete(delete_data)

    ### This function is called withid recovery to handle write log and storage engine using WALLogEntry as parameter
    def write_log_recover(self, info: WALLogEntry) -> None:
        self.log_sequence_number += 1
        info.log_sequence_number = self.log_sequence_number

        self.write_ahead_log.append(info)

        if (info.operation_type == "INSERT"):
            ### PREVIOUS VERSION
            # for loop using the info.data_after (the format is DataPass) for each list in rows use the primary key and use it in the condition
            # delete_conditions = [
            #     Condition(column=info.data_after.primary_key, operation='=', operand=row[info.data_after.cols.index(info.data_after.primary_key)])
            #     for row in info.data_after.data.data
            # ]
            # delete_data = DataDeletion(
            #     table=info.data_after.table,
            #     conditions=delete_conditions
            # )
            # self.storageEngine.delete(delete_data)

            insert_data = [
                DataWrite(
                    table=info.data_after.table,
                    columns=info.data_after.cols,
                    new_value=[row[info.data_after.cols.index(col)] for col in info.data_after.cols],
                    conditions=[]
                )
                for row in info.data_after.data.data
            ]
            for data in insert_data:
                print(data.new_value)
                self.storageEngine.insert(data)            
        elif (info.operation_type == "UPDATE"):
            ### PREVIOUS VERSION
            # for loop using the info.data_after (the format is DataPass) for each list in rows use the primary key and use it in the condition
            # delete_conditions = [
            #     Condition(column=info.data_after.primary_key, operation='=', operand=row[info.data_after.cols.index(info.data_after.primary_key)])
            #     for row in info.data_after.data.data
            # ]
            # delete_data = DataDeletion(
            #     table=info.data_after.table,
            #     conditions=delete_conditions
            # )
            # self.storageEngine.delete(delete_data)

            # # for loop using the info.data_after (the format is DataPass) for each list in rows use the primary key and use it in the condition
            # insert_data = [
            #     DataWrite(
            #         table=info.data_before.table,
            #         columns=info.data_before.cols,
            #         new_value=[row[info.data_before.cols.index(col)] for col in info.data_before.cols],
            #         conditions=[]
            #     )
            #     for row in info.data_before.data.data
            # ]

            # for data in insert_data:
            #     print(data.new_value)
            #     self.storageEngine.insert(data)

            delete_conditions = [
                [
                    Condition(column=col, operation='=', operand=row[info.data_before.cols.index(col)])
                    for col in info.data_before.cols
                ]
                for row in info.data_before.data.data
            ]
            for b in delete_conditions:
                for a in b:
                    print (a.column, a.operation, a.operand)
            print(info.data_before.table)

            for conditions in delete_conditions:
                delete_data = DataDeletion(
                    table=info.data_before.table,
                    conditions=conditions
                )
                self.storageEngine.delete(delete_data)

            insert_data = [
                DataWrite(
                    table=info.data_after.table,
                    columns=info.data_after.cols,
                    new_value=[row[info.data_after.cols.index(col)] for col in info.data_after.cols],
                    conditions=[]
                )
                for row in info.data_after.data.data
            ]
            for data in insert_data:
                print(data.new_value)
                self.storageEngine.insert(data)

        if (info.operation_type == "DELETE"):
            ### PREVIOUS VERSION
            # for loop using the info.data_after (the format is DataPass) for each list in rows use the primary key and use it in the condition
            # insert_data = [
            #     DataWrite(
            #         table=info.data_before.table,
            #         columns=info.data_before.cols,
            #         new_value=[row[info.data_before.cols.index(col)] for col in info.data_before.cols],
            #         conditions=[]
            #     )
            #     for row in info.data_before.data.data
            # ]

            # for data in insert_data:
            #     print(data.new_value)
            #     self.storageEngine.insert(data)

            delete_conditions = [
                [
                    Condition(column=col, operation='=', operand=row[info.data_before.cols.index(col)])
                    for col in info.data_before.cols
                ]
                for row in info.data_before.data.data
            ]
            for b in delete_conditions:
                for a in b:
                    print (a.column, a.operation, a.operand)
            print(info.data_before.table)

            for conditions in delete_conditions:
                delete_data = DataDeletion(
                    table=info.data_before.table,
                    conditions=conditions
                )
                self.storageEngine.delete(delete_data)
        
        self._write_to_file()

        # print(f"Stable storage confirmed: {info.to_dict()}")
        return 0

    ### This function is called when system failure occured and Failure Recovery is reinstantiated
    def write_log_system_failure(self) -> None:
        def process_log_entry(log_entry):
            if log_entry.operation_type == "INSERT":
                insert_data = [
                    DataWrite(
                        table=log_entry.data_after.table,
                        columns=log_entry.data_after.cols,
                        new_value=[row[log_entry.data_after.cols.index(col)] for col in log_entry.data_after.cols],
                        conditions=[]
                    )
                    for row in log_entry.data_after.data.data
                ]
                for data in insert_data:
                    print(data.new_value)
                    self.storageEngine.insert(data)

            elif log_entry.operation_type == "UPDATE":
                delete_conditions = [
                    [
                        Condition(column=col, operation='=', operand=row[log_entry.data_before.cols.index(col)])
                        for col in log_entry.data_before.cols
                    ]
                    for row in log_entry.data_before.data.data
                ]

                for conditions in delete_conditions:
                    delete_data = DataDeletion(
                        table=log_entry.data_before.table,
                        conditions=conditions
                    )
                    self.storageEngine.delete(delete_data)

                insert_data = [
                    DataWrite(
                        table=log_entry.data_after.table,
                        columns=log_entry.data_after.cols,
                        new_value=[row[log_entry.data_after.cols.index(col)] for col in log_entry.data_after.cols],
                        conditions=[]
                    )
                    for row in log_entry.data_after.data.data
                ]
                for data in insert_data:
                    print(data.new_value)
                    self.storageEngine.insert(data)

            elif log_entry.operation_type == "DELETE":
                delete_conditions = [
                    [
                        Condition(column=col, operation='=', operand=row[log_entry.data_before.cols.index(col)])
                        for col in log_entry.data_before.cols
                    ]
                    for row in log_entry.data_before.data.data
                ]

                for conditions in delete_conditions:
                    delete_data = DataDeletion(
                        table=log_entry.data_before.table,
                        conditions=conditions
                    )
                    self.storageEngine.delete(delete_data)

        checkpoint_index = -1
        for i, log in enumerate(self.write_ahead_log):
            if log.operation_type == "CHECKPOINT":
                checkpoint_index = i
                break

        if checkpoint_index == -1:
            print("No checkpoint found in logs.")
            self.save_checkpoint()

        for log_entry in self.write_ahead_log[checkpoint_index + 1:]:
            process_log_entry(log_entry)


    
    ### This function is called when no storage engine involved in logging (start, commit, abort)
    def write_log_wal(self, info: WALLogEntry) -> None:
        self.log_sequence_number += 1
        info.log_sequence_number = self.log_sequence_number

        self.write_ahead_log.append(info)

        self._write_to_file()

        # print(f"Stable storage confirmed: {info.to_dict()}")

    ### This function is called when ...
    def save_checkpoint(self) -> None:
        self.write_ahead_log = [
            log for log in self.write_ahead_log if log.operation_type != "CHECKPOINT"
        ]

        active_transactions = set()
        transaction_status = {}
        last_start_log = {}

        for i, log in enumerate(self.write_ahead_log):
            transaction_id = log.transaction_id

            if log.operation_type == "START":
                transaction_status[transaction_id] = "ACTIVE"
                last_start_log[transaction_id] = i
            elif log.operation_type == "COMMIT":
                transaction_status[transaction_id] = "COMMITTED"
            elif log.operation_type == "ABORT":
                transaction_status[transaction_id] = "ABORTED"

        for transaction_id, status in transaction_status.items():
            if status == "ACTIVE":
                active_transactions.add(transaction_id)

        filtered_logs = []
        for i, log in enumerate(self.write_ahead_log):
            transaction_id = log.transaction_id

            if transaction_id in active_transactions:
                if transaction_id in last_start_log and i >= last_start_log[transaction_id]:
                    filtered_logs.append(log)
            elif transaction_id == -1:
                filtered_logs.append(log)

        self.write_ahead_log = filtered_logs

        checkpoint_entry = WALLogEntry(
            log_sequence_number=len(self.write_ahead_log) + 1,
            transaction_id=-1,
            operation_type="CHECKPOINT",
            data_before=None,
            data_after=None,
            table_name="",
            active_trans=list(active_transactions),
        )
        self.write_ahead_log.append(checkpoint_entry)

        print(f"Checkpoint active transactions: {active_transactions}")

        for transaction_id, status in transaction_status.items():
            if status == "COMMITTED":
                print(f"Transaction {transaction_id} committed.")
            elif status == "ABORTED":
                print(f"Transaction {transaction_id} aborted.")

        self._write_to_file()
        self.storageEngine.buffer_manager.flush_all_block()



    ### This function is called when...
    def system_recover(self) -> None:
        if not self.write_ahead_log:
            self.save_checkpoint()

        else:
            # for log_entry in self.write_ahead_log:
            self.write_log_system_failure() # load to buffer

            print("System recovery complete.")
            return 0

    ### This function is called when a transaction is getting aborted
    def recover(self, criteria: RecoverCriteria) -> None:
        if not self.write_ahead_log:
            self._load_logs()
        for entry in reversed(self.write_ahead_log):
            # print(entry.operation_type)
            if criteria.transaction_id and entry.transaction_id != criteria.transaction_id:
                continue
            if criteria.timestamp and entry.timestamp > criteria.timestamp:
                continue

            # Start -> Abort
            if entry.operation_type == "START":
                undo_entry = WALLogEntry(
                    log_sequence_number=self.log_sequence_number + 1,
                    transaction_id=entry.transaction_id,
                    operation_type="ABORT",
                    data_before=None,
                    data_after=None,
                    table_name=entry.table_name,
                    active_trans=entry.active_trans,
                    timestamp=datetime.now(),
                )
                self.write_log_wal(undo_entry)

            # Undo (belum yakin operation type penting atau engga)
            elif entry.operation_type in ("INSERT", "DELETE", "UPDATE"):
                undo_entry = entry.get_undo_entry()
                if undo_entry:
                    self.write_log_recover(undo_entry)

            # Safety measure
            if entry.operation_type in ("COMMIT", "ABORT"):
                break

    def _write_to_file(self) -> None:
        with open(self.log_file_path, "w") as file:
            json.dump([entry.to_dict() for entry in self.write_ahead_log], file)
        json_to_bin("write_ahead_log.json", "write_ahead_log.bin")

    def _load_logs(self) -> None:
        bin_to_json("write_ahead_log.bin", "write_ahead_log.json")
        if os.path.exists(self.log_file_path):
            with open(self.log_file_path, "r") as file:
                file_content = file.read().strip()
                if not file_content:
                    print("Log file empty, starting new log")
                    self.write_ahead_log = []
                else:
                    try:
                        log_data = json.loads(file_content)
                        self.write_ahead_log = [WALLogEntry.from_dict(entry) for entry in log_data]
                        self.log_sequence_number = max(
                            entry.log_sequence_number for entry in self.write_ahead_log
                        )
                    except json.JSONDecodeError:
                        print("Error reading log file, starting new log")
                        self.write_ahead_log = []
        else:
            print("Log file not found, starting new log")
            self.write_ahead_log = []

    def display_logs(self) -> None:
        print("\nIn-Memory Write Ahead Log:")
        for entry in self.write_ahead_log:
            print(entry.to_dict())
