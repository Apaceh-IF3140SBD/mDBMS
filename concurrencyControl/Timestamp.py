import threading
from concurrencyControl.Utils import *
from concurrencyControl.CCManager import CCManager
import time


class TimestampBasedConcurrencyControlManager(CCManager):
    def __init__(self):
        self.transactions = {}
        self.timestamps = {}
        self.current_timestamp = 0
        self.lock = threading.Lock()

    def begin_transaction(self) -> int:
        # Increase the current timestamp and use it to create a new transaction
        with self.lock:
            self.current_timestamp += 1
            transaction = Transaction(self.current_timestamp)
            self.transactions[transaction.id] = transaction

        print(
            f"Transaction {transaction.id} started with timestamp {transaction.timestamp}.")
        return transaction.id

    def log_object(self, row: Row, transaction_id: int):
        with self.lock:
            if transaction_id not in self.transactions:
                raise ValueError(f"Transaction {transaction_id} not found.")

            for record in row.data:
                record_tuple = tuple(record)
                record_id = hash(record_tuple)
                if record_id not in self.timestamps:
                    self.timestamps[record_id] = {"read_ts": 0, "write_ts": 0}
                print(
                    f"Transaction {transaction_id} logged access to record {record_id}.")

    def validate_object(self, row: Row, transaction_id: int, action: Action) -> Response:
        conflicting_records = []

        if transaction_id not in self.transactions:
            raise ValueError(f"Transaction {transaction_id} not found.")

        transaction = self.transactions[transaction_id]

        for record in row.data:
            
            record_tuple = tuple(record)
            record_id = hash(record_tuple)

            if record_id not in self.timestamps:
                self.log_object(
                    Row([record], row.rows_count), transaction_id)
                continue
            with self.lock:
                read_ts = self.timestamps[record_id]["read_ts"]
                write_ts = self.timestamps[record_id]["write_ts"]

            if action == Action.READ:
                if transaction.timestamp < write_ts:
                    conflicting_records.append(record_id)
            elif action == Action.WRITE:
                if transaction.timestamp < write_ts or transaction.timestamp < read_ts:
                    conflicting_records.append(record_id)

        # Check for conflicts after releasing the lock
        if conflicting_records:
            # Phase 2: Abort Transaction
            print(
                f"Transaction {transaction_id} aborted due to timestamp conflicts on records: {conflicting_records}.")
            with self.lock:
                self.transactions[transaction_id].state = TransactionState.ABORTED
            return Response(allowed=False, transaction_id=transaction_id)

        # Phase 3: Update Timestamps
        with self.lock:
            for record in row.data:
                # Replace with a stable identifier as discussed
                
                record_tuple = tuple(record)
                record_id = hash(record_tuple)

                if action == Action.READ:
                    self.timestamps[record_id]["read_ts"] = max(
                        self.timestamps[record_id]["read_ts"], transaction.timestamp)
                elif action == Action.WRITE:
                    self.timestamps[record_id]["write_ts"] = transaction.timestamp

        print(
            f"Transaction {transaction_id} allowed {action.value} on all records in the Row.")
        return Response(allowed=True, transaction_id=transaction_id)

    def end_transaction(self, transaction_id: int):
        if transaction_id not in self.transactions:
            raise ValueError(f"Transaction {transaction_id} not found.")

        transaction = self.transactions[transaction_id]

        if transaction.state == TransactionState.ABORTED:
            print(f"Transaction {transaction_id} aborted and rolled back.")
        else:
            print(f"Transaction {transaction_id} committed successfully.")
            transaction.state = TransactionState.COMMITTED
        with self.lock:
            del self.transactions[transaction_id]
