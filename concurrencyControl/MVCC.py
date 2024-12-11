import threading
from concurrencyControl.Utils import *
from concurrencyControl.CCManager import CCManager


class MVCC(CCManager):
    def __init__(self):
        self.object_versions = {}  # {'row_id': [{'version': int, 'w_ts': int, 'r_ts': int, 'data': any}]}
        self.transactions = {}
        self.current_timestamp = 0
        self.lock = threading.Lock()
        self.read_dependency = {}  # {'transaction_id': [list of dependent transaction_ids]}

    def find_latest_version(self, row_id, trans_ts):
        if row_id not in self.object_versions:
            return None
        for o in reversed(self.object_versions[row_id]):
            if o['w_ts'] <= trans_ts:
                return o
        return None

    def begin_transaction(self):
        with self.lock:
            self.current_timestamp += 1
            t = TransactionMVCC(self.current_timestamp)
            self.transactions[t.timestamp] = t
            self.read_dependency[t.timestamp] = []
        # print(f"Transaction {t.timestamp} started.")
        return t.timestamp

    def log_object(self, row, trans_timestamp):
        with self.lock:
            if trans_timestamp not in self.transactions:
                raise ValueError(f"Transaction {trans_timestamp} not found.")

            for record in row.data:
                row_id = hash(record)
                if row_id not in self.object_versions:
                    self.object_versions[row_id] = [{'version': 0, 'w_ts': 0, 'r_ts': 0}]
                    # print(f"Transaction {trans_timestamp} logged access to row {row_id}.")

    def validate_object(self, row, trans_timestamp, action):
        conflicting_records = []

        if trans_timestamp not in self.transactions:
            raise ValueError(f"Transaction {trans_timestamp} not found.")

        t = self.transactions[trans_timestamp]
        for record in row.data:
            row_id = hash(record)

            with self.lock:
                latest_version = self.find_latest_version(row_id, t.timestamp)

            if action == Action.READ:
                if not latest_version:
                    self.log_object(Row([record], 1), trans_timestamp)
                    continue

                if latest_version['r_ts'] < t.timestamp:
                    latest_version['r_ts'] = t.timestamp
                # print(f"Transaction {trans_timestamp} reads row {row_id} version {latest_version['version']}.")

                with self.lock:
                    writer_transaction = latest_version['w_ts']
                    if writer_transaction != 0:
                        self.read_dependency[writer_transaction].append(trans_timestamp)

            elif action == Action.WRITE:
                if latest_version and t.timestamp < latest_version['r_ts']:
                    conflicting_records.append(row_id)
                    break

                with self.lock:
                    new_version = {
                        'version': t.timestamp,
                        'w_ts': t.timestamp,
                        'r_ts': t.timestamp,
                    }
                    if row_id not in self.object_versions:
                        self.object_versions[row_id] = []
                    self.object_versions[row_id].append(new_version)
                    latest_version = new_version
                # print(f"Transaction {trans_timestamp} creates new version of row {row_id}.")
            print(f"TS({record}{latest_version['version']})=({latest_version['r_ts']},{latest_version['w_ts']})")

        if conflicting_records:
            print(f"Transaction {trans_timestamp} aborted due to conflicts on rows: {conflicting_records}.")
            self.abort_transaction(trans_timestamp)
            return Response(allowed=False, transaction_id=trans_timestamp)

        return Response(allowed=True, transaction_id=trans_timestamp)

    def abort_transaction(self, trans_timestamp):
        t = self.transactions[trans_timestamp]
        t.state = TransactionState.ABORTED
 
        for row_id, versions in self.object_versions.items():
            self.object_versions[row_id] = [v for v in versions if v['w_ts'] != trans_timestamp]

        dependent_transactions = self.read_dependency.get(trans_timestamp, [])
        for dep_trans in dependent_transactions:
            if self.transactions[dep_trans].state == TransactionState.ACTIVE:
                print(f"Transaction {dep_trans} aborted due to dependency on transaction {trans_timestamp}.")
                self.abort_transaction(dep_trans)

        self.read_dependency[trans_timestamp] = []

        # print(f"Transaction {trans_timestamp} aborted and rolled back.")

    def end_transaction(self, trans_timestamp: int):
        if trans_timestamp not in self.transactions:
            raise ValueError(f"Transaction {trans_timestamp} not found.")
        with self.lock:
            del self.transactions[trans_timestamp]



# Contoh Penggunaan
if __name__ == "__main__":
    tm_manager = MVCC()

    # Membuat beberapa Row untuk diuji
    w = Row(data=["w"], rows_count=1)
    x = Row(data=["x"], rows_count=1)
    y = Row(data=["y"], rows_count=1)
    z = Row(data=["z"], rows_count=1)

    # Memulai transaksi
    t1 = tm_manager.begin_transaction()
    t2 = tm_manager.begin_transaction()
    t3 = tm_manager.begin_transaction()
    t4 = tm_manager.begin_transaction()
    t5 = tm_manager.begin_transaction()

    # Transaksi 1 membaca row1

    
    tm_manager.log_object(x, t5)
    tm_manager.validate_object(x, t5, Action.READ)
    tm_manager.log_object(y, t2)
    tm_manager.validate_object(y, t2, Action.READ)
    tm_manager.log_object(y, t1)
    tm_manager.validate_object(y, t1, Action.READ)
    tm_manager.log_object(y, t3)
    tm_manager.validate_object(y, t3, Action.WRITE)
    tm_manager.log_object(z, t3)
    tm_manager.validate_object(z, t3, Action.WRITE)
    tm_manager.log_object(z, t5)
    tm_manager.validate_object(z, t5, Action.READ)
    tm_manager.log_object(z, t2)
    tm_manager.validate_object(z, t2, Action.READ)
    tm_manager.log_object(x, t1)
    tm_manager.validate_object(x, t1, Action.READ)
    tm_manager.log_object(w, t4)
    tm_manager.validate_object(w, t4, Action.READ)
    tm_manager.log_object(w, t3)
    tm_manager.validate_object(w, t3, Action.WRITE)
    # tm_manager.log_object(z, t5)
    # tm_manager.validate_object(z, t5, Action.WRITE)


    # Mengakhiri transaksi
    tm_manager.end_transaction(t1)
    tm_manager.end_transaction(t2)
    tm_manager.end_transaction(t3)
    tm_manager.end_transaction(t4)
    tm_manager.end_transaction(t5)