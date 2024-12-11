from concurrencyControl.Utils import *
from concurrencyControl.CCManager import CCManager
import threading


class TwoPhaseLockingConcurrencyControlManager(CCManager):
    def __init__(self):
        self.exclusive_lock_table: dict[int, int] = {} 
        self.shared_lock_table: dict[int, set[int]] = {} 
        self.transactions: dict[int, dict] = {} 
        self.current_transaction_id = 0
        self.lock = threading.Lock()
        self.wait_for: dict[int, set[int]] = {} 

    def begin_transaction(self) -> int:
        with self.lock:
            self.current_transaction_id += 1 # ID ++ untuk membuat transaction baru
            transaction_id = self.current_transaction_id
            self.transactions[transaction_id] = {
                'state': TransactionState.ACTIVE,
                'phase': 'growing',
                'locked_records': set()  
            }
        
        print(f"Transaction {transaction_id} started.")
        return transaction_id

    def log_object(self, row: Row, transaction_id: int):
        with self.lock:
            if transaction_id not in self.transactions:
                raise ValueError(f"Transaction {transaction_id} not found.")

            for record in row.data:
                record_id = hash(record)
                print(f"Transaction {transaction_id} logged access to record {record_id}.")

    def validate_object(self, row: Row, transaction_id: int, action: Action) -> Response:
        with self.lock:
            if transaction_id not in self.transactions:
                raise ValueError(f"Transaction {transaction_id} not found.")

            transaction = self.transactions[transaction_id]
            
            # If transaction is already aborted
            if transaction['state'] == TransactionState.ABORTED:
                print(f"Transaction {transaction_id} is already aborted.")
                return Response(allowed=False, transaction_id=transaction_id)

            # If transaction is in shrinking phase
            if transaction['phase'] == TransactionPhase.SHRINKING:
                print(f"Transaction {transaction_id} aborted due to lock request in shrinking phase.")
                self._abort_transaction(transaction_id)
                return Response(allowed=False, transaction_id=transaction_id)

            # Try to acquire appropriate locks based on action
            success = True
            acquired_locks = []
            
            for record in row.data:
                record_id = hash(record)
                
                if action == Action.READ:
                    if not self._acquire_shared_lock(transaction_id, record_id):
                        print(f"Transaction {transaction_id} failed to acquire READ lock on record {record_id}.")
                        success = False
                        break
                    acquired_locks.append(('shared', record_id))
                
                elif action == Action.WRITE:
                    if not self._acquire_exclusive_lock(transaction_id, record_id):
                        print(f"Transaction {transaction_id} failed to acquire WRITE lock on record {record_id}.")
                        success = False
                        break
                    acquired_locks.append(('exclusive', record_id))

            # If any lock acquisition failed, release acquired locks
            if not success:
                for lock_type, record_id in acquired_locks:
                    if lock_type == 'shared':
                        self._release_shared_lock(transaction_id, record_id)
                    else:
                        self._release_exclusive_lock(transaction_id, record_id)
                return Response(allowed=False, transaction_id=transaction_id)

            print(f"Transaction {transaction_id} allowed {action.value} on all records in the Row.")
            return Response(allowed=True, transaction_id=transaction_id)

    def _acquire_shared_lock(self, transaction_id: int, record_id: int) -> bool:
        # Cek apakah transaction sudah memiliki lock pada record
        if record_id in self.transactions[transaction_id]['locked_records']:
            return True
        
        # Cek konflik lock X (transaction lain memiliki exclusive lock)
        if record_id in self.exclusive_lock_table and self.exclusive_lock_table[record_id] != transaction_id:
            if self._detect_deadlock(transaction_id, self.exclusive_lock_table[record_id]):
                self._abort_transaction(transaction_id)
                return False
            else:
                print(f"Waiting for exclusive lock to be released on record {record_id}")
                return False

        # Grant shared lock
        if record_id not in self.shared_lock_table:
            self.shared_lock_table[record_id] = set()
        self.shared_lock_table[record_id].add(transaction_id)
        self.transactions[transaction_id]['locked_records'].add(record_id)
        print(f"Granted shared lock on record {record_id} to transaction {transaction_id}")
        return True

    def _acquire_exclusive_lock(self, transaction_id: int, record_id: int) -> bool:
        # Cek apakah transaction sudah memiliki exclusive lock pada record
        if record_id in self.exclusive_lock_table and self.exclusive_lock_table[record_id] == transaction_id:
            return True
        
        # Kalau transaction uda memiliki exclusive lock pada record
        if (record_id in self.exclusive_lock_table and 
            self.exclusive_lock_table[record_id] == transaction_id):
            return True
                
        # Cek konflik lock X (transaction lain memiliki lock pada record)
        if (record_id in self.exclusive_lock_table or 
            (record_id in self.shared_lock_table and 
            (len(self.shared_lock_table[record_id]) > 1 or 
            (len(self.shared_lock_table[record_id]) == 1 and 
            transaction_id not in self.shared_lock_table[record_id])))):
            # Check for deadlock
            current_owner = None
            if record_id in self.exclusive_lock_table:
                current_owner = self.exclusive_lock_table[record_id]
            elif record_id in self.shared_lock_table:
                other_owners = [t for t in self.shared_lock_table[record_id] if t != transaction_id]
                if other_owners:
                    current_owner = other_owners[0]

            if current_owner and self._detect_deadlock(transaction_id, current_owner):
                self._abort_transaction(transaction_id)
                print(f"Transaction {transaction_id} aborted due to deadlock.")
                return False
            else:
                print(f"Waiting for locks to be released on record {record_id}")
                return False

        # Upgrade lock kalau transaction uda memiliki shared lock
        if (record_id in self.shared_lock_table and 
            transaction_id in self.shared_lock_table[record_id]):
            self.shared_lock_table[record_id].remove(transaction_id)
            if not self.shared_lock_table[record_id]:
                del self.shared_lock_table[record_id]
            print(f"Upgraded lock to exclusive for transaction {transaction_id} on record {record_id}")

        # Grant exclusive lock
        self.exclusive_lock_table[record_id] = transaction_id
        self.transactions[transaction_id]['locked_records'].add(record_id)
        print(f"Granted exclusive lock on record {record_id} to transaction {transaction_id}")
        return True

    def _detect_deadlock(self, waiting_transaction_id: int, holding_transaction_id: int) -> bool:
        if waiting_transaction_id not in self.wait_for:
            self.wait_for[waiting_transaction_id] = set()
        
        self.wait_for[waiting_transaction_id].add(holding_transaction_id)
        
        visited = set()
        def has_cycle(current_id: int) -> bool:
            if current_id in visited:
                return current_id == waiting_transaction_id
            visited.add(current_id)
            if current_id in self.wait_for:
                for next_id in self.wait_for[current_id]:
                    if has_cycle(next_id):
                        return True
            return False
        
        has_deadlock = has_cycle(holding_transaction_id)
        
        if has_deadlock:
            self.wait_for[waiting_transaction_id].remove(holding_transaction_id)
            if not self.wait_for[waiting_transaction_id]:
                del self.wait_for[waiting_transaction_id]
        
        return has_deadlock
    
    def _abort_transaction(self, transaction_id: int):
        transaction = self.transactions[transaction_id]
        transaction['state'] = TransactionState.ABORTED
        print(f"Transaction {transaction_id} aborted.")
        self._release_all_locks(transaction_id)

    def _release_shared_lock(self, transaction_id: int, record_id: int):
        if record_id in self.shared_lock_table:
            self.shared_lock_table[record_id].discard(transaction_id)
            if not self.shared_lock_table[record_id]:
                del self.shared_lock_table[record_id]
            self.transactions[transaction_id]['locked_records'].discard(record_id)

    def _release_exclusive_lock(self, transaction_id: int, record_id: int):
        if record_id in self.exclusive_lock_table and self.exclusive_lock_table[record_id] == transaction_id:
            del self.exclusive_lock_table[record_id]
            self.transactions[transaction_id]['locked_records'].discard(record_id)

    def _release_all_locks(self, transaction_id: int):
        # Release exclusive locks
        for record_id, lock_holder in list(self.exclusive_lock_table.items()):
            if lock_holder == transaction_id:
                self._release_exclusive_lock(transaction_id, record_id)

        # Release shared locks
        for record_id, holders in list(self.shared_lock_table.items()):
            if transaction_id in holders:
                self._release_shared_lock(transaction_id, record_id)

        # Clear wait-for
        if transaction_id in self.wait_for:
            del self.wait_for[transaction_id]
        for t_id in list(self.wait_for.keys()):
            if transaction_id in self.wait_for[t_id]:
                self.wait_for[t_id].remove(transaction_id)
                if not self.wait_for[t_id]:
                    del self.wait_for[t_id]

    def end_transaction(self, transaction_id: int):
        with self.lock:
            if transaction_id not in self.transactions:
                raise ValueError(f"Transaction {transaction_id} not found.")

            transaction = self.transactions[transaction_id]
            transaction['phase'] = TransactionPhase.SHRINKING
            
            if transaction['state'] == TransactionState.ABORTED:
                print(f"Transaction {transaction_id} was already aborted.")
            else:
                transaction['state'] = TransactionState.COMMITTED
                print(f"Transaction {transaction_id} committed successfully.")
            
            self._release_all_locks(transaction_id)
            del self.transactions[transaction_id]