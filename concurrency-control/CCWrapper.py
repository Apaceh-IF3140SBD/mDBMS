from typing import Union
from CCManager import CCManager
from Timestamp import TimestampBasedConcurrencyControlManager
from TwoPL import TwoPhaseLockingConcurrencyControlManager
from MVCC import MVCC
from Utils import Action, Response, Row


class ConcurrencyControlWrapper:
    def __init__(self, algorithm: str = 'timestamp'):
        if algorithm == 'Timestamp':
            self.cc_manager: CCManager = TimestampBasedConcurrencyControlManager()
        elif algorithm == 'TwoPhaseLocking':
            self.cc_manager: CCManager = TwoPhaseLockingConcurrencyControlManager()
        elif algorithm == 'MVCC':
            self.cc_manager = MVCC()
        else:
            raise ValueError(
                f"Unsupported concurrency control algorithm: {algorithm}")

    def switch_algorithm(self, algorithm: str):

        if algorithm == 'Timestamp':
            self.cc_manager = TimestampBasedConcurrencyControlManager()
        elif algorithm == 'TwoPhaseLocking':
            self.cc_manager = TwoPhaseLockingConcurrencyControlManager()
        elif algorithm == 'MVCC':
            self.cc_manager = MVCC()
        else:
            raise ValueError(
                f"Unsupported concurrency control algorithm: {algorithm}")
        print(f"Switched to {algorithm} concurrency control.")

    def begin_transaction(self) -> int:
        return self.cc_manager.begin_transaction()

    def log_object(self, row: Row, transaction_id: int):
        self.cc_manager.log_object(row, transaction_id)

    def validate_object(self, row: Row, transaction_id: int, action: Action) -> Response:
        return self.cc_manager.validate_object(row, transaction_id, action)

    def end_transaction(self, transaction_id: int):
        self.cc_manager.end_transaction(transaction_id)
