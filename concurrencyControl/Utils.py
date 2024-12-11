from enum import Enum
from typing import Optional
import uuid


class Action(Enum):
    READ = "Read"
    WRITE = "Write"


class TransactionPhase(Enum):
    GROWING = "Growing"
    SHRINKING = "Shrinking"


class Response:
    def __init__(self, allowed: bool, transaction_id: Optional[int] = None):
        self.allowed = allowed
        self.transaction_id = transaction_id

    def __repr__(self):
        return f"Response(allowed={self.allowed}, transaction_id={self.transaction_id})"


class TransactionState(Enum):
    ACTIVE = "Active"
    ABORTED = "Aborted"
    COMMITTED = "Committed"


class Transaction:
    def __init__(self, timestamp: int):
        self.id = uuid.uuid4().int
        self.state = TransactionState.ACTIVE
        self.timestamp = timestamp  # Timestamp saat transaction dibuat
        self.phase = TransactionPhase.GROWING  

class TransactionMVCC:
    def __init__(self, timestamp: int):
        self.timestamp = timestamp
        self.state = TransactionState.ACTIVE

class Row:
    def __init__(self, data: list[any], rows_count: int):
        self.data = data
        self.rows_count = rows_count
