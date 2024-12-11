from datetime import datetime
from typing import Optional

class RecoverCriteria:
    def __init__(self, timestamp: Optional[datetime] = None, transaction_id: Optional[int] = None):
        self.timestamp = timestamp
        self.transaction_id = transaction_id
