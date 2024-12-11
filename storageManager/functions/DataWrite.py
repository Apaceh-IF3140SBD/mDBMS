from typing import List
from storageManager.functions.Condition import Condition

class DataWrite:
    def __init__(self, table, columns, new_value, conditions: List[Condition]):
        self.table = table
        self.columns = columns
        self.new_value = new_value
        self.conditions = conditions

    def new_value_tuple_format(self):
        return tuple(self.new_value)