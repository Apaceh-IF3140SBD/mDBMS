from typing import Literal, Union

class Condition:
    def __init__(self, column: str, operation: Literal['=', '<>', '>', '>=', '<', '<='], operand: Union[int, str]):
        self.column = column
        self.operation = operation
        self.operand = operand
