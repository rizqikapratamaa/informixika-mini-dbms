from typing import List, Literal, Union, Dict

class Condition:
    def __init__(self, column: str, operation: Literal['=', '<>', '>', '>=', '<', '<='], operand: Union[str, int]):
        self.column = column
        self.operation = operation
        self.operand = operand

class DataDeletion:
    def __init__(self, table: str, conditions: List[Condition]):
        self.table = table
        self.conditions = conditions

class DataRetrieval:
    def __init__(self, table: str, column: List[str], conditions: List[Condition]):
        self.table = table
        self.column = column
        self.conditions = conditions

class DataWrite:
    def __init__(self, table: str, column: List[str], conditions: List[Condition], new_value: None):
        self.table = table
        self.column = column
        self.conditions = conditions
        self.new_value = new_value

class Statistic:
    def __init__(self, n_r: int, b_r: int, l_r: int, f_r: int, V_a_r: Dict[str, int]):
        self.n_r = n_r
        self.b_r = b_r
        self.l_r = l_r
        self.f_r = f_r
        self.V_a_r = V_a_r