from typing import Union
import sys
from copy import deepcopy
from Utils.component_logger import log_frm 

class table:
    class table_type(enumerate):
        data = 1
        index = 2
    
    def __init__(self,name : str,data : list[dict], type : table_type = table_type.data):
        self.name : str = name
        self.data : list[dict] = data
        self.type : table.table_type = type
    
    def __eq__(self,other : Union[str , any]):
        if type(other) == type("string"):
            if other == self.name:
                return True
        elif type(other) == type(self):
            if other.data == self.data:
                return True
        else:
            return False
    
class buffer:
    def __init__(self, max_size_bytes = 134217728):
        # default max 128 MB (The actual buffer may be higher but it's to not let it overflow)
        self.tables : list[table] = []
        self.current_size = 0
        self.max_size_bytes = max_size_bytes
        
    def insert_table(self, table : table):
        for loadedTable in self.tables:
            if table.name == loadedTable.name:
                loadedTable.data = deepcopy(table.data)
                log_frm("Override buffer")
                return
        self.tables.append(deepcopy(table))
        log_frm("Insert buffer")
        return
    
    def get_table(self, table_name : str) -> table | None:
        for loadedTable in self.tables:
            if loadedTable.name == table_name:
                return deepcopy(loadedTable)
        return None
    
    def _table_id(self, table_name : str) -> int:
        for i in range(len(self.tables)):
            if self.tables[i].name == table_name:
                return i
        return None
        
    def checkpoint(self):
        for table in self.tables:
            pass #write to disk
        # Clear disk nya sebagian aja, mana aja yang udah gak perlu, specifically udah 2 kali checkpoint gk ke akses
        self.tables.clear()
        log_frm("Clear buffer")

    def _calculate_table_size(self, table: table) -> int:
        return sys.getsizeof(table) + sum(sys.getsizeof(row) for row in table.data)
    
    def _should_save_checkpoint(self, incomingTable = None):
        if (incomingTable != None):
            if self.current_size + self._calculate_table_size(incomingTable) > self.max_size_bytes:
                self.checkpoint()
                self.current_size = 0
        else :
            if self.current_size > self.max_size_bytes:
                self.checkpoint()
                self.current_size = 0
