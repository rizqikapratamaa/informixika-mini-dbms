from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional
from datetime import datetime
from typing import List

class Action(Enum):
    READ = "READ"
    WRITE = "WRITE"

class Response(Enum):
    ALLOW = "ALLOW"
    ABORT = "ABORT"

@dataclass
class Table:
    nama: str
    read_timestamp: int = 0
    write_timestamp: int = 0

class TransactionStatus(Enum):
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"

@dataclass
class Transaction:
    id: int
    timestamp: int
    status: TransactionStatus
    accessed_tables: set

class TimestampBasedConcurrencyControl:
    def __init__(self):
        self.tables:List[Table] = []
        
    def _get_next_timestamp(self) -> int:
        """Generate next timestamp"""
        self._current_timestamp += 1
        return self._current_timestamp
        
    def _get_next_transaction_id(self) -> int:
        """Generate next transaction ID"""
        self._current_transaction_id += 1
        return self._current_transaction_id
