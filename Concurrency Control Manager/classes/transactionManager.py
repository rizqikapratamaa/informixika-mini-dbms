from time import time
from typing import Set, Dict
import math

class TransactionManager:
    def __init__(self):
        self.transactions = {}  # Stores all transaction metadata

    def create_transaction(self, transaction_id: int) -> int:
        self.transactions[transaction_id] = {
            "startTS": transaction_id,  # Start timestamp
            "validationTS": math.inf,  # Validation timestamp
            "finishTS": math.inf,  # Finish timestamp
            "read_set": set(),  # Set of data items read
            "write_set": set(),  # Set of data items written
        }
        return transaction_id

    def log_read(self, transaction_id: int, object_name: str):
        self.transactions[transaction_id]["read_set"].add(object_name)

    def log_write(self, transaction_id: int, object_name: str):
        self.transactions[transaction_id]["write_set"].add(object_name)

    def get_transaction(self, transaction_id: int) -> Dict:
        return self.transactions.get(transaction_id)

    def delete_transaction(self, transaction_id: int):
        if transaction_id in self.transactions:
            del self.transactions[transaction_id]
