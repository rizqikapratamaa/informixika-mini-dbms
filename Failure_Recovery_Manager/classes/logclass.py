from datetime import datetime

class log:
    def __init__(self, transaction_id, action, timestamp, old_data = None, new_data = None, table_name : str = None) -> None:
        self.transaction_id = transaction_id
        self.action = action
        self.timestamp = timestamp
        self.old_data = old_data
        self.new_data = new_data
        self.table_name = table_name

    def __str__(self) -> str:
        return (
            f"Transaction ID: {self.transaction_id}, "
            f"Action: {self.action}, "
            f"Timestamp: {self.timestamp}, "
            f"Old Data: {self.old_data if self.old_data else 'N/A'}, "
            f"New Data: {self.new_data if self.new_data else 'N/A'}, "
            f"Table: {self.table_name}"
        )

class actiontype(enumerate):
    start = 0
    write = 1
    commit = 2
    abort = 3
    

class RecoveryCriteria:
    def __init__(self,transaction_id: int = None, timestamp: datetime = None):
        if (transaction_id != None and timestamp != None):
            raise("Cannot use both timestamp and checkpoint!")
        self.transaction_id = transaction_id
        self.timestamp = timestamp