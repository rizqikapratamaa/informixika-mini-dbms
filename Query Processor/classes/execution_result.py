from Storage_Manager.classes.rows import Rows
from datetime import datetime


class ExecutionResult:
    def __init__(
        self, transaction_id=None, timestamp=None, message="", data=None, query=""
    ) -> None:
        self.transaction_id = transaction_id
        self.timestamp = timestamp if timestamp is not None else datetime
        self.message: str = message
        self.data: Rows = data if data is not None else Rows()
        self.query: str = query

    def __str__(self) -> str:
        return (
            f"Transaction ID: {self.transaction_id}\n"
            f"Timestamp: {self.timestamp}\n"
            f"Message: {self.message}\n"
            f"Data: {self.data}\n"
            f"Query: {self.query}"
        )
