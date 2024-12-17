from enum import Enum

class ResponseType(Enum):
    DIE = -1
    WAITING = 0
    ALLOWED = 1

class Response:
    def __init__(self, allowed: ResponseType, transaction_id: int):
        self.allowed = allowed
        self.transaction_id = transaction_id

    def __str__(self):
        return f"Response: {self.allowed}, Transaction ID: {self.transaction_id}"
    
    def __repr__(self):
        return f"Response: {self.allowed}, Transaction ID: {self.transaction_id}"
        
    