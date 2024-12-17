from enum import Enum

# Define the ActionType enum
class ActionType(Enum):
    READ = 0
    WRITE = 1
    START = 2
    COMMIT = 3

    def __repr__(self):
        return self.name

class Action:
    def __init__(self, action: ActionType):
        self.action = action

    def __str__(self):
        return f"Action({self.action})" 

    def __repr__(self):
        return f"Action({self.action})" 
