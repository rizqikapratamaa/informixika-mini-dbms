from enum import Enum
from typing import List
from Concurrency_Control_Manager.classes.action import Action
from Concurrency_Control_Manager.classes.action import ActionType

class LockType(Enum):
    SHARED = 0
    EXC = 1

class LockEntry:
    def __init__(self, type:LockType, object:str, transaction_id:int):
        self.type:LockType = type
        self.object = object
        self.transaction_id = transaction_id

class LockBook:
    def __init__(self):
        self.Log: List[LockEntry] = []

    def getLockList(self) -> List[LockEntry]: 
        return self.Log
        
    def addEntry(self, object: str, transaction_id: int,action:Action):
        if (action.action == ActionType.WRITE):
            self.Log.append(LockEntry(LockType.EXC,object,transaction_id))
        else:
            self.Log.append(LockEntry(LockType.SHARED,object,transaction_id))

    def checkEntry(self, object: str, entry: LockEntry, action: Action) -> bool:
        if (entry.object==object and (entry.type == LockType.EXC or (entry.type==LockType.SHARED and action.action==ActionType.WRITE))):
            return False
        return True
    
    def checkAllEntries(self, object: str,action:Action):
        valid = True
        for entry in self.Log:
            if not self.checkEntry(object,entry,action):
                valid = False

        return valid
    
    def deleteLocksFromTransaction(self, transaction_id):
        i = 0
        while (i<len(self.Log)):
            if (self.Log[i].transaction_id==transaction_id):
                self.Log.pop(i)
            else:
                i+=1
