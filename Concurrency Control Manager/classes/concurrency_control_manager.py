from typing import List, Dict, Set
from Concurrency_Control_Manager.classes.action import ActionType,Action
from Concurrency_Control_Manager.classes.response import Response,ResponseType
from Concurrency_Control_Manager.classes.lockbook import LockBook, LockEntry
from Concurrency_Control_Manager.classes.transactionManager import TransactionManager
from Concurrency_Control_Manager.classes.timestampManager import TimestampBasedConcurrencyControl,Table
from enum import Enum
from time import time
from Failure_Recovery_Manager.classes.failure_recovery_manager import Failure_recovery_manager as frm, failure_recovery_manager as _frm, actiontype as AT, RecoveryCriteria

"""
TODO:
1. Response Type (done)
2. Convert to Wait-Die (done)
3. Unit Testing
4. Rollback Call Function
5. Integration to Query Processor
"""

class ConcurrencyMechanism(Enum):
    LOCK_BASED = 0
    TIMESTAMP_BASED = 1
    MULTI_VERSION = 2
    VALIDATION_BASED = 3

from datetime import datetime

    
class ConcurrencyControlManager:
    # Singleton attributes
    _instance = None
    # initialized = False
    
    # Lock handler attributes
    lockHandler = LockBook()

    # Transaction manager attributes
    lastTransactionID = 0
    transactionManager = TransactionManager()
    
    # Failure Recovery Manager
    FRM : _frm = frm.enable()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # Prevent reinitialization
        if not hasattr(self, 'initialized'):  
            self.lockHandler = LockBook()
            self.lastTransactionID = 0
            self.transactionManager = TransactionManager()
            self.timestampManager = TimestampBasedConcurrencyControl()
            self.mechanism = ConcurrencyMechanism.LOCK_BASED #Default
            self.initialized = True
            self.FRM = frm.enable()

    def set_mechanism(self, mechanism: ConcurrencyMechanism) -> None:
        if not isinstance(mechanism, ConcurrencyMechanism):
            raise ValueError("Invalid concurrency mechanism")
        self.mechanism = mechanism

    def get_instance(self):
        return self._instance

    def begin_transaction(self) -> int:
        self.lastTransactionID += 1
        trans_id = self.lastTransactionID
        
        self.transactionManager.create_transaction(trans_id)
        # self.timestampManager.begin_transaction(self.lastTransactionID)
        # act: Action = Action(ActionType.START, self.lastTransactionID)
        # frm.get_instance().write_log(self.lastTransactionID, act, datetime.now())
        self.FRM.write_log_stamp(trans_id, AT.start)
        return trans_id

    def log_object(self, object: str, transaction_id: int, action: Action) -> None:
        if(self.mechanism == ConcurrencyMechanism.LOCK_BASED):
            self._lock_based_log_object(object, transaction_id, action)
        elif self.mechanism == ConcurrencyMechanism.VALIDATION_BASED:
            self._occ_log_object(object, transaction_id, action)


        # Continue for other mechanism
    def _occ_log_object(self, object: str, transaction_id: int, action: Action) -> None:
        # Validation Based Protocol
        if transaction_id not in self.transactionManager.transactions:
            raise ValueError("Invalid transaction ID")
        if action.action == ActionType.READ:
            self.transactionManager.log_read(transaction_id, object)
        elif action.action == ActionType.WRITE:
            self.transactionManager.log_write(transaction_id,object)

    def _lock_based_log_object(self, object: str, transaction_id: int, action: Action) -> None:
        # Lock Based Protocol 
        self.lockHandler.addEntry(object, transaction_id, action)


    def validate_object(self, object: str, transaction_id: int, action: Action) -> Response:
        if self.mechanism == ConcurrencyMechanism.LOCK_BASED:
            return self.validate_lock(object, transaction_id, action)
        elif self.mechanism == ConcurrencyMechanism.VALIDATION_BASED:
            return self.validate_object_optimistic(object, transaction_id, action)
        elif self.mechanism == ConcurrencyMechanism.TIMESTAMP_BASED:
            return self.validate_timeStamp(object,transaction_id,action)
            
    def validate_lock(self, object: str, transaction_id: int, action: Action) -> Response:
        # Get list of locks
        lockList: List[LockEntry] = self.lockHandler.getLockList()

        # Check if there are conflicting locks
        for entry in lockList:
            # Handle deadlocks using wound-wait algorithm
            if not self.lockHandler.checkEntry(object, entry, action):
                if entry.transaction_id == transaction_id:
                    continue
                elif entry.transaction_id < transaction_id:
                    # self.handleRollback(transaction_id)
                    self.handleRollback(transaction_id)
                    return Response(ResponseType.DIE,transaction_id)
                else: # entry.transaction_id > transaction_id
                    # busyWait(self.lockHandler.checkEntry(object, entry, action))
                    # return self.validate_object(object, transaction_id, action)
                    return Response(ResponseType.WAITING, transaction_id)
                
        # No conflicting locks
        self.log_object(object,transaction_id,action)
        return Response(ResponseType.ALLOWED, transaction_id)
    
    def validate_object_optimistic(self, object: str, transaction_id: int, action: Action) -> Response:
        transaction = self.transactionManager.get_transaction(transaction_id)
        transaction["validationTS"] = time()  # Set validation timestamp

        # Iterate through all other transactions
        for tid, tinfo in self.transactionManager.transactions.items():
            if tid == transaction_id:  # Skip self
                continue

            if tinfo["validationTS"] < transaction["startTS"]:
                # Rule 1: Ti finishes before Tj starts
                if tinfo["finishTS"] is None or tinfo["finishTS"] >= transaction["startTS"]:
                    self.handleRollback(transaction_id)
                    return Response(ResponseType.DIE, transaction_id)  # Validation fails
            
            elif (
                tinfo["startTS"] < transaction["startTS"]
                and tinfo["finishTS"] < transaction["validationTS"]
                and not transaction["read_set"].isdisjoint(tinfo["write_set"])
            ):
                # Rule 2: Concurrent execution, and conflict in read/write
                self.handleRollback(transaction_id)
                return Response(ResponseType.DIE, transaction_id)  # Validation fails
        self.log_object(object,transaction_id,action)
        return Response(ResponseType.ALLOWED, transaction_id)  # Validation succeeds

    
    def validate_timeStamp(self, object:str, transaction_id: int, action: Action) -> Response:
        isthere = False
        for i in range(0,len(self.timestampManager.tables)):
            if (self.timestampManager.tables[i].nama==object):
                isthere = True
        if (not isthere):
            self.timestampManager.tables.append(Table(object,0,0))
        
        for table in self.timestampManager.tables:
            if object == table.nama:
                # print(self.timestampManager.tables)
                # print(f"ID: {transaction_id} / Action: {action} / Object: {object} / Table: {self.timestampManager.tables}")
                if action.action == ActionType.READ:
                    if transaction_id < table.write_timestamp:
                        self.handleRollback(transaction_id)
                        return Response(ResponseType.DIE,transaction_id)

                    else :
                        table.read_timestamp = max(transaction_id,table.read_timestamp)
                        return Response(ResponseType.ALLOWED, transaction_id)
                else:
                    if ((transaction_id < table.write_timestamp) or (transaction_id < table.read_timestamp)):
                        self.handleRollback(transaction_id)
                        return Response(ResponseType.DIE,transaction_id)
                    else:
                        table.write_timestamp = max(transaction_id,table.write_timestamp)
                        return Response(ResponseType.ALLOWED, transaction_id)

    def end_transaction(self, transaction_id: int) -> None:
        self.lockHandler.deleteLocksFromTransaction(transaction_id) 
        self.transactionManager.delete_transaction(transaction_id)
        
        # Log to failure recovery manager
        self.FRM.write_log_stamp(transaction_id, AT.commit)

        # Create action for failure recovery manager
        # act: Action = Action(ActionType.COMMIT, transaction_id)
        # FailureRecoveryManager.get_instance().write_log(transaction_id, act, datetime.now())

    def handleRollback(self, transaction_id: int) -> None:
        self.lockHandler.deleteLocksFromTransaction(transaction_id)
        self.transactionManager.delete_transaction(transaction_id)
        
        # Send rollback to failure recovery manager
        self.FRM.recover(RecoveryCriteria(transaction_id))