from threading import Thread
from concurrency_control_manager import ConcurrencyControlManager, ConcurrencyMechanism
from action import Action, ActionType
from response import Response, ResponseType
import time
from threading import Thread
from random import randint

""" 
class ActionType(Enum):
    READ = 0
    WRITE = 1
    START = 2
    COMMIT = 3

class ResponseType(Enum):
    DIE = -1
    WAITING = 0
    ALLOWED = 1

def validate_lock(self, object: str, transaction_id: int, action: Action) -> Response:
"""
# Define color variables for ANSI escape codes
RESET = "\033[0m"      # Reset color
BLACK = "\033[30m"     # Black text
RED = "\033[31m"       # Red text
GREEN = "\033[32m"     # Green text
YELLOW = "\033[33m"    # Yellow text
BLUE = "\033[34m"      # Blue text
MAGENTA = "\033[35m"   # Magenta text
CYAN = "\033[36m"      # Cyan text
WHITE = "\033[37m"     # White text

COLOR_LIST = [GREEN, YELLOW, BLUE, MAGENTA, GREEN]

# query prosesor
class TesterUnit():
    def __init__(self,input_file):
        self.manager = ConcurrencyControlManager()
        self.access_list_need = []
        self.transaction_id = -1
        self.input_file =input_file

    def parse_input_file(self):
        """Parse the input file to generate a list of actions."""
        read_result = []
        with open(self.input_file, 'r') as file:
            for line in file:
                line = line.strip()  # Remove leading/trailing whitespace
                if not line:
                    continue  # Skip empty lines

                action_type, obj = line[0], line[2:-1]  # Extract action type and object
                if action_type == 'R':
                    action = Action(ActionType.READ)
                    action_name = "READ"
                elif action_type == 'W':
                    action = Action(ActionType.WRITE)
                    action_name = "WRITE"
                else:
                    print(f"Unknown action: {line}")
                    continue

                read_result.append([obj, self.transaction_id, action, action_name])
        return read_result
    
    def beginTransaction(self):
        self.transaction_id = self.manager.begin_transaction()
        print(f"Transaction {self.transaction_id} has begun...")
        
    def read_Transaction_access(self,file_path):
        # read from txt file the parse like this example
        # read_result = [['A',self.transaction_id,Action(ActionType.START),"START"],
        #                ['A',self.transaction_id,Action(ActionType.READ),"READ"],
        #             #    more...
        #                ]
        read_result = self.parse_input_file()
        # print("Transaction", self.transaction_id)
        # print(read_result)
        for access in read_result:
            time.sleep(randint(1, 9) * 0.1)
            if(self.manager.mechanism == ConcurrencyMechanism.VALIDATION_BASED):
                self.manager.log_object(access[0],access[1],access[2])
                print(f"{COLOR_LIST[self.transaction_id]} Transaction {self.transaction_id}: {GREEN} [TEMP {access[3]}] on table {access[0]} | READ SET: {self.manager.transactionManager.transactions[self.transaction_id]['read_set']} WRITE SET: {self.manager.transactionManager.transactions[self.transaction_id]['write_set']} {RESET}")
            else:
                validate_val = self.manager.validate_object(access[0],access[1],access[2]).allowed
                count = 0
                while True:
                    if (validate_val==ResponseType.ALLOWED):
                        print(f"{COLOR_LIST[self.transaction_id]} Transaction {self.transaction_id}: {GREEN} Granted access [{access[3]}] on table {access[0]} {RESET}")
                        break
                    elif (validate_val==ResponseType.WAITING):
                        print(f"{COLOR_LIST[self.transaction_id]} Transaction {self.transaction_id}: {CYAN} Waiting for access [{access[3]}] on table {access[0]} {RESET}")
                        time.sleep(0.2)
                    else:
                        print(f"{COLOR_LIST[self.transaction_id]} Transaction {self.transaction_id}: {RED} Failed to access [{access[3]}] on table {access[0]} {RESET}")
                        print(f"{COLOR_LIST[self.transaction_id]} Transaction {self.transaction_id}: {RED} Calling Rollback {RESET}")
                        return
                    
                    validate_val = self.manager.validate_object(access[0],access[1],access[2]).allowed
                    count += 1
        if(self.manager.mechanism == ConcurrencyMechanism.VALIDATION_BASED):
            validate_val = self.manager.validate_object(access[0],access[1],access[2]).allowed
            if (validate_val==ResponseType.ALLOWED):
                print(f"Transaction {self.transaction_id}: VALIDATED")
                print(f"Transaction {self.transaction_id}: Success Running Query!")
            else:
                    print(f"{COLOR_LIST[self.transaction_id]} Transaction {self.transaction_id}: {RED} Failed to validate {RESET}")
                    print(f"{COLOR_LIST[self.transaction_id]} Transaction {self.transaction_id}: {RED} Calling Rollback {RESET}")
            return
        print(f"Transaction {self.transaction_id}: Success Running Query!")
        return

    def start(self):
        self.manager.set_mechanism(ConcurrencyMechanism.VALIDATION_BASED)
        self.beginTransaction()
        self.read_Transaction_access(self.input_file)
        self.manager.end_transaction(self.transaction_id)
"""
 Implement the testing 
 input file e.g:

 R(A) ->READ object 'A'
 W(B) -> WRITE object 'B'
 ...


"""
# Define the test function for each thread
def test_thread(input_file):
    tester = TesterUnit(input_file)
    tester.start()
    
# Input file simulation (normally, this would be read from a file)
input_file_1 = "input1.txt"
input_file_2 = "input2.txt"
input_file_3 = "input3.txt"
input_file_4 = "input4.txt"

# Create two threads simulating two concurrent transactions
thread1 = Thread(target=test_thread, args=(input_file_1,))
thread2 = Thread(target=test_thread, args=(input_file_2,))
thread3 = Thread(target=test_thread, args=(input_file_3,))
thread4 = Thread(target=test_thread, args=(input_file_4,))

# Start both threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()

# Wait for both threads to complete
thread1.join()
thread2.join()
thread3.join()
thread4.join()