from .logclass import actiontype as at, log
from datetime import datetime
from json import dumps, loads

def create_log(transaction_id : int, action : at, old_data = None, new_data = None, table_name : str = None):
    log_entry = {
        "transaction_id" : transaction_id,
        "action" : action,
        "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "old_data" : old_data,
        "new_data" : new_data,
        "table_name" : table_name,
        "message" : None
    }

    if action is at.start:
        log_entry["message"] = f"Transaction {transaction_id} started"
    
    elif action is at.write:
        if new_data is None:
            raise ValueError("There are no values beaing exchanged")
        log_entry["message"] = f"Transaction {transaction_id} written"
    
    elif action is at.commit:
        log_entry["message"] = f"Transaction {transaction_id} committed"
    
    elif action is at.abort:
        log_entry["message"] = f"Transaction {transaction_id} aborted"

    return dumps(log_entry)

def translate(log_string : str):
    log_json = loads(log_string)

    log_item = log(log_json["transaction_id"], log_json["action"], log_json["timestamp"], log_json["old_data"], log_json["new_data"], log_json["table_name"])

    return log_item

if __name__ == "__main__":
    data = "Apapapapapap \n hklahkhakfh"
    a = create_log(1, at.start, data)

    b = translate(a)

    print(b.transaction_id, b.action, b.timestamp, b.old_data, b.new_data)