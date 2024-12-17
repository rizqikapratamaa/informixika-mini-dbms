#import dummy_rw as dr
from . import log_rw as rw
from .logger import translate, create_log
from datetime import datetime
from json import loads
from .logclass import log as logitem, actiontype as at
# from .failure_recovery_manager import Failure_recovery_manager as frm
from Storage_Manager.classes.rows import Rows
from .buffer import buffer


# If you really think about it, ini sama/mirip kayak update_finder
def buf_write(table_name: str, target_data: list[dict], data_to_write: list[dict], buf : buffer):
    class mode(enumerate):
        REPLACE = 0 #IF TARGET_DATA is NOT EMPTY AND DATA_TO_WRITE is NOT EMPTY 
        INSERT = 1 #IF TARGET_DATA is EMPTY
        DELETE = 2 #IF DATA_TO_WRITE is EMPTY
    
    opmode : mode
    if (len(target_data) > 0 and len(data_to_write) > 0):
        opmode = mode.REPLACE
    elif (len(target_data) == 0 and len(data_to_write) > 0):
        opmode = mode.INSERT
    elif (len(target_data) > 0 and len(data_to_write) == 0):
        opmode = mode.DELETE
    else:
        raise("Target data and Data to write is empty!")
    
    table : list[dict]
    for loadedTable in buf.tables:
            if loadedTable.name == table_name:
                table = loadedTable.data
                break
    
    target_itr = 0
    write_itr = 0
    
    assert(opmode == mode.REPLACE or opmode == mode.INSERT or opmode == mode.DELETE)
    while (target_itr < len(target_data) and write_itr < len(data_to_write)):
        #for now do the expensive way
        if (opmode == mode.INSERT): #hackjob
            table += data_to_write
            write_itr == len(data_to_write)
        
        for i in range(len(table)):
            if opmode == mode.REPLACE or opmode == mode.DELETE: #when target_data is NOT empty
                if table[i] == target_data[target_itr]:
                    if opmode == mode.REPLACE: #replace target_data with data_to_write
                        table[i] = data_to_write[write_itr]
                        write_itr += 1 #move write itr
                    else: #opmode == mode.DELETE
                        table.pop(i)
                    target_itr += 1
            if (target_itr == len(target_data) and write_itr == len(data_to_write)):
                break
            #elif opmode == mode.INSERT:
                #mungkin ada criterianya masukin dimana, cari kolom mana yang sorted data, and then continue from there
                #for the time being, ill just do a hack job trick
                #break
        
                    
        #sampe dua-duanya kosong

#ONLY REVERT
def recover(timestamp : str = None, transaction_id : int = None, buf : buffer = None):
    #itterator = dr.dummy_iterator()
    assert(buf != None)
    itterator = rw.log_iterator(datetime.now())
    log_history : list[logitem] = []

    #ini cuman meng-undo specified timestamp ternyata lmao

    end : bool = False
    while not end:
        ll = translate(itterator.next())
        if ll.transaction_id == transaction_id:
            if ll.action != at.start:
                log_history += [ll]
            else:
                end = True
            print(ll)

    for item in log_history:
        if item.action is at.write: #undo write // DELETE NEW DATA
            buf_write(item.table_name,item.new_data,item.old_data,buf)
            
    rw.write_log(create_log(transaction_id,at.abort))
    
def rollback(timestamp : str = None, transaction_id : int = None):
    print("Not implemented lmao")

    #MUNDUR this is untested btw i just typed shit in a blind rage
def rollback(timestamp : str = None, transaction_id : int = None):
    #itterator = dr.dummy_iterator()
    itterator = rw.log_iterator(datetime.now())
    log_history : list[logitem] = []

    #jika menemukan start yang gak ada commit nya, jangan lupa log abort(?) or i guess transaction_id yang di abort
    blacklist = []
    whitelist = []
    end : bool = False
    while not end:
        ll = translate(itterator.next())
        if ll.transaction_id in blacklist:
            continue
        if ll.action == at.abort:
            blacklist += [ll.transaction_id]
        elif ll.action == at.commit:
            whitelist += [ll.transaction_id]
        elif ll.action == at.start:
            if ll.transaction_id in whitelist:
                whitelist.remove(ll.transaction_id)
            if ll.transaction_id in blacklist:
                blacklist.remove(ll.transaction_id)
        else:
            log_history += [ll]
        print(ll)
        
        if (ll.transaction_id != transaction_id or ll.action != 0):
            end = True

    for item in log_history:
        if item.action is at.write: #undo write // DELETE NEW DATA
            buf_write(item.table_name,item.new_data,item.old_data)
    
