import dummydb as db
from logclass import log as logitem, actiontype as at
from datetime import datetime
import logger as l
import log_rw as rw

log : list[logitem] = []
def init_dummy_db():
    global log
    data = db.DummyDatabase()
    log += [logitem(1,at.start,datetime.now())]
    data.write(1,10)
    log += [logitem(1,at.write,datetime.now(),None,{"id":1,"value":10})]
    data.write(2,20)
    log += [logitem(1,at.write,datetime.now(),None,{"id":2,"value":20})]
    log += [logitem(1,at.commit,datetime.now())]
    data.write(1,30)
    data.write(2,40)
    log += [logitem(2,at.start,datetime.now())]
    log += [logitem(2,at.write,datetime.now(),{"id":1,"value":10},{"id":1,"value":30})]
    log += [logitem(2,at.write,datetime.now(),{"id":2,"value":20},{"id":2,"value":40})]
    return data

class dummy_iterator(rw.log_iterator):
    global log

    def __init__(self):
        self.log = log
        self.iterration = len(log) - 1
    
    def next(self):
        if self.iterration < 0:
            return None
        retval = self.log[self.iterration]
        self.iterration -= 1
        return retval