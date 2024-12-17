import log_rw as l
from logger import create_log
from logclass import actiontype as at
import time

#using data from dummy_rw
l.write_log(create_log(1,at.start,None,None))
l.write_log(create_log(1,at.write,None,{"id":1,"value":10}))
l.write_log(create_log(1,at.write,None,{"id":2,"value":20}))
l.write_log(create_log(1,at.commit,None,None))

l.write_log(create_log(2,at.start,None,None))
l.write_log(create_log(2,at.write,{"id":1,"value":10},{"id":1,"value":30}))
l.write_log(create_log(2,at.write,{"id":2,"value":20},{"id":2,"value":40}))