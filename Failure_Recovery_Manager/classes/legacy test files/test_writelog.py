import logger as l
import log_rw as rw

print("start")

rw.write_log(l.create_log(1,l.at.start))
rw.write_log(l.create_log(1,l.at.write,None,{"id" : 100,"value" : 2000 }))
rw.write_log(l.create_log(1,l.at.commit))

print("bru")