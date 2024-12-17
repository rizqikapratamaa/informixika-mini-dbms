import logger as l
import log_rw as rw
from datetime import datetime

liter = rw.log_iterator(datetime.now())
print(liter.next())
print(liter.next())
print(liter.next())
print(liter.next())