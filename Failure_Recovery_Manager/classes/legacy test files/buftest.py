import buffer as b

buff = b.buffer()

buff.insert_table(b.table("table1",{"overrid":False}))
buff.insert_table(b.table("table1",{"overrid":True}))