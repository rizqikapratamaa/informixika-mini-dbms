import dummy_rw as dr
import dummydb as db
import recovery as recover

# Init Database
data = dr.init_dummy_db()
print("\nDATA:", data)
db.activeDB = data

# Rollback
rollback = recover.rollback(transaction_id=2)

# Menulis Data Hasil Rollback
print("\nDATA:", db.activeDB)