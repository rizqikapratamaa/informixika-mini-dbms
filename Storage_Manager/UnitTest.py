import unittest, os
from classes.storage_engine import *
from classes.utils import *
from classes.serializer import *
from classes.rows import *

class TestStorageEngine(unittest.TestCase):
    def setUp(self):
        self.serializer = Serializer()
        self.storage_engine = StorageEngine(self.serializer)

    def test_read_block(self):
        self.serializer.block_size = 1024

        data_retrieval = DataRetrieval(
        "products", ["nama", "harga"], [Condition("product_id", ">", 5)])
        result = self.storage_engine.read_block(data_retrieval)
     
        self.assertEqual(result.rows_count, 65)

    def test_write_block_with_data(self):
        self.serializer.block_size = 1024

        data_write = DataWrite("users", ["umur"], [Condition("user_id", "=", 1)], [2, "*", "(", "umur", "+", 2, ")"])
        count = self.storage_engine.write_block(data_write)
        result = count.rows_count

        self.assertEqual(result, 1)


    def test_read_spesific_block(self):
        self.serializer.block_size = 1024

        data_retrieval = DataRetrieval(
        "products", ["nama", "harga"], [Condition("nama", "=", "V60")])
        block_to_read = [0]

        result = self.storage_engine.read_spesific_block(data_retrieval, block_to_read)
        count = len(result.data)

        self.assertEqual(count,2)

    def test_write_spesific_block(self):
        self.serializer.block_size = 1024

        data_write = DataWrite("products", ["harga"], [Condition("nama", "=", "Donat")], 2000)
        block_to_write = [0]

        count = self.storage_engine.write_specific_block(data_write, block_to_write)
        result = count.rows_count

        self.assertEqual(result, 0)

    def test_set_index(self):
        self.serializer.block_size = 1024

        table = "orders"
        column = "product_id"

        self.storage_engine.set_index(table, column)

        success = 0

        if os.path.exists(f"data/{table}_{column}_hash.dat"):
            success = 1

        self.assertEqual(success, 1)

    def test_get_stats(self):
        self.serializer.block_size = 1024

        table = "orders"

        result = self.storage_engine.get_stats(table)

        self.assertEqual(result.n_r, 69)
        self.assertEqual(result.b_r, 2)
        self.assertEqual(result.l_r, 20)
        self.assertEqual(result.f_r, 51)
        self.assertEqual(result.V_a_r, {'order_id': 69, 'user_id': 40, 'product_id': 49, 'quantity': 10, 'total_harga': 69})


if __name__ == "__main__":
    unittest.main()