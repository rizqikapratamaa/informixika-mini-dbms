from Storage_Manager.classes.utils import Statistic
from Storage_Manager.classes.serializer import Serializer
import os


class Stats:
    def __init__(self, serializer: Serializer) -> None:
        self.serializer = serializer

    def get_stats(self, table: str) -> Statistic:
        block_size = 1024

        schema_file = f"data/{table}_schema.dat"
        data_file = f"data/{table}.dat"

        if not os.path.exists(schema_file) or not os.path.exists(data_file):
            raise FileNotFoundError(f"Schema or data file for table {table} not found.")

        with open(schema_file, "rb") as f:
            schema = f.read()

        skema = self.serializer.deserialize_schema(schema)

        # lr: size of tuple of r.
        tuple_size = sum(
            col.get("length", 4) if col["type"] in ["varchar", "char"] else 4
            for col in skema["columns"]
        )

        # fr: blocking factor of r – i.e., the number of tuples of r that fit into one block.
        blocking_factor = block_size // tuple_size

        with open(data_file, "rb") as f:
            binary_data = f.read()

        # nr: number of tuples in a relation r.
        rows_data = self.serializer.deserialize_with_blocks(
            binary_data, skema["columns"]
        )
        num_tuples = len(rows_data)

        # br: number of blocks containing tuples of r.
        num_blocks = (num_tuples + blocking_factor - 1) // blocking_factor

        # V(A,r): number of distinct values that appear in r for attribute A; same as the size of A(r)
        distinct_values = {}
        for col in skema["columns"]:
            col_name = col["name"]
            distinct_values[col_name] = len(set(row[col_name] for row in rows_data))

        return Statistic(
            n_r=num_tuples,
            b_r=num_blocks,
            l_r=tuple_size,
            f_r=blocking_factor,
            V_a_r=distinct_values,
        )
