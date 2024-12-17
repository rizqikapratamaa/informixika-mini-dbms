import os
import pickle
from Storage_Manager.classes.utils import (
    Condition,
    DataDeletion,
    DataRetrieval,
    DataWrite,
)
from Storage_Manager.classes.rows import Rows
from Storage_Manager.classes.serializer import Serializer
from Storage_Manager.classes.hash_index import HashIndex
from Storage_Manager.classes.statistic import Stats
from Storage_Manager.classes.utils import Statistic
from typing import List, Union
from Failure_Recovery_Manager.classes.failure_recovery_manager import (
    Failure_recovery_manager as frm,
)
from Utils.component_logger import log_sm


class StorageEngine:
    def __init__(self, serializer: Serializer) -> None:
        self.serializer = serializer
        self.__frm_inst = frm.enable()
        frm.set_checkpoint_routine(self.save_buffer_to_disk)
        
    def isFloat(self, value) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def read_block(self, data_retrieval: DataRetrieval) -> Rows:
        """
        Read block data from the hard disk based on the DataRetrieval object and return it in Rows format.
        Args:
            data_retrieval (DataRetrieval): Object that contains information about the table, columns, and conditions.

        Returns:
            Rows: Rows object containing the read data.
        """
        hash_indexed_table = []

        try:
            with open(
                f"Storage_Manager/data_demo_lowercase/{data_retrieval.table[0]}_schema.dat",
                "rb",
            ) as f:
                schema = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File schema for {data_retrieval.table} table is not found"
            )

        if len(data_retrieval.conditions) > 0:
            for i in range(len(data_retrieval.conditions)):
                hash_path = f"Storage_Manager/data_demo_lowercase/{data_retrieval.table[0]}_{data_retrieval.conditions[i].column}_hash.dat"
                if os.path.exists(hash_path):
                    hash_indexed_table.append(hash_path)

        skema = self.serializer.deserialize_schema(schema)
        offset = 0
        temp = Rows()
        try:
            with open(
                f"Storage_Manager/data_demo_lowercase/{data_retrieval.table[0]}.dat",
                "rb",
            ) as f:
                # membaca data dari file .dat dengan nama column yang ada pada data_retrieval.column
                f.seek(offset)
                binary_data = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File data table for {data_retrieval.table} table is not found"
            )

        rows_data = self.serializer.deserialize_with_blocks(
            binary_data, skema["columns"]
        )

        if len(rows_data) == 0:
            return 0

        # mencari semua column yang ada di rows_data
        columns = set()
        for row in rows_data:
            columns.update(row.keys())

        # mengurangi columns dengan kolom yang ada pada data_retrieval.column, jadi yang tersisa hanya column yang tidak ada pada data_retrieval.column
        columns = list(columns - set(data_retrieval.column))

        # column index
        column_index = []
        if len(hash_indexed_table) > 0:
            log_sm("Using hash index...")
            for hash_path in hash_indexed_table:
                with open(hash_path, "rb") as f:
                    hash_index = pickle.load(f)
                column_index = hash_index.get(data_retrieval.conditions[i].operand)

        if len(column_index) > 0:
            for index in column_index:
                if self._matches_conditions(
                    rows_data[index], data_retrieval.conditions
                ):
                    if (
                        len(data_retrieval.column) == 1
                        and data_retrieval.column[0] == "*"
                    ):
                        temp.data.append(rows_data[index])
                        temp.idx.append(index)
                    else:
                        # mengurangi rows_data[i] dengan column yang ada pada columns
                        for col in columns:
                            del rows_data[index][col]
                        temp.data.append(rows_data[index])
                        temp.idx.append(index)

        else:
            # mengubah data menjadi list of Rows
            for i in range(len(rows_data)):
                # mengecek apakah data sudah sesuai dengan kondisi dan hanya mengambil column yang ada pada data_retrieval.column
                if self._matches_conditions(rows_data[i], data_retrieval.conditions):
                    if (
                        len(data_retrieval.column) == 1
                        and data_retrieval.column[0] == "*"
                    ):
                        temp.data.append(rows_data[i])
                        temp.idx.append(i)
                    else:
                        # mengurangi rows_data[i] dengan column yang ada pada columns
                        for col in columns:
                            del rows_data[i][col]
                        temp.data.append(rows_data[i])
                        temp.idx.append(i)

        temp.rows_count = len(temp.data)

        log_sm(
            "Reading column "
            + ", ".join(map(str, data_retrieval.column))
            + " from table ",
            data_retrieval.table,
        )

        return temp

    def buffered_read_block(self, data_retrieval: DataRetrieval) -> Rows:
        """
        Read block data from the hard disk based on the DataRetrieval object and return it in Rows format.
        Args:
            data_retrieval (DataRetrieval): Object that contains information about the table, columns, and conditions.

        Returns:
            Rows: Rows object containing the read data.
        """
        hash_indexed_table = []
        rows_data = self.__frm_inst.table_from_buffer(data_retrieval.table[0])
        temp = Rows()
        if rows_data == None:
            try:
                with open(
                    f"Storage_Manager/data_demo_lowercase/{data_retrieval.table[0]}_schema.dat",
                    "rb",
                ) as f:
                    schema = f.read()
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"File schema for {data_retrieval.table} table is not found"
                )

            if len(data_retrieval.conditions) > 0:
                for i in range(len(data_retrieval.conditions)):
                    hash_path = f"Storage_Manager/data_demo_lowercase/{data_retrieval.table[0]}_{data_retrieval.conditions[i].column}_hash.dat"
                    if os.path.exists(hash_path):
                        hash_indexed_table.append(hash_path)

            skema = self.serializer.deserialize_schema(schema)
            offset = 0
            try:
                with open(
                    f"Storage_Manager/data_demo_lowercase/{data_retrieval.table[0]}.dat",
                    "rb",
                ) as f:
                    # membaca data dari file .dat dengan nama column yang ada pada data_retrieval.column
                    f.seek(offset)
                    binary_data = f.read()
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"File data table for {data_retrieval.table} table is not found"
                )

            rows_data = self.serializer.deserialize_with_blocks(
                binary_data, skema["columns"]
            )

            if len(rows_data) == 0:
                return 0

            self.__frm_inst.send_table_to_buffer(data_retrieval.table[0], rows_data)

        # mencari semua column yang ada di rows_data
        columns = set()
        for row in rows_data:
            columns.update(row.keys())

        # mengurangi columns dengan kolom yang ada pada data_retrieval.column, jadi yang tersisa hanya column yang tidak ada pada data_retrieval.column
        columns = list(columns - set(data_retrieval.column))

        # column index
        column_index = []
        if len(hash_indexed_table) > 0:
            log_sm("Using hash index...")
            for hash_path in hash_indexed_table:
                with open(hash_path, "rb") as f:
                    hash_index = pickle.load(f)
                column_index = hash_index.get(data_retrieval.conditions[i].operand)

        if len(column_index) > 0:
            for index in column_index:
                if self._matches_conditions(
                    rows_data[index], data_retrieval.conditions
                ):
                    if (
                        len(data_retrieval.column) == 1
                        and data_retrieval.column[0] == "*"
                    ):
                        temp.data.append(rows_data[index])
                        temp.idx.append(index)
                    else:
                        # mengurangi rows_data[i] dengan column yang ada pada columns
                        for col in columns:
                            del rows_data[index][col]
                        temp.data.append(rows_data[index])
                        temp.idx.append(index)

        else:
            # mengubah data menjadi list of Rows
            for i in range(len(rows_data)):
                # mengecek apakah data sudah sesuai dengan kondisi dan hanya mengambil column yang ada pada data_retrieval.column
                if self._matches_conditions(rows_data[i], data_retrieval.conditions):
                    if (
                        len(data_retrieval.column) == 1
                        and data_retrieval.column[0] == "*"
                    ):
                        temp.data.append(rows_data[i])
                        temp.idx.append(i)
                    else:
                        # mengurangi rows_data[i] dengan column yang ada pada columns
                        for col in columns:
                            del rows_data[i][col]
                        temp.data.append(rows_data[i])
                        temp.idx.append(i)

        temp.rows_count = len(temp.data)

        log_sm(
            "Reading column "
            + ", ".join(map(str, data_retrieval.column))
            + " from table ",
            data_retrieval.table,
        )

        return temp

    def _matches_conditions(self, row: dict, conditions: List[Condition]) -> bool:
        """
        Checks whether a row satisfies all given conditions.

        Args:
            row (dict): Data row to be checked.
            conditions (List[Condition]): List of conditions to be satisfied.

        Returns:
            bool: True if the row satisfies all conditions, False otherwise.
        """
        for condition in conditions:
            column_value = row.get(condition.column)
            if column_value is None:
                return False  # kolom tidak ada dalam baris
            if not self._evaluate_condition(column_value, condition):
                return False
        return True

    def _evaluate_condition(self, value: Union[str, int], condition: Condition) -> bool:
        """
        Evaluates whether a value satisfies a given condition.

        Args:
            value (Union[str, int]): Value to be evaluated.
            condition (Condition): Condition to be applied.

        Returns:
            bool: True if the value satisfies the condition, False otherwise.
        """

        if condition.operation == "=":
            return value == condition.operand
        elif condition.operation == "<>":
            return value != condition.operand
        elif condition.operation == ">":
            return value > condition.operand
        elif condition.operation == ">=":
            return value >= condition.operand
        elif condition.operation == "<":
            return value < condition.operand
        elif condition.operation == "<=":
            return value <= condition.operand
        else:
            raise ValueError(f"Operation is not known: {condition.operation}")

    def buffered_write_block(self, data_write: DataWrite) -> Rows:
        log_sm(
            str(data_write.table)
            + " "
            + str(data_write.conditions)
            + " "
            + str(data_write.new_value)
            + " "
            + str(data_write.column)
        )

        rows_data = self.__frm_inst.table_from_buffer(data_write.table)
        # rows_data = None
        # membuka schema.dat dengan nama yang sama dengan data_write.table
        with open(
            f"Storage_Manager/data_demo_lowercase/{data_write.table}_schema.dat", "rb"
        ) as f:
            schema = f.read()

        skema = self.serializer.deserialize_schema(schema)
        if rows_data is None:  # kosong
            log_sm("NONE BRO 1")

            offset = 0
            with open(
                f"Storage_Manager/data_demo_lowercase/{data_write.table}.dat", "rb"
            ) as f:
                f.seek(offset)
                binary_data = f.read()

            rows_data = self.serializer.deserialize_with_blocks(
                binary_data, skema["columns"]
            )

            if len(rows_data) == 0:
                return 0

            self.__frm_inst.send_table_to_buffer(data_write.table, rows_data)

        # mengambil data yang akan diubah
        temp = Rows()
        for i in range(len(rows_data)):
            if isinstance(data_write.new_value, list):
                operasi = ""
                for j in data_write.new_value:
                    # mengecek apakah i merupakan int atau str
                    if isinstance(j, int):
                        operasi += str(j)
                    else:
                        # mencari apakah i merupakan salah satu column yang ada pada rows_data[i]
                        if j in rows_data[i]:
                            operasi += str(rows_data[i][j])
                        else:
                            operasi += str(j)

                tempNewValue = int(eval(operasi))

                if (
                    self._matches_conditions(rows_data[i], data_write.conditions)
                    and isinstance(tempNewValue, int)
                    and isinstance(rows_data[i][data_write.column[0]], int)
                ):
                    rows_data[i][data_write.column[0]] = tempNewValue
                    temp.data.append(rows_data[i])
                    temp.idx.append(i)
            else:
                for j in skema["columns"]:
                    if j["name"] == data_write.column[0]:
                        tipe = j["type"]
                        break
                
                type_map = {
                    "int": int,
                    "float": float,
                    "varchar": str,
                    "char": str,
                }
                
                type_rill = type_map.get(tipe)
                
                if self.isFloat(data_write.new_value):
                    tempNewValue = float(data_write.new_value)
                
                if type_rill == int and isinstance(tempNewValue, float):
                    tempNewValue = int(tempNewValue)
                elif type_rill == float and isinstance(tempNewValue, int):
                    tempNewValue = float(tempNewValue)
                
                if self._matches_conditions(
                    rows_data[i], data_write.conditions
                ) and isinstance(tempNewValue, type_rill):
                    rows_data[i][data_write.column[0]] = tempNewValue
                    temp.data.append(rows_data[i])
                    temp.idx.append(i)

        temp.rows_count = len(temp.data)

        return temp

    def buffered_delete_block(self, data_deletion: DataDeletion) -> Rows:
        rows_data = self.__frm_inst.table_from_buffer([data_deletion.table[0]])
        if rows_data is None:  # kosong
            log_sm("NONE BRO 1")
            # membuka schema.dat dengan nama yang sama dengan data_write.table
            with open(
                f"Storage_Manager/data_demo_lowercase/{data_deletion.table[0]}_schema.dat",
                "rb",
            ) as f:
                schema = f.read()

            skema = self.serializer.deserialize_schema(schema)

            offset = 0
            with open(
                f"Storage_Manager/data_demo_lowercase/{data_deletion.table[0]}.dat",
                "rb",
            ) as f:
                f.seek(offset)
                binary_data = f.read()

            rows_data = self.serializer.deserialize_with_blocks(
                binary_data, skema["columns"]
            )

            if len(rows_data) == 0:
                return 0

            # self.__frm_inst.send_table_to_buffer(data_write.table[0], rows_data)

        temp = Rows()
        for i in range(len(rows_data)):
            if self._matches_conditions(rows_data[i], data_deletion.conditions):
                temp.data.append(rows_data[i])
                temp.idx.append(i)
        temp.rows_count = len(temp.data)

        return temp

    def save_buffer_to_disk(self):
        buffer = self.__frm_inst.buf.tables
        for table in buffer:
            # membuka schema.dat dengan nama yang sama dengan data_write.table
            with open(
                f"Storage_Manager/data_demo_lowercase/{table.name}_schema.dat", "rb"
            ) as f:
                schema = f.read()
            skema = self.serializer.deserialize_schema(schema)

            # menulis data yang baru ke dalam file
            tes = {"columns": skema["columns"]}
            binary_data = self.serializer.serialize_with_blocks(table.data, tes)
            with open(
                f"Storage_Manager/data_demo_lowercase/{table.name}.dat", "wb"
            ) as f:
                f.write(binary_data)

    def write_block(self, data_write: DataWrite) -> Rows:
        """
        Read block data from the hard disk based on the DataRetrieval object and return it in Rows format.
        Args:
            data_write (DataWrite): Object that contains information about the table, columns, conditions, and also new_value.

        Returns:
            int: integer that is the number of rows that are updated according to data_write.conditions
        """
        # membuka schema.dat dengan nama yang sama dengan data_write.table
        with open(
            f"Storage_Manager/data_demo_lowercase/{data_write.table}_schema.dat", "rb"
        ) as f:
            schema = f.read()

        skema = self.serializer.deserialize_schema(schema)

        offset = 0
        with open(
            f"Storage_Manager/data_demo_lowercase/{data_write.table}.dat", "rb"
        ) as f:
            f.seek(offset)
            binary_data = f.read()

        rows_data = self.serializer.deserialize_with_blocks(
            binary_data, skema["columns"]
        )

        if len(rows_data) == 0:
            return Rows()

        # mengambil data yang akan diubah
        temp = Rows()
        for i in range(len(rows_data)):
            if isinstance(data_write.new_value, list):
                operasi = ""
                for j in data_write.new_value:
                    # mengecek apakah i merupakan int atau str
                    if isinstance(j, int):
                        operasi += str(j)
                    else:
                        # mencari apakah i merupakan salah satu column yang ada pada rows_data[i]
                        if j in rows_data[i]:
                            operasi += str(rows_data[i][j])
                        else:
                            operasi += str(j)

                tempNewValue = eval(operasi)

                if (
                    self._matches_conditions(rows_data[i], data_write.conditions)
                    and isinstance(tempNewValue, int)
                    and isinstance(rows_data[i][data_write.column[0]], int)
                ):
                    rows_data[i][data_write.column[0]] = tempNewValue
                    temp.data.append(rows_data[i])
                    temp.idx.append(i)
            else:
                for j in skema["columns"]:
                    if j["name"] == data_write.column[0]:
                        tipe = j["type"]
                        break

                type_mapping = {
                    "int": int,
                    "float": float,
                    "varchar": str,
                    "char": str,
                }

                type_rill = type_mapping.get(tipe)

                if self._matches_conditions(
                    rows_data[i], data_write.conditions
                ) and isinstance(data_write.new_value, type_rill):
                    rows_data[i][data_write.column[0]] = data_write.new_value
                    temp.data.append(rows_data[i])
                    temp.idx.append(i)

        temp.rows_count = len(temp.data)

        tes = {"columns": skema["columns"]}
        binary_data = self.serializer.serialize_with_blocks(temp.data, tes)
        with open(
            f"Storage_Manager/data_demo_lowercase/{data_write.table}.dat", "wb"
        ) as f:
            f.write(binary_data)

        return temp

    def write_specific_block(self, data_write: DataWrite, block_index: int) -> int:
        """
        Write specific block data to the hard disk based on the DataWrite object and block index.
        Args:
            data_write (DataWrite): Object that contains information about the table, columns, conditions, and new_value.
            block_index (int): The index of the block to write.

        Returns:
            int: The number of rows that are updated according to data_write.conditions.
        """
        with open(
            f"Storage_Manager/data_demo_lowercase/{data_write.table}_schema.dat", "rb"
        ) as f:
            schema = f.read()

        skema = self.serializer.deserialize_schema(schema)

        with open(
            f"Storage_Manager/data_demo_lowercase/{data_write.table}.dat", "rb"
        ) as f:
            binary_data = f.read()

        rows_data = self.serializer.deserialize_with_blocks(
            binary_data, skema["columns"]
        )

        if len(rows_data) == 0:
            return 0

        temp = Rows()
        for i in range(len(rows_data)):
            if i == block_index:
                if isinstance(data_write.new_value, list):
                    operasi = ""
                    for j in data_write.new_value:
                        if isinstance(j, int):
                            operasi += str(j)
                        else:
                            if j in rows_data[i]:
                                operasi += str(rows_data[i][j])
                            else:
                                operasi += str(j)

                    tempNewValue = eval(operasi)

                    if self._matches_conditions(rows_data[i], data_write.conditions):
                        rows_data[i] = tempNewValue
                        temp.data.append(rows_data[i])
                        temp.idx.append(i)

        temp.rows_count = len(temp.data)

        binary_data = self.serializer.serialize_with_blocks(temp.data, skema["columns"])
        with open(
            f"Storage_Manager/data_demo_lowercase/{data_write.table}.dat", "wb"
        ) as f:
            f.write(binary_data)

        return temp.rows_count

    def delete_block(self, data_deletion: DataDeletion) -> int:
        try:
            with open(
                f"Storage_Manager/data_demo_lowercase/{data_deletion.table}_schema.dat",
                "rb",
            ) as f:
                schema = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File schema for {data_deletion.table} table is not found"
            )

        skema = self.serializer.deserialize_schema(schema)

        offset = 0

        try:
            with open(
                f"Storage_Manager/data_demo_lowercase/{data_deletion.table}.dat", "rb"
            ) as f:
                f.seek(offset)
                binary_data = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File data table for {data_deletion.table} table is not found"
            )

        rows_data = self.serializer.deserialize_with_blocks(
            binary_data, skema["columns"]
        )

        if len(rows_data) == 0:
            return 0

        count = 0
        temp = Rows()
        for i in range(len(rows_data)):
            if self._matches_conditions(rows_data[i], data_deletion.conditions):
                # skip data yang akan dihapus
                count += 1
            else:
                temp.data.append(rows_data[i])
                temp.idx.append(i)
        temp.rows_count = len(temp.data)

        binary_data = self.serializer.serialize_with_blocks(temp.data, skema["columns"])

        try:
            with open(
                f"Storage_Manager/data_demo_lowercase/{data_deletion.table}.dat", "wb"
            ) as f:
                f.write(binary_data)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File data table for {data_deletion.table} table is not found"
            )

        return count

    def delete_specific_block(
        self, data_deletion: DataDeletion, block_index: int
    ) -> int:
        """
        Delete specific block data from the hard disk based on the DataDeletion object and block index.
        Args:
            data_deletion (DataDeletion): Object that contains information about the table, columns, and conditions.
            block_index (int): The index of the block to delete.

        Returns:
            int: The number of rows that are deleted according to data_deletion.conditions.
        """
        with open(
            f"Storage_Manager/data_demo_lowercase/{data_deletion.table}_schema.dat",
            "rb",
        ) as f:
            schema = f.read()

        skema = self.serializer.deserialize_schema(schema)

        with open(
            f"Storage_Manager/data_demo_lowercase/{data_deletion.table}.dat", "rb"
        ) as f:
            binary_data = f.read()

        rows_data = self.serializer.deserialize_with_blocks(
            binary_data, skema["columns"]
        )

        if len(rows_data) == 0:
            return 0

        temp = Rows()
        count = 0
        for i in range(len(rows_data)):
            if i == block_index:
                if self._matches_conditions(rows_data[i], data_deletion.conditions):
                    count += 1
                else:
                    temp.data.append(rows_data[i])
                    temp.idx.append(i)

        temp.rows_count = len(temp.data)

        tes = {"columns": skema["columns"]}
        binary_data = self.serializer.serialize_with_blocks(temp.data, tes)
        with open(
            f"Storage_Manager/data_demo_lowercase/{data_deletion.table}.dat", "wb"
        ) as f:
            f.write(binary_data)

        return count

    def set_index(self, table: str, column: str, index_type: str = "hash") -> None:
        """
        Create an index on a specific column in a table.
        Args:
            table (str): Table name to be indexed.
            column (str): Column name to be indexed.
            index_type (str): Type of index to be used. Choices: "hash", "btree".
        """
        if index_type.lower() not in ["b+tree", "hash"]:
            raise ValueError("index_type should be b+tree or hash")

        index_file = (
            f"Storage_Manager/data_demo_lowercase/{table}_{column}_{index_type}.dat"
        )

        data_file = f"Storage_Manager/data_demo_lowercase/{table}.dat"

        schema_file = f"Storage_Manager/data_demo_lowercase/{table}_schema.dat"

        if not os.path.exists(data_file):
            raise FileNotFoundError(f"File data table for {table} table is not found")

        if not os.path.exists(schema_file):
            raise FileNotFoundError(
                f"File {column} schema for {table} table is not found"
            )

        with open(data_file, "rb") as f:
            binary_data = f.read()

        with open(schema_file, "rb") as f:
            schema = f.read()

        skema = self.serializer.deserialize_schema(schema)

        rows_data = self.serializer.deserialize_with_blocks(
            binary_data, skema["columns"]
        )

        if index_type == "hash":
            log_sm(f"Creating hash index for column '{column}' on table '{table}'...")
            hash_index = HashIndex()
            for i, row in enumerate(rows_data):
                hash_index.insert(row[column], i)
            hash_index.save(index_file)
            log_sm(f"Hash index is successfully saved on {index_file}.")

        return None

    def set_index_on_buffer(
        self, table: str, column: str, index_type: str = "hash"
    ) -> None:
        if index_type.lower() not in ["b+tree", "hash"]:
            raise ValueError("index_type should be b+tree or hash")

        if table not in self.__frm_inst.buf.tables:
            raise ValueError("Table is not in buffer")

        rows_data = self.__frm_inst.get_table_data(table)
        hash_index = HashIndex()
        for i, row in enumerate(rows_data):
            hash_index.insert(row[column], i)
        self.__frm_inst.send_table_to_buffer(
            f"{table}_{column}_index", hash_index.index
        )

    def get_stats(self, table) -> Statistic:
        stat_builder = Stats(self.serializer)
        stats = stat_builder.get_stats(table)
        return stats

    def read_spesific_block(
        self, data_retrieval: DataRetrieval, blocks: List[int]
    ) -> Rows:
        """
        Read specific blocks from the hard disk based on the DataRetrieval object and block numbers.
        Args:
            data_retrieval (DataRetrieval): Object that contains information about the table, columns, and conditions.
            blocks (List[int]): List of block numbers to read.

        Returns:
            Rows: Rows object containing the read data.
        """
        block_size = 1024  # Block size in bytes
        hash_indexed_table = []

        try:
            with open(f"data/{data_retrieval.table}_schema.dat", "rb") as f:
                schema = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File schema for {data_retrieval.table} table is not found"
            )

        skema = self.serializer.deserialize_schema(schema)

        temp = Rows()
        try:
            with open(f"data/{data_retrieval.table}.dat", "rb") as f:
                # Read only specified blocks
                for block_num in blocks:
                    f.seek(block_num * block_size)
                    block_data = f.read(block_size)
                    if not block_data:  # End of file
                        break

                    # Deserialize block data
                    rows_in_block = self.serializer.deserialize_with_blocks(
                        block_data, skema["columns"]
                    )

                    # Filter columns and conditions
                    columns_to_remove = (
                        set(rows_in_block[0].keys()) - set(data_retrieval.column)
                        if rows_in_block
                        else set()
                    )

                    for i in range(len(rows_in_block)):
                        row = rows_in_block[i]
                        if self._matches_conditions(row, data_retrieval.conditions):
                            if (
                                len(data_retrieval.column) == 1
                                and data_retrieval.column[0] == "*"
                            ):
                                temp.data.append(row)
                            else:
                                # Remove unnecessary columns
                                filtered_row = row.copy()
                                for col in columns_to_remove:
                                    del filtered_row[col]
                                temp.data.append(filtered_row)

        except FileNotFoundError:
            raise FileNotFoundError(
                f"File data table for {data_retrieval.table} table is not found"
            )

        temp.rows_count = len(temp.data)
        return temp
