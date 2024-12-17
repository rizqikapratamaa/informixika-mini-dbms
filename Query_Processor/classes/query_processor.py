from Query_Optimizer.classes.query_object import ParsedQuery, QueryTree
from Query_Optimizer.classes.optimization_engine import OptimizationEngine
from Storage_Manager.classes.rows import Rows
from Storage_Manager.classes.storage_engine import StorageEngine
from Storage_Manager.classes.serializer import Serializer
from Storage_Manager.classes.utils import DataRetrieval, Condition, DataWrite
from Concurrency_Control_Manager.classes.concurrency_control_manager import (
    ConcurrencyControlManager,
    ConcurrencyMechanism,
)
from Concurrency_Control_Manager.classes.action import Action, ActionType
import time
from Concurrency_Control_Manager.classes.response import ResponseType
from Query_Processor.classes.execution_result import ExecutionResult
from Failure_Recovery_Manager.classes.failure_recovery_manager import (
    Failure_recovery_manager as frm,
)
from Utils.component_logger import (
    log_ccm,
    log_qp,
    GREEN,
    YELLOW,
    BLUE,
    MAGENTA,
    RED,
    RESET,
    CYAN,
)

COLOR_LIST = [GREEN, YELLOW, BLUE, MAGENTA, GREEN]
PROTOCOL_LIST = [
    ConcurrencyMechanism.LOCK_BASED,
    ConcurrencyMechanism.TIMESTAMP_BASED,
    ConcurrencyMechanism.VALIDATION_BASED,
]


class QueryProcessor:
    concurency_mechanism = 0

    def __init__(self) -> None:
        self.query_optimizer = OptimizationEngine()
        self.storage_engine = StorageEngine(Serializer())
        self.concurrency_manager = ConcurrencyControlManager()
        self.transaction_id = -1
        self.multiple_transaction = False
        self.result_storage = []
        self.query_storage = []
        self.__frm_inst = frm.enable()
        self.enable_sleep = False

    def execute_query(self, query: str) -> ExecutionResult:
        query = query.strip()
        if query.lower() == "sleep;":
            self.enable_sleep = not self.enable_sleep
            message = (
                "Sleep mode is enabled"
                if self.enable_sleep
                else "Sleep mode is disabled"
            )
            return [ExecutionResult(message=message)]

        if query.lower() == "protocol;":
            QueryProcessor.concurency_mechanism = (
                QueryProcessor.concurency_mechanism + 1
            ) % 3
            self.concurrency_manager.set_mechanism(
                PROTOCOL_LIST[QueryProcessor.concurency_mechanism]
            )
            message = "Concurrency Protocol is changed to " + str(
                PROTOCOL_LIST[QueryProcessor.concurency_mechanism]
            )
            return [ExecutionResult(message=message)]

        if query.lower() == "begin transaction;":
            self.BEGIN_TRANSACTION()
            self.multiple_transaction = True
            return

        if query.lower() == "commit;":
            try:
                for query in self.query_storage:
                    if not query.endswith(";"):
                        raise ValueError("Query must end with a semicolon")

                    parsed_query: ParsedQuery = self.query_optimizer.parse_query(query)
                    result = self._process_node(parsed_query.query_tree)

                    execution_result = ExecutionResult(
                        data=result,
                        message="Query executed successfully",
                        query=query,
                        transaction_id=self.transaction_id,
                    )

                    if query.split(" ")[0].lower() == "update":
                        self.__frm_inst.write_log(result)

                    self.result_storage.append(execution_result)
            except Exception as e:
                return [ExecutionResult(message=str(e), query=query)]
            finally:
                self.COMMIT()

            final_result = self.result_storage.copy()

            self.result_storage.clear()
            return final_result

        if self.multiple_transaction:
            self.query_storage.append(query)
            return

        if not query.endswith(";"):
            raise ValueError("Query must end with a semicolon")

        parsed_query: ParsedQuery = self.query_optimizer.parse_query(query)
        log_qp(parsed_query.query_tree)
        self.BEGIN_TRANSACTION()
        try:
            result = self._process_node(parsed_query.query_tree)
        except Exception as e:
            return [ExecutionResult(message=str(e), query=query)]

        execution_result = ExecutionResult(
            data=result,
            message="Query executed successfully",
            query=query,
            transaction_id=self.transaction_id,
        )

        if query.split(" ")[0].lower() == "update":
            self.__frm_inst.write_log(execution_result)

        self.COMMIT()
        return [execution_result]

    def _process_node(self, node: QueryTree) -> Rows:
        result = None
        if node.type == "PROJECTION":
            result = self.SELECT(self._process_node(node.children[0]), node.value)
        elif node.type == "RELATION":
            result = self.FROM(node)
        elif node.type == "SELECTION_STMT":
            result = self._process_selection_stmt(node)
        elif node.type == "ORDER BY":
            result = self.ORDER_BY(
                self._process_node(node.children[0]), node.value, node.operand
            )
        elif node.type == "LIMIT":
            result = self.LIMIT(self._process_node(node.children[0]), node.value)
        elif node.type == "JOIN":
            left_result = self._process_node(node.children[0])
            right_result = self._process_node(node.children[1])
            result = self.JOIN(left_result, right_result, node.value)
        elif node.type == "CROSS":
            result = self.CARTESIAN(node)
        elif node.type in ["AND", "OR"]:
            result = self._process_logical_condition(node)
        elif node.type == "UPDATE":
            result = self.UPDATE(node.children[0])
        elif node.type == "INSERT":
            result = self.INSERT(node.children[0])
        else:
            result = Rows()
        return result

    def apply_condition(self, rows: Rows, condition_node: QueryTree) -> Rows:
        if rows.rows_count == 0:
            return rows

        rows = rows.data

        if condition_node.type == "SELECTION":
            attr = condition_node.attr.lower()
            op = condition_node.operand
            val = condition_node.value

            if isinstance(rows[0][attr], (int, float)):
                val = float(val)

            if op == "=":
                filtered_rows = [row for row in rows if row[attr] == val]
                current_rows = Rows(filtered_rows, len(filtered_rows))
                if condition_node.children:
                    return self.apply_condition(
                        current_rows, condition_node.children[0]
                    )
                return current_rows
            elif op == "<":
                filtered_rows = [row for row in rows if row[attr] < val]
                current_rows = Rows(filtered_rows, len(filtered_rows))
                if condition_node.children:
                    return self.apply_condition(
                        current_rows, condition_node.children[0]
                    )
                return current_rows
            elif op == ">":
                filtered_rows = [row for row in rows if row[attr] > val]
                current_rows = Rows(filtered_rows, len(filtered_rows))
                if condition_node.children:
                    return self.apply_condition(
                        current_rows, condition_node.children[0]
                    )
                return current_rows
            elif op == "<=":
                filtered_rows = [row for row in rows if row[attr] <= val]
                current_rows = Rows(filtered_rows, len(filtered_rows))
                if condition_node.children:
                    return self.apply_condition(
                        current_rows, condition_node.children[0]
                    )
                return current_rows
            elif op == ">=":
                filtered_rows = [row for row in rows if row[attr] >= val]
                current_rows = Rows(filtered_rows, len(filtered_rows))
                if condition_node.children:
                    return self.apply_condition(
                        current_rows, condition_node.children[0]
                    )
                return current_rows
            elif op == "<>":
                filtered_rows = [row for row in rows if row[attr] != val]
                current_rows = Rows(filtered_rows, len(filtered_rows))
                if condition_node.children:
                    return self.apply_condition(
                        current_rows, condition_node.children[0]
                    )
                return current_rows
            else:
                raise ValueError(f"Unsupported operator: {op}")

        elif condition_node.type == "AND":
            current_rows = Rows(rows, len(rows))
            for child in condition_node.children:
                current_rows = self.apply_condition(current_rows, child)
            return current_rows

        elif condition_node.type == "OR":
            or_results = []
            for child in condition_node.children:
                or_results.extend(
                    self.apply_condition(Rows(rows, len(rows)), child).data
                )

            # Remove duplicates
            or_results = [dict(t) for t in {tuple(d.items()) for d in or_results}]
            return Rows(or_results, len(or_results))

        else:
            raise ValueError(f"Unsupported condition type: {condition_node.type}")

    def _process_selection_stmt(self, node: QueryTree) -> Rows:
        base_rows = self._process_node(node.children[0])

        if len(node.children) < 2:
            return base_rows

        filter_node = node.children[1]

        return self.apply_condition(base_rows, filter_node)

    def FROM(self, data):
        data_retrieval = DataRetrieval([data.value], ["*"], [])
        validate_val = self.concurrency_manager.validate_object(
            data.value, self.transaction_id, Action(ActionType.READ)
        ).allowed

        count = 0
        while True:
            if validate_val == ResponseType.ALLOWED:
                log_ccm(
                    f"{COLOR_LIST[self.transaction_id % 4]}Transaction {self.transaction_id}: {GREEN}Granted{RESET} access [READ] on table {data.value} "
                )
                break
            elif validate_val == ResponseType.WAITING:
                log_ccm(
                    f"{COLOR_LIST[self.transaction_id%4]}Transaction {self.transaction_id}: {CYAN}Waiting{RESET} for access [READ] on table {data.value} "
                )
                time.sleep(0.2)
            else:
                log_ccm(
                    f"{COLOR_LIST[self.transaction_id%4]}Transaction {self.transaction_id}: {RED}Failed{RESET} to access [READ] on table {data.value} "
                )
                log_ccm(
                    f"{COLOR_LIST[self.transaction_id%4]}Transaction {self.transaction_id}: {RED}Calling{RESET} Rollback "
                )
                # DIE
                raise Exception("Transaction failed")

            validate_val = self.manager.validate_object(
                data.value, self.transaction_id, Action(ActionType.READ)
            ).allowed

            count += 1

        result = self.storage_engine.buffered_read_block(data_retrieval)

        return result

    def SELECT(self, data, select_cols) -> Rows:
        if select_cols == ["*"]:
            return data

        result = []
        for row in data.data:
            result_row = {}
            for col in select_cols:
                col_lower = col.lower()

                if col_lower.count(".") != 0:
                    col_lower = col_lower.split(".")[1]

                if col_lower in row:
                    result_row[col_lower] = row[col_lower]
            result.append(result_row)

        return Rows(result, len(result))

    def LIMIT(self, data, limit_value):
        if limit_value < 0:
            return []
        limited_results = data.data[:limit_value]
        return Rows(limited_results, len(limited_results))

    def ORDER_BY(self, data, order_by, order_type=["asc"]):
        if not isinstance(order_by, list):
            order_by = [order_by]

        if not isinstance(order_type, list):
            order_type = [order_type] * len(order_by)

        if len(order_by) != len(order_type):
            raise ValueError("Length of order_by and order_type must match")

        def custom_sort_key(item):
            return tuple(
                (
                    item.get(col) is None,
                    item.get(col),
                )
                for col in order_by
            )

        sorted_data = sorted(data.data, key=custom_sort_key)

        for i, order in enumerate(order_type):
            if order.lower() == "desc":
                sorted_data = sorted(
                    sorted_data,
                    key=lambda x: x.get(order_by[i])
                    if x.get(order_by[i]) is not None
                    else float("inf"),
                    reverse=True,
                )

        return Rows(sorted_data, len(sorted_data))

    def JOIN(self, left_result: Rows, right_result: Rows, on_condition: list) -> Rows:
        if not isinstance(on_condition, list) or len(on_condition) != 2:
            raise ValueError(
                "Invalid JOIN condition. Expected a list with two elements."
            )

        left_col = on_condition[0].split(".")[-1]
        right_col = on_condition[1].split(".")[-1]

        left_unique = [dict(t) for t in {tuple(d.items()) for d in left_result.data}]
        right_unique = [dict(t) for t in {tuple(d.items()) for d in right_result.data}]

        join_data = []

        for left_row in left_unique:
            left_value = left_row.get(left_col)  # Ambil nilai dari tabel kiri
            
            matched = False
            for right_row in right_unique:
                right_value = right_row.get(right_col)  # Ambil nilai dari tabel kanan
                if left_value == right_value:
                    combined_row = {
                        **left_row,
                        **{f"{key}": value for key, value in right_row.items()},
                    }
                    join_data.append(combined_row)
                    matched = True

            if not matched:
                continue

        if not join_data:
            return Rows([], 0)
        
        return Rows(join_data, len(join_data))

    def CARTESIAN(self, node) -> Rows:
        table_names = []
        for child in node.children:
            table_names.append(child.value)

        data: list[Rows] = []
        for child in node.children:
            data.append(self._process_node(child))

        if not data:
            return Rows()

        result = []
        for i in range(0, len(data)):
            if i == 0:
                result = [
                    {f"{table_names[i]}.{key}": value for key, value in row.items()}
                    for row in data[0].data
                ]
            else:
                result = [
                    {
                        **row1,
                        **{
                            f"{table_names[i]}.{key}": value
                            for key, value in row2.items()
                        },
                    }
                    for row1 in result
                    for row2 in data[i].data
                ]
        return Rows(result, len(result))

    def UPDATE(self, node: QueryTree) -> Rows:
        new_value = node.value
        columns = [node.attr]

        return self._update_selection_stmt(columns, new_value, node.children[0])

    def _update_selection_stmt(
        self,
        column: list[str],
        new_value: None,
        current_node: QueryTree,
    ):
        table = current_node.children[0].value[0]
        next_node = current_node.children[1]

        return self._apply_update_condition(next_node, column, new_value, table)

    def _apply_update_condition(
        self, current_node, columns, new_value, table, conditions: list[Condition] = []
    ) -> Rows:
        if current_node.type == "SELECTION":
            attr = current_node.attr.lower()
            op = current_node.operand
            val = current_node.value

            if len(current_node.children) > 0:
                return self._apply_update_condition(
                    current_node.children[0], columns, new_value, table, conditions
                )

            conditions.append(Condition(attr, op, float(val) if val.isdigit() else val))

            data_write = DataWrite(
                table=table,
                column=columns,
                conditions=conditions,
                new_value=new_value[0] if len(new_value) == 1 else new_value,
            )

            validate_val = self.concurrency_manager.validate_object(
                table, self.transaction_id, Action(ActionType.WRITE)
            ).allowed

            count = 0
            while True:
                if validate_val == ResponseType.ALLOWED:
                    log_ccm(
                        f"{COLOR_LIST[self.transaction_id % 4]}Transaction {self.transaction_id}: {GREEN}Granted{RESET} access [WRITE] on table {table} {RESET}"
                    )
                    break
                elif validate_val == ResponseType.WAITING:
                    log_ccm(
                        f"{COLOR_LIST[self.transaction_id%4]}Transaction {self.transaction_id}: {CYAN}Waiting{RESET} for access [WRITE] on table {table} {RESET}"
                    )
                    time.sleep(0.2)
                else:
                    log_ccm(
                        f"{COLOR_LIST[self.transaction_id%4]}Transaction {self.transaction_id}: {RED}Failed{RESET} to access [WRITE] on table {table} {RESET}"
                    )
                    log_ccm(
                        f"{COLOR_LIST[self.transaction_id%4]}Transaction {self.transaction_id}: {RED}Calling{RESET} Rollback {RESET}"
                    )
                    # DIE
                    raise Exception("Transaction failed")

                validate_val = self.manager.validate_object(
                    table, self.transaction_id, Action(ActionType.WRITE)
                ).allowed

                count += 1

            row = self.storage_engine.buffered_write_block(data_write)

            return row

        elif current_node.type == "AND":
            return self._apply_update_condition(
                current_node.children[0], columns, new_value, table, conditions
            )

        elif current_node.type == "OR":
            or_results = []
            for child in current_node.children:
                or_results.extend(
                    self._apply_update_condition(
                        child, columns, new_value, table, conditions
                    ).data
                )

            or_results = [dict(t) for t in {tuple(d.items()) for d in or_results}]
            return Rows(or_results, len(or_results))

        else:
            raise ValueError(f"Unsupported current type: {current_node.type}")

    def _update_set(self, data: Rows, set_node: QueryTree) -> Rows:
        if data.rows_count == 0:
            return data
        rows = data.data
        attr = set_node.attr
        val = set_node.value
        if isinstance(rows[0][attr], (int, float)):
            val = float(val)
        for row in rows:
            row[attr] = val
        return Rows(rows, len(rows))

    def INSERT(self, node: QueryTree) -> Rows:
        columns = node.attr
        values = node.value

        table = node.children[0].value[0]

        data_write = DataWrite(
            table=table, column=columns, conditions=[], new_value=values
        )

        validate_val = self.concurrency_manager.validate_object(
            table, self.transaction_id, Action(ActionType.WRITE)
        ).allowed

        count = 0
        while True:
            if validate_val == ResponseType.ALLOWED:
                log_ccm(
                    f"{COLOR_LIST[self.transaction_id % 4]}Transaction {self.transaction_id}: {GREEN}Granted{RESET} access [WRITE] on table {table} {RESET}"
                )
                break
            elif validate_val == ResponseType.WAITING:
                log_ccm(
                    f"{COLOR_LIST[self.transaction_id % 4]}Transaction {self.transaction_id}: {CYAN}Waiting{RESET} for access [WRITE] on table {table} {RESET}"
                )
                time.sleep(0.2)
            else:
                log_ccm(
                    f"{COLOR_LIST[self.transaction_id % 4]}Transaction {self.transaction_id}: {RED}Failed{RESET} to access [WRITE] on table {table} {RESET}"
                )
                log_ccm(
                    f"{COLOR_LIST[self.transaction_id % 4]}Transaction {self.transaction_id}: {RED}Calling{RESET} Rollback {RESET}"
                )
                # Rollback atau akhiri transaksi
                raise Exception("Transaction failed")

            validate_val = self.manager.validate_object(
                table, self.transaction_id, Action(ActionType.WRITE)
            ).allowed

            count += 1

        inserted_rows = self.storage_engine.buffered_write_block(data_write)

        message = f"{len(inserted_rows.data)} rows affected"
        return Rows(
            data=inserted_rows.data, rows_count=len(inserted_rows.data), message=message
        )

    def BEGIN_TRANSACTION(self):
        self.transaction_id = self.concurrency_manager.begin_transaction()

    def COMMIT(self) -> None:
        if self.enable_sleep:
            time.sleep(5)

        self.concurrency_manager.end_transaction(self.transaction_id)
        self.multiple_transaction = False
        log_qp(
            f"{COLOR_LIST[self.transaction_id % 4]}Transaction {self.transaction_id}: {RESET} Transaction finished"
        )
