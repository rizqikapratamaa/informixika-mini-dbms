import re
import math
from Query_Optimizer.classes.query_object import ParsedQuery, QueryTree
from Query_Optimizer.classes.parser import Parser
from Query_Optimizer.classes.optimize import OptimizeRule
from Utils.component_logger import log_qo


class OptimizationEngine:
    def __init__(self) -> None:
        pass

    def parse_query(self, query: str) -> ParsedQuery:
        def check_alias(query: str) -> bool:
            alias_pattern = re.compile(r"(\bFROM|\bJOIN|\bNATURAL JOIN)\s+(\w+)\s+(?:AS\s+){1}(\w+)", re.IGNORECASE)
            return bool(alias_pattern.search(query))

        def remove_alias_from_query(query: str) -> str:
            alias_pattern = re.compile(r"(\bFROM|\bJOIN|\bNATURAL JOIN)\s+(\w+)\s+(?:AS\s+){1}(\w+)", re.IGNORECASE)
            alias_matches = alias_pattern.findall(query)

            alias_to_table = {match[2]: match[1] for match in alias_matches}

            query = alias_pattern.sub(r"\1 \2", query)

            def replace_alias(match):
                alias, column = match.groups()
                if alias in alias_to_table:
                    return f"{alias_to_table[alias]}.{column}"
                return match.group(0)

            alias_usage_pattern = re.compile(r"\b(\w+)\.(\w+)")
            query = alias_usage_pattern.sub(replace_alias, query)

            return query

        if check_alias(query):
            query = remove_alias_from_query(query)

        is_valid, error = self.validate_sql_query(query)
        # print(f"Query: {query}")
        if is_valid:
            log_qo(f"Query is valid: {query}")
        else:
            raise Exception(f"Query is invalid: {query}")

        query_tree = Parser(query).parse()
        parsed_query = ParsedQuery(query_tree, query)
        optimized_query = self.optimize_query(parsed_query)
        return optimized_query

    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        def check_cross_on_node(query: ParsedQuery) -> bool:
            for node in query.query_tree.children:
                if node.type == "CROSS":
                    return True
            return False

        optimizeQuery = query
        optimizer = OptimizeRule()

        count_update = len(
            optimizer.find_type_nodes(optimizeQuery.query_tree, "UPDATE")
        )
        count_relation = len(
            optimizer.find_type_nodes(optimizeQuery.query_tree, "RELATION")
        )
        log_qo("Cout relation " + str(count_relation))
        if count_update > 0 or count_relation <= 1:
            return ParsedQuery(optimizeQuery.query_tree, query)
        log_qo("Start Optimize ")

        optimizeQuery4 = optimizeQuery.copy()
        optimizeQuery7 = optimizeQuery.copy()
        optimizeQuery8 = optimizeQuery.copy()

        cost7 = 999999999999999999999999999999999999999999
        cost8 = 999999999999999999999999999999999999999999

        if check_cross_on_node(optimizeQuery4):
            # Apply Rule 4: Example optimization rule, uncomment others if needed
            optimizeQuery7 = optimizer.rule4(
                optimizeQuery4.query_tree
            )  # Assuming rule4 exists
            optimizeQuery8 = optimizer.rule4(
                optimizeQuery4.query_tree
            )  # Assuming rule4 exists

        if optimizer.find_type_nodes(optimizeQuery7.query_tree, "SELECTION"):
            # Apply Rule 7: Example optimization rule, uncomment others if needed
            optimizeQuery_rule7 = optimizer.rule7(
                optimizeQuery7.query_tree
            )  # Assuming rule7 exists
            cost7 = self.get_cost(optimizeQuery_rule7)
            log_qo("Cost Rule 7: " + str(cost7))

        proj_nodes = (
            optimizeQuery8.query_tree
            if optimizer.find_type_nodes(optimizeQuery8.query_tree, "PROJECTION") == []
            else optimizer.find_type_nodes(optimizeQuery8.query_tree, "PROJECTION")[0]
        )
        if proj_nodes.value[0] != "*":
            # Apply Rule 8: Example optimization rule, uncomment others if needed
            optimizeQuery_rule8 = optimizer.rule8(
                optimizeQuery8.query_tree
            )  # Assuming rule8 exists
            cost8 = self.get_cost(optimizeQuery_rule8)
            log_qo("Cost Rule 8: " + str(cost8))

        # use the optimized query with the lowest cost
        if int(cost7) < int(cost8):
            # log_qo(optimizeQuery7)
            # print(optimizeQuery8)
            return optimizeQuery7
        else:
            # print(optimizeQuery7)
            # print(optimizeQuery8)
            return optimizeQuery8

    def get_cost(self, query: ParsedQuery) -> int:
        cost = 0  # Initialize total cost

        def calculate_cost(node: QueryTree):
            if node.type == "RELATION":
                stats = self.get_table_stats(node.value)
                return stats["br"]  # Biaya akses langsung ke tabel
            elif not node.children or len(node.children) < 1:
                return 0
            elif node.type == "SELECTION":
                if not node.children or len(node.children) < 1:
                    log_qo(
                        f"Warning: SELECTION node '{node.attr} {node.operand} {node.value}' has no children."
                    )
                    # Asumsi default untuk node tanpa anak
                    base_stats = self.get_table_stats(
                        "default_table"
                    )  # Ganti dengan nama tabel default
                    selectivity = 1 / max(
                        1, self.get_distinct_values(node.attr, base_stats)
                    )
                    return base_stats["nr"] * selectivity
                base_stats = self.get_table_stats(node.children[0].value)
                selectivity = 1 / max(
                    1, self.get_distinct_values(node.attr, base_stats)
                )
                return base_stats["nr"] * selectivity
            elif node.type == "JOIN":
                if not node.children or len(node.children) < 2:
                    raise ValueError("Join node requires two children.")
                left_stats = self.get_table_stats(node.children[0].value)
                right_stats = self.get_table_stats(node.children[1].value)
                return (
                    left_stats["nr"]
                    * right_stats["nr"]
                    / max(left_stats["distinct"], right_stats["distinct"])
                )
            elif node.type == "PROJECTION":
                if not node.children or len(node.children) < 1:
                    log_qo("Warning: PROJECTION node has no children.")
                base_stats = self.get_table_stats(node.children[0].value)
                projected_columns = (
                    len(node.value) if isinstance(node.value, list) else 1
                )
                total_columns = base_stats.get("total_columns", projected_columns)
                projection_cost = base_stats["nr"] * (projected_columns / total_columns)
                return projection_cost
            elif node.type == "PROJECTION":
                if not node.children or len(node.children) < 1:
                    log_qo("Warning: PROJECTION node has no children.")
                    return 0  # Tidak ada biaya jika PROJECTION tidak memiliki anak
                return calculate_cost(node.children[0])  # Projection cost negligible
            elif node.type == "ORDER BY":
                if not node.children or len(node.children) < 1:
                    raise ValueError("Order By node requires at least one child.")
                stats = self.get_table_stats(node.children[0].value)
                return stats["nr"] * math.log(stats["nr"], 2)  # Sorting cost
            elif node.type == "LIMIT":
                return 1  # LIMIT operation cost is constant
            return 0

        def traverse_tree(node):
            nonlocal cost
            if node is None:
                return
            cost += calculate_cost(node)
            for child in node.children or []:  # Pastikan anak dapat diiterasi
                traverse_tree(child)

        traverse_tree(query.query_tree)
        return cost

    def get_table_stats(self, table_name: str) -> dict:
        # Mock stats for now, replace with actual data retrieval
        return {
            "nr": 1000,  # Total tuples
            "br": 50,  # Blocks
            "lr": 128,  # Tuple size
            "distinct": 100,  # Distinct values for attributes
        }

    def get_distinct_values(self, attribute, stats) -> int:
        return stats.get("distinct", 1)  # Fallback to 1 if no data

    # def validate_query(self, query: str) -> bool:
    #     # Define patterns for valid queries
    #     patterns = {
    #         # SELECT queries
    #         "SELECT_JOIN_LIMIT": r"^SELECT\s+(\*|\w+)(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+JOIN\s+\w+(\s+AS\s+\w+)?\s+ON\s+\w+\.\w+\s*=\s*\w+\.\w+\s+LIMIT\s+\d+\s*;$",
    #         "SELECT_BASIC": r"^SELECT\s+(\*|\w+)(\s+AS\s+\w+)?(\s*,\s*(\*|\w+)(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?\s*;$",
    #         "SELECT_WHERE": r"^SELECT\s+\w+(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+WHERE\s+\w+\s*(=|>|<|>=|<=|!=)\s*(\w+|\d+)\s*;$",
    #         "SELECT_ALL_WHERE": r"^SELECT\s+\*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+WHERE\s+\w+\s*(=|>|<|>=|<=|!=)\s*(\w+|\d+)\s*;$",
    #         "SELECT_ORDER_BY": r"^SELECT\s+(\*|\w+)(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+ORDER\s+BY\s+\w+(\s+ASC|\s+DESC){1}\s*;$",
    #         "SELECT_LIMIT": r"^SELECT\s+(\*|\w+)(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+LIMIT\s+\d+\s*;$",
    #         "SELECT_JOIN": r"^SELECT\s+(\w+\.?\w*|\w+\s+AS\s+\w+)(\s*,\s*(\w+\.?\w*|\w+\s+AS\s+\w+))*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+JOIN\s+\w+(\s+AS\s+\w+)?\s+ON\s+(\w+\.)\w+\s*=\s*(\w+\.)\w+\s*;$",
    #         "SELECT_JOIN_WHERE": r"^SELECT\s+\w+(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+JOIN\s+\w+(\s+AS\s+\w+)?\s+ON\s+\w+\.\w+\s*=\s*\w+\.\w+\s+WHERE\s+\w+\s*(=|>|<|>=|<=|!=)\s*(\w+|\d+)\s*;$",
    #         "SELECT_JOIN_ORDER_BY_LIMIT": r"^SELECT\s+\w+(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+JOIN\s+\w+(\s+AS\s+\w+)?\s+ON\s+\w+\.\w+\s*=\s*\w+\.\w+\s+ORDER\s+BY\s+\w+(\s+ASC|\s+DESC){1}\s+LIMIT\s+\d+\s*;$",
    #         # "SELECT_GROUP_BY": r"^SELECT\s+\w+(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*,\s+COUNT\(\*\)\s+FROM\s+\w+(\s+AS\s+\w+)?\s+GROUP\s+BY\s+\w+;$",
    #         # "SELECT_GROUP_BY_HAVING": r"^SELECT\s+\w+(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*,\s+COUNT\(\*\)\s+FROM\s+\w+(\s+AS\s+\w+)?\s+GROUP\s+BY\s+\w+(\s+HAVING\s+COUNT\(\*\)\s*>\s*\d+)?;$",
    #         # "SELECT_SUBQUERY": r"^SELECT\s+\w+(\s+AS\s+\w+)?(\s*,\s*\w+(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+WHERE\s+\w+\s*=\s*\(SELECT\s+\w+\s+FROM\s+\w+(\s+AS\s+\w+)?\);$",
    #         # "SELECT_ALL_JOIN": r"^SELECT\s+\*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+JOIN\s+\w+(\s+AS\s+\w+)?\s+ON\s+\w+\.\w+\s*=\s*\w+\.\w+;$",
    #         # # "SELECT_LIKE": r"^SELECT\s+\*\s+FROM\s+\w+(\s+AS\s+\w+)?\s+WHERE\s+\w+\s+LIKE\s+'.*';$",
    #         # # UPDATE queries
    #         # "UPDATE_BASIC": r"^UPDATE\s+\w+\s+SET\s+\w+\s*=\s*.+;$",
    #         # "UPDATE_WHERE": r"^UPDATE\s+\w+\s+SET\s+\w+\s*=\s*.+\s+WHERE\s+\w+\s*(=|>|<|>=|<=|!=)\s*.+;$",
    #         "UPDATE": r"^UPDATE\s+\w+\s+SET\s+\w+\s*=\s*(\s+(\d+|((\"|\')\s*\w+\s*(\"|\')))\s*)(\s+WHERE\s+\w+\s*(=|>|<|>=|<=|!=)\s*(\w+|\d+)(\s+(AND|OR)\s+\w+\s*(=|>|<|>=|<=|!=)\s*(\w+|\d+))*)?\s*;$",
    #         # TRANSACTION queries
    #         # "BEGIN_TRANSACTION": r"^BEGIN\s+TRANSACTION;$",
    #         # "COMMIT": r"^COMMIT;$",
    #         "ALL": r"^SELECT\s+(\*|\w+)(\s+AS\s+\w+)?(\s*,\s*(\*|\w+)(\s+AS\s+\w+)?)*\s+FROM\s+\w+(\s+AS\s+\w+)?((\s+JOIN\s+\w+(\s+AS\s+\w+)?\s+ON\s+\w+\.\w+\s*=\s*\w+\.\w+)|(\s+NATURAL JOIN\s+\w+(\s+AS\s+\w+)?))*(\s+WHERE\s+\w+\s*(=|>|<|>=|<=|!=)\s*(\w+|\d+)(\s+(AND|OR)\s+\w+\s*(=|>|<|>=|<=|!=)\s*(\w+|\d+))*)?(\s+ORDER\s+BY\s+\w+(\s+ASC|\s+DESC){1})?(\s+LIMIT\s+\d+)?\s*;$",
    #     }
    #     # Get the query string from the ParsedQuery object
    #     query_str = query

    #     # Iterate over each pattern to check if it matches the query
    #     for key, pattern in patterns.items():
    #         if re.match(pattern, query_str, re.IGNORECASE):
    #             return True

    #     # If none of the patterns match, return False
    #     return False

    def validate_sql_query(self, query):
        """
        Validates an SQL query string to ensure it conforms to basic SQL syntax rules.
        Handles simple and complex queries containing keywords such as SELECT, UPDATE, FROM, WHERE, AS, JOIN, NATURAL JOIN, ORDER BY, LIMIT, BEGIN TRANSACTION, and COMMIT.

        Args:
            query (str): The SQL query string to validate.

        Returns:
            bool: True if the query is valid, False otherwise.
            str: Error message if invalid, None otherwise.
        """
        if not isinstance(query, str):
            return False, "Query must be a string."

        # Check for empty query
        if not query.strip():
            return False, "Query cannot be empty."

        # Basic SQL keywords validation (uppercase and lowercase)
        sql_keywords = [
            "SELECT",
            "UPDATE",
            "FROM",
            "WHERE",
            "AS",
            "JOIN",
            "NATURAL JOIN",
            "ORDER BY",
            "LIMIT",
            "BEGIN TRANSACTION",
            "COMMIT",
            "GROUP BY",
            "HAVING",
        ]
        keyword_pattern = re.compile(
            r"\b(" + "|".join(sql_keywords) + r")\b", re.IGNORECASE
        )
        if not keyword_pattern.search(query):
            return False, "Query does not contain valid SQL keywords."

        # Check for balanced parentheses
        stack = []
        for char in query:
            if char == "(":
                stack.append(char)
            elif char == ")":
                if not stack:
                    return False, "Unbalanced parentheses: more closing than opening."
                stack.pop()
        if stack:
            return False, "Unbalanced parentheses: more opening than closing."

        # Detect dangerous keywords to prevent SQL injection (basic check)
        dangerous_keywords = [
            "--",
            "/*",
            "*/",
            "xp_",
            "exec",
            "drop",
            "truncate",
            "alter",
        ]
        for keyword in dangerous_keywords:
            if keyword.lower() in query.lower():
                return False, f"Query contains potentially dangerous keyword: {keyword}"

        # Validate proper clause order for basic SQL structure
        clause_order = [
            "BEGIN TRANSACTION",
            "SELECT",
            "UPDATE",
            "FROM",
            "WHERE",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "COMMIT",
        ]
        last_index = -1
        for clause in clause_order:
            matches = re.finditer(r"\b" + clause + r"\b", query, re.IGNORECASE)
            for match in matches:
                current_index = match.start()
                if current_index < last_index:
                    return False, f"Clause '{clause}' is out of order."
                last_index = current_index

        # Validate JOIN clauses
        join_pattern = re.compile(
            r'(NATURAL JOIN|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)', re.IGNORECASE
        )
        join_follow_pattern = re.compile(
            r'(NATURAL JOIN|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)(\s+(JOIN|NATURAL JOIN|ON|WHERE|GROUP BY|ORDER BY|LIMIT|AS|$))?', re.IGNORECASE
        )
        joins = re.findall(
            r'(NATURAL JOIN|JOIN)\b', query, re.IGNORECASE
        )

        for join in joins:
            if not join_pattern.search(query):
                return False, f"Incomplete {join} clause: missing table name or alias."
            if not join_follow_pattern.search(query):
                return False, "Each JOIN clause must be followed by exactly one table."

        # Validate ORDER BY clause
        order_by_pattern = re.compile(
            r"ORDER BY\s+([a-zA-Z_][a-zA-Z0-9_\.]*)\s+(ASC|DESC)", re.IGNORECASE
        )
        if "ORDER BY" in query.upper():
            if not order_by_pattern.search(query):
                return False, "Incomplete ORDER BY clause: missing column or direction."

        # Validate AS clause only if present and not part of ASC
        as_pattern = re.compile(r"\bAS\s+[a-zA-Z_][a-zA-Z0-9_]*\b", re.IGNORECASE)
        if re.search(r"\bAS\b", query, re.IGNORECASE):
            if not as_pattern.search(query):
                return False, "Incomplete AS clause: missing alias."

        # Validate LIMIT clause
        limit_pattern = re.compile(r"LIMIT\s+\d+", re.IGNORECASE)
        if "LIMIT" in query.upper():
            if not limit_pattern.search(query):
                return False, "Incomplete LIMIT clause: missing numeric value."
        
        # Validate ON clause for JOIN
        on_pattern = re.compile(r'ON\s+([a-zA-Z_][a-zA-Z0-9_\.]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_\.]*)', re.IGNORECASE)
        if 'JOIN' in query.upper() and 'NATURAL JOIN' not in query.upper():
            if not on_pattern.search(query):
                return False, "Incomplete JOIN clause: missing ON condition."
        if 'NATURAL JOIN' in query.upper() and 'ON' in query.upper():
            natural_join_pattern = re.compile(r'NATURAL JOIN\s+([a-zA-Z_][a-zA-Z0-9_\.]*)', re.IGNORECASE)
            if not natural_join_pattern.search(query):
                return False, "NATURAL JOIN does not require an ON condition."

        return True, None


if __name__ == "__main__":
    # Example queries for testing
    test_queries = [
        # "SELECT e.name , d.department_name FROM Employees AS e JOIN Departments AS d ON e.department_id = d.department_id;"
        # "UPDATE employees SET salary = 75000 WHERE id = 101;",
        # "UPDATE employees SET position = 'Manager', department = 'Finance' WHERE id = 102;",
        # "UPDATE employees SET salary = salary * 1.10 WHERE department = 'IT';",
        # "UPDATE products SET discount_price = price * 0.80 WHERE category = 'Sale';",
        # "SELECT a FROM Table1 JOIN Table2 ON Table1.id = Table2.id JOIN Table3 ON Table2.name = Table3.name ORDER BY b DESC LIMIT 10",
        # "SELECT a FROM Table1 JOIN Table2 ON Table1.id = Table2.id LIMIT 10",
        # "SELECT a FROM Table1 WHERE a <= 10",
        # "SELECT * FROM Table1 WHERE b > 2",
        # "SELECT name FROM Products WHERE price > (SELECT AVG(price) FROM Products)",
        # "SELECT department, COUNT(*) FROM Employees GROUP BY department HAVING COUNT(*) > 5",
        # "SELECT * FROM Products WHERE name LIKE 'A%'",
        #  # SELECT queries
        # "SELECT a, b FROM Table1",  # Basic SELECT
        # "SELECT a, b FROM Table1, Table2",  # SELECT with multiple tables
        # "SELECT a, b FROM Table1, Table2 WHERE Table1.id = Table2.id",  # SELECT with WHERE clause
        # "SeLeCT a, b FroM Table1 wHERE a > 10",  # SELECT with WHERE clause
        # "SELECT a, b FROM Table1 ORDER BY b",  # SELECT with ORDER BY clause
        # "SELECT a, b FROM Table1 LIMIT 5",  # SELECT with LIMIT clause
        # "SELECT a, b FROM Table1 WHERE a > 10 ORDER BY b LIMIT 5",  # SELECT with WHERE, ORDER BY, LIMIT
        # "SELECT a FROM Table1 JOIN Table2 ON Table1.id = Table2.id",  # SELECT with JOIN
        "SELECT users.nama, products.harga FROM users NATURAL JOIN products JOIN orders ON users.user_id = orders.user_id WHERE users.user_id = 2",  # SELECT with JOIN and WHERE
        # "SELECT Table1.a FROM Table1 JOIN Table2 ON Table1.id = Table2.id ORDER BY Table1.id ASC LIMIT 10",  # SELECT with JOIN, ORDER BY, LIMIT
        # "SELECT a FROM Table1 JOIN Table2 ON Table1.id = Table2.id JOIN Table3 ON Table2.name = Table3.name ORDER BY b LIMIT 10",  # SELECT with JOIN and ORDER BY
        # "SELECT a FROM Table1 JOIN Table2 ON Table1.id = Table2.id LIMIT 10",
        # "SELECT tab1.a, tab2.b FROM tab1,tab2 WHERE tab1.a > 10 AND tab2.b < 5 OR tab2.c = 3 AND tab1.a < 5",  # SELECT with JOIN and LIMIT
        # "SELECT a, b FROM Table1 WHERE a > 10 AND b < 5 OR c = 3 OR a < 5",  # SELECT with JOIN and LIMIT
        # "SELECT a, b FROM Table1 WHERE a > 10 AND b < 5 OR c = 3",  # SELECT with JOIN and LIMIT
        # "SELECT a, b FROM Table1 WHERE a > 10 AND b < 5 AND C > 5",  # SELECT with JOIN and LIMIT
        # UPDATE queries
        # "UPDATE Table1 SET a = 10",  # Basic UPDATE
        # "UPDATE Table1 SET a = 10 WHERE a > 10 AND b < 5 OR c = 3 AND a < 5",  # UPDATE with WHERE clause
        # "UPDATE Table2 SET col1 = 'value', col2 = col2 + 1 WHERE col3 IS NULL",  # UPDATE with multiple columns and WHERE
        # # TRANSACTION queries
        # "BEGIN TRANSACTION",  # Basic transaction query
        # # COMMIT queries
        # "COMMIT",  # Basic commit query
        # "SELECT * FROM a WHERE b > 2"
        "SELECT * FROM Users",
    ]

    engine = OptimizationEngine()

    for query_str in test_queries:
        try:
            is_valid, error = engine.validate_sql_query(query_str)
            print(f"Query: {query_str}")
            if error:
                raise Exception(f"Query is invalid: {query_str}\n Error: {error}")
            print("valid")

            parsed_query = engine.parse_query(query_str)
            print(f"Parsed query:\n{parsed_query}")

            # Uncomment this if you want to apply optimization
            optimized_query = engine.optimize_query(parsed_query)
            print(f"Optimized query:\n{optimized_query}")
            print(f"Cost: {engine.get_cost(optimized_query)}")
            print("tessss")
            # Cost calculation (currently returning 0 as placeholder)
            # cost = engine.get_cost(optimized_query)
            # print(f"Cost: {cost}")

            # Validation

        except Exception as e:
            print(f"Error parsing query: {e}")

        print("-" * 50)
