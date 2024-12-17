import re
from Query_Optimizer.classes.query_object import QueryTree

class Parser:
    def __init__(self, query):
        self.query = query.strip()

    def parse(self):
        if self.query.upper().startswith("SELECT"):
            return self.parse_select()
        elif self.query.upper().startswith("UPDATE"):
            return self.parse_update()
        elif self.query.upper().startswith("BEGIN TRANSACTION"):
            return self.parse_transaction()
        elif self.query.upper().startswith("COMMIT"):
            return self.parse_commit()
        else:
            raise ValueError("Unsupported query type.")

    def split_condition(self, condition: str) -> list:
        # Split the condition on `AND`, then process `OR` separately
        # condition = condition.upper()
        or_conditions = or_conditions = re.split(r"(?i)\s+OR\s+", condition, maxsplit=1)
        if len(or_conditions) > 1:
            or_node = QueryTree("OR", None, [])
            or_node.children.append(self.split_condition(or_conditions[0]))
            or_node.children.append(self.split_condition(or_conditions[1]))
            return or_node

        and_conditions = re.split(r"(?i)\s+AND\s+", condition, maxsplit=1)
        if len(and_conditions) > 1:
            and_node = QueryTree("AND", None, [])
            first_parts = and_conditions[0].strip().split()
            for cond in and_conditions[1:]:
                and_node.children.append(self.split_condition(cond))
            first_child = QueryTree("SELECTION", first_parts[2], [and_node], first_parts[0], first_parts[1])
            return first_child

        parts = condition.strip().split()
        return QueryTree("SELECTION", parts[2], [], parts[0], parts[1])
        # else:
        #     # Process individual conditions like `a > 10`, `b < 5`
        #     condition_parts = and_conditions[0].split(" OR ")
        #     if len(condition_parts) > 1:
        #         # Handle `OR` within the AND condition
        #         or_node = QueryTree("OR", None, [])
        #         for cond in condition_parts:
        #             or_node.children.append(self.split_condition(cond))
        #         return or_node
        #     else:
        #         # Single simple condition like `a > 10`
    def add_parent(self,tree:QueryTree) -> QueryTree:
        for child in tree.children:
            child.parent = tree
            self.add_parent(child)
        return tree

    def parse_select(self):
        """
        Parse a SELECT query into a parse tree.
        :return: Root node of the SELECT query's parse tree.
        """
        # Extract clauses using regex
        select_pattern = re.compile(r"SELECT (.+?) FROM", re.IGNORECASE)
        from_pattern = re.compile(r"FROM (.+?)( WHERE| JOIN| ORDER BY| NATURAL JOIN| LIMIT|;$)", re.IGNORECASE)
        join_pattern = re.compile(r"JOIN (\w+) ON ([\w\.]+ = [\w\.]+)", re.IGNORECASE)
        natural_join_pattern = re.compile(r"NATURAL JOIN (\w+)", re.IGNORECASE)  # New pattern
        where_pattern = re.compile(r"WHERE (.+?)( ORDER BY| LIMIT|;$)", re.IGNORECASE)
        order_by_pattern = re.compile(r"ORDER BY (.+?)( ASC| DESC|;$)", re.IGNORECASE)
        limit_pattern = re.compile(r"LIMIT (\d+)", re.IGNORECASE)
        select_match = select_pattern.search(self.query)
        from_match = from_pattern.search(self.query)
        join_matches = join_pattern.findall(self.query)
        natural_join_matches = natural_join_pattern.findall(self.query)  # New matches
        where_match = where_pattern.search(self.query)
        order_by_match = order_by_pattern.search(self.query)
        limit_match = limit_pattern.search(self.query)

        if not select_match or not from_match:
            raise ValueError("Invalid SELECT query. Missing SELECT or FROM clause.")

        # Extract attributes and tables
        attributes = [attr.strip() for attr in select_match.group(1).split(",")]
        base_tables = [table.strip() for table in from_match.group(1).split(",")]
        joins = [{"table": join[0].strip(), "condition": join[1].strip()} for join in join_matches]
        natural_joins = [table.strip() for table in natural_join_matches]  # New natural joins list
        condition = where_match.group(1).strip() if where_match else None
        order_by = order_by_match.group(1).strip() if order_by_match else None
        order_by_operand = order_by_match.group(2).strip() if order_by_match and order_by_match.group(2) else None
        limit = int(limit_match.group(1)) if limit_match else None

        # Handle Cartesian product if multiple base tables and no JOIN
        if len(base_tables) > 1 and not joins and not natural_joins:

            relation_nodes = [QueryTree("RELATION", table) for table in base_tables]
            current_node = QueryTree("CROSS", "none", relation_nodes)
        else:
            # Start with the first table for JOINs
            current_node = QueryTree("RELATION", base_tables[0])

        # Process JOINs hierarchically
        for join in joins:
            join_table = join["table"]
            join_condition = join["condition"]
            join_attributes = [attr.strip() for attr in join_condition.split(" = ")]
            join_node = QueryTree(
                "JOIN",
                join_attributes,
                [current_node, QueryTree("RELATION", join_table)],
            )
            current_node = join_node

        for join_table in natural_joins:
            join_node = QueryTree(
                "JOIN", 
                None, 
                [current_node, QueryTree("RELATION", join_table)]
            )
            current_node = join_node
            if len(self.get_natural_join_tables(current_node.children[0], current_node.children[1])) == 0:
                raise ValueError("Invalid NATURAL JOIN query. No common attribute found.")

            current_node.value = self.get_natural_join_tables(current_node.children[0], current_node.children[1]) 

        # Add WHERE conditions (selection) if present
        if condition:
            condition_tree = self.split_condition(condition)
            current_node = QueryTree("SELECTION_STMT", None, [current_node, condition_tree])

        # Add projection (SELECT attributes)
        current_node = QueryTree("PROJECTION", attributes, [current_node])

        # Add ORDER BY if present
        if order_by:
            current_node = QueryTree("ORDER BY", order_by, [current_node], operand=order_by_operand)

        # Add LIMIT if present
        if limit is not None:
            current_node = QueryTree("LIMIT", limit, [current_node])

        return self.add_parent(current_node)

    def parse_update(self):
        """
        Parse an UPDATE query into a parse tree.
        :return: Root node of the UPDATE query's parse tree.
        """
        update_pattern = re.compile(r"UPDATE (\w+) SET (.+?)( WHERE (.+))?;$", re.IGNORECASE)
        match = update_pattern.match(self.query)

        if not match:
            raise ValueError("Invalid UPDATE query format.")

        table = match.group(1).strip()
        set_clause = match.group(2).strip()
        where_clause = match.group(4).strip() if match.group(4) else None

        # Parse SET clause into attribute and value
        set_pattern = re.compile(r"(\w+)\s*=\s*(.+)")
        set_match = set_pattern.match(set_clause)
        if not set_match:
            raise ValueError("Invalid SET clause format.")

        set_attribute = set_match.group(1).strip()
        set_value = set_match.group(2).strip()

        operations = ['+', '-', '*', '/', '%', '(', ')']

        def parse_expression(expression):
            tokens = re.findall(r'\d+\.\d+|\d+|[\w]+|[+*/()-]', expression)
            return tokens

        # Check if the value contains an operation
        if any(op in set_value for op in operations):
            set_value = parse_expression(set_value)

        # Build tree for the base table
        table_node = QueryTree("RELATION", [table])

        # Build SET node
        if isinstance(set_value, str) and set_value[0] in {"'", '"'} and set_value[-1] in {"'", '"'}:
            set_value = set_value[1:-1]
        set_node = QueryTree("SET", [set_value])

        # Handle WHERE clause (if present)
        if where_clause:
            condition_tree = self.split_condition(where_clause)
            where_node = QueryTree("SELECTION_STMT", None, [table_node,condition_tree])
            # condition_list = self.split_condition(where_clause)  # Split into [attribute, operator, value]
            # where_node = QueryTree("SELECTION", condition_list[2], [table_node], condition_list[0], condition_list[1])
            # Build SET node
            set_node = QueryTree("SET", [set_value], [where_node], set_attribute)
            update_node = QueryTree("UPDATE", None, [set_node])

        else:
            set_node = QueryTree("SET", [set_value], [table_node], set_attribute)
            update_node = QueryTree("UPDATE", None, [set_node])

        return self.add_parent(update_node)

    def parse_transaction(self):
        transaction_pattern = re.compile(r"BEGIN TRANSACTION", re.IGNORECASE)

        if not transaction_pattern.match(self.query):
            raise ValueError("Invalid BEGIN TRANSACTION query format.")

        return QueryTree("TRANSACTION", "Begin")

    def parse_commit(self):
        commit_pattern = re.compile(r"COMMIT", re.IGNORECASE)

        if not commit_pattern.match(self.query):
            raise ValueError("Invalid COMMIT query format.")

        return QueryTree("COMMIT", "Commit")

    def check_id_match(self, i: str, j: str) -> list:
        # Split strings on dot
        i_parts = i.split('.')
        j_parts = j.split('.')

        # Check if both contain 'id' in column part
        if len(i_parts) > 1 and len(j_parts) > 1:
            if 'id' in i_parts[1].lower() and 'id' in j_parts[1].lower():
                # Compare values after dot
                if i_parts[1] == j_parts[1]:
                    return [i, j]

        return []

    def get_natural_join_tables(self, tree1: QueryTree, tree2: QueryTree):
        # Lists to store tables from each tree
        tree1_tables = []
        tree2_tables = []

        # Get tables from tree1
        def collect_tables(node: QueryTree, tables_list: list):
            if node.type == "RELATION":
                tables_list.append(node.value)
            for child in node.children:
                collect_tables(child, tables_list)

        # Collect tables recursively from both trees
        collect_tables(tree1, tree1_tables)
        collect_tables(tree2, tree2_tables)

        # Print tables from both trees
        # print(f"Tree1 tables: {tree1_tables}")
        # print(f"Tree2 tables: {tree2_tables}")

        list1 = []
        list2 = []

        for table in tree1_tables:
            list1.extend(self.get_natural_join_tables_attribute(table))
        # print(list1)

        for table in tree2_tables:
            list2.extend(self.get_natural_join_tables_attribute(table))
        # print(list2)

        for i in list1:
            for j in list2:
                match = self.check_id_match(i, j)
                if match:
                    # print("Match : ", match)
                    return match

        return None

    def get_natural_join_tables_attribute(self, table: str):
        if table == "students":
            return [f"{table}.studentid", f"{table}.gpa", f"{table}.fullname"]
        elif table == "courses":
            return [f"{table}.courseid", f"{table}.year", f"{table}.coursename", f"{table}.coursedescription"]
        elif table == "attends":
            return [f"{table}.studentid", f"{table}.courseid"]
        else:
            raise ValueError(f"Table {table} is not recognized.")
