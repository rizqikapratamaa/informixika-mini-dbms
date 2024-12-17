from Query_Optimizer.classes.query_object import QueryTree, ParsedQuery
from typing import List, Optional

class OptimizeRule:

    def tree_to_query(node: QueryTree) -> str:
        """Convert a QueryTree back into a query string."""
        query = ""
        # print(f"Processing node: {node.type}, value: {node.value}") 
        # if node.type == "SELECTION":
        #     query = f"{node.attr} {node.operand} {node.value}"
        # elif node.type == "PROJECTION":
        #     if node.children:
        #         query = f"SELECT {', '.join(node.value)} FROM {OptimizeRule.tree_to_query(node.children[0])}"
        # elif node.type == "JOIN":
        #     if len(node.children) == 2:
        #         query = f"{OptimizeRule.tree_to_query(node.children[0])} JOIN {OptimizeRule.tree_to_query(node.children[1])} ON {node.value[0]} = {node.value[1]}"
        # elif node.type == "CARTESIAN" or node.type == "CROSS":
        #     if len(node.children) == 2:
        #         query = f"{OptimizeRule.tree_to_query(node.children[0])}, {OptimizeRule.tree_to_query(node.children[1])}"
        # elif node.type == "RELATION":
        #     query = node.value
        # elif node.type == "SELECTION_STMT":
        #     if len(node.children) == 2:
        #         query = f"{OptimizeRule.tree_to_query(node.children[0])} WHERE {OptimizeRule.tree_to_query(node.children[1])}"
        # elif node.type == "AND":
        #     if len(node.children) == 2:
        #         query = f"{OptimizeRule.tree_to_query(node.children[0])} AND {OptimizeRule.tree_to_query(node.children[1])}"
        # elif node.type == "OR":
        #     if len(node.children) == 2:
        #         query = f"{OptimizeRule.tree_to_query(node.children[0])} OR {OptimizeRule.tree_to_query(node.children[1])}"
        # elif node.type == "ORDER BY":
        #     if node.children:
        #         query = f"{OptimizeRule.tree_to_query(node.children[0])} ORDER BY {node.value}"
        # elif node.type == "LIMIT":
        #     if node.children:
        #         query = f"{OptimizeRule.tree_to_query(node.children[0])} LIMIT {node.value}"
        # elif node.type == "UPDATE":
        #     if len(node.children) == 1 and node.children[0].type == "SET":
        #         query = f"UPDATE {OptimizeRule.tree_to_query(node.children[0].children[0])} SET {node.children[0].attr} = {node.children[0].value[0]}"
        # else:
        #     raise ValueError(f"Unknown node type: {node.type}")

        return query

    def remove_node(self, root, node):
        if root.children:
            for child in root.children:
                if child == node:
                    root.children.remove(child)
                    return
                self.remove_node(child, node)

    def find_selection_stmt_nodes(self, root):
        selection_nodes = []

        # Recursively search in children
        for child in root.children:
            if child.type == "SELECTION_STMT":
                selection_nodes.append(child)
            selection_nodes.extend(self.find_selection_stmt_nodes(child))
        return selection_nodes

    def rule4(self, root) -> ParsedQuery:
        selection_stmt_nodes = self.find_selection_stmt_nodes(root)
        # print(f"Selection nodes:\n{selection_stmt_nodes}")
        for node in selection_stmt_nodes:
            if node.children[0].type == "CROSS":
                # Create new JOIN node
                join_node = QueryTree("JOIN")
                join_node.attr = None
                join_node.operand = None
                join_node.value = [node.children[1].attr ,node.children[1].value]
                join_node.children = node.children[0].children
                for child in join_node.children:
                    child.parent = join_node
                self.remove_node(root, node)
                root.children.append(join_node)
        return ParsedQuery(root, OptimizeRule.tree_to_query(root))

    def find_selection_nodes(self,root):
        selection_nodes = []

        # Recursively search in children
        for child in root.children:
            if child.type == "SELECTION":
                selection_nodes.append(child)
            selection_nodes.extend(self.find_selection_nodes(child))
        return selection_nodes

    def rule7(self, root) -> ParsedQuery :
        selection_nodes = self.find_selection_nodes(root)
        for node in selection_nodes:
            table_name = node.attr.split(".")[0] if "." in node.attr else None
            relation = self.find_node(root, "RELATION", table_name)
            # print(f"Rel:\n{relation}")
            # print(f"Node:\n{node}")
            node.children = []
            if relation:
                if relation.parent:
                    par = relation.parent
                    if par.type != "SELECTION_STMT":
                        par.children.remove(relation)

                        # Create new SELECTION_STMT node and add it as a child
                        new_selection_stmt = QueryTree("SELECTION_STMT", None, [relation, node])
                        par.children.append(new_selection_stmt)

                        # Update parent references
                        relation.parent = new_selection_stmt
                        node.parent = new_selection_stmt
                    else:
                        leaf = par.children[1]
                        while leaf.children:
                            leaf = leaf.children[0]
                        and_node = QueryTree("AND", None, [node],None,None,leaf)
                        leaf.children.append(and_node)
                        node.parent = and_node

                else:
                    print(f"Error: The relation node does not have a parent.")
            else:
                print(f"Error: Could not find relation node for table {table_name}.")

        join_node = self.find_node(root,"JOIN")
        par_join = join_node.parent
        par_join.parent.children.append(join_node)
        join_node.parent = par_join.parent
        par_join.parent.children.remove(par_join)

        return ParsedQuery(root, OptimizeRule.tree_to_query(root))

    def find_node(self, root, type: str, value: Optional[str] = None) -> QueryTree:
        if root.type == type and (value is None or root.value.upper() == value.upper()):
            return root
        for child in root.children:
            result = self.find_node(child, type, value)
            if result:
                return result
        return None

    def find_type_nodes(self, root, node_type: str):
        type_nodes = []
        # Recursively search in children
        for child in root.children:
            if child.type == node_type:
                type_nodes.append(child)
            # Recursively search in the child nodes
            type_nodes.extend(self.find_type_nodes(child, node_type))
        return type_nodes

    def rule8(self, root) -> ParsedQuery:
        join_nodes = self.find_type_nodes(root,"JOIN")
        proj_nodes = root if self.find_type_nodes(root, "PROJECTION") == [] else self.find_type_nodes(root, "PROJECTION")[0]
        # print(proj_nodes)
        for node in join_nodes:
            # Left
            if node.children[0].type == "RELATION":
                relation = node.children[0]
                relation_name = relation.value
                l1 = [val for val in proj_nodes.value if val.startswith(relation_name+".")]
                l3 = [val for val in node.value if val.startswith(relation_name + ".")]
                unique_values = set()
                if l1:
                    unique_values.update(l1)  # Add elements of l1 to the set
                if l3:
                    unique_values.update(l3)
                arr = list(unique_values)

                left_tree = QueryTree("PROJECTION", arr, [relation], None, None, node)
                relation.parent = left_tree
                node.children[0] = left_tree
                # print(f"node =  {node.children[0].value}")
            # Right
            if node.children[1].type == "RELATION":
                relation = node.children[1]
                relation_name = relation.value
                l2 = [val for val in proj_nodes.value if val.startswith(relation_name+".")]
                l4 = [val for val in node.value if val.startswith(relation_name + ".")]
                unique_values = set()
                if l2:
                    unique_values.update(l2)  # Add elements of l1 to the set
                if l4:
                    unique_values.update(l4)
                arr = list(unique_values)
                right_tree = QueryTree("PROJECTION", arr, [relation], None, None, node)
                relation.parent = right_tree
                node.children[1] = right_tree
                # print("arr : ", arr)
                # print("right tree : ", right_tree)
                # print(f"node =  {node.children[0].value}")

        return ParsedQuery(root, OptimizeRule.tree_to_query(root))


# while current and current.type != "SELECTION_STMT":
# current = current.parent

# # Append the SELECTION_STMT and the SELECTION node
# if current and current.type == "SELECTION_STMT":
# selection_nodes.append((current.type, child))
# selection_nodes.extend(self.find_selection_nodes(child))

# if true_relation and true_relation.parent:
#         true_relation.parent.children = []
#         true_relation.parent.children.append(QueryTree("SELECTION_STMT", None, [true_relation, node]))
