from typing import List, Optional

class QueryTree:
    def __init__(self, type: str, value: Optional[str] = None, children: Optional[List['QueryTree']] = None, attr: Optional[str] = None, operand: Optional[str] = None, parent : Optional['QueryTree'] = None):
        """
        Initialize the query tree node.
        :param type: Type of the node.
        :param value: Value of the node.
        :param children: List of child nodes.
        :param attr: Attribute of the node.
        :param operand: Operand of the node.
        :param parent: Parent of the node.
        """
        self.type = type
        self.value = value if value else None
        self.children = children if children else []
        self.attr = attr
        self.operand = operand
        self.parent = parent

    def __repr__(self, level=0):
        """
        Represent the tree structure as a string.
        """
        if isinstance(self.value, list):
            value_str = ", ".join(map(str, self.value))
        else:
            value_str = str(self.value) if self.value else ""

        ret = (
            "  " * level
            + f"{self.type}(val : {self.value}, atr : {self.attr}, op : {self.operand}, par : {self.parent.type if self.parent else 'None'})\n"
        )
        for child in self.children:
            ret += child.__repr__(level + 1)
        return ret

class ParsedQuery:
    def __init__(self, query_tree: QueryTree, query: str):
        """
        Initialize the parsed query object.
        :param query_tree: Root node of the query tree.
        :param query: Original query string.
        """
        self.query_tree = query_tree
        self.query = query

    def __repr__(self):
        """
        Represent the parsed query as a string.
        """
        return f"Query: {self.query}\nTree:\n{self.query_tree}"

    def copy(self) -> 'ParsedQuery':
        """
        Create a deep copy of the ParsedQuery object.
        """
        def copy_tree(node: QueryTree) -> QueryTree:
            if node is None:
                return None
            new_node = QueryTree(
                type=node.type,
                value=node.value,
                children=[copy_tree(child) for child in node.children],
                attr=node.attr,
                operand=node.operand,
                parent=None  # Parent will be set later
            )
            for child in new_node.children:
                child.parent = new_node
            return new_node

        new_query_tree = copy_tree(self.query_tree)
        return ParsedQuery(new_query_tree, self.query)