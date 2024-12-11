from typing import Optional, List

class ParsedQuery:
    query_tree: "QueryTree"
    query: str

    def __init__(self, query_tree:"QueryTree", query: str) -> None:
        self.query_tree = query_tree
        self.query = query

class QueryTree:
    type: str
    val: dict
    childs: list["QueryTree"]
    parent: "QueryTree"

    def __init__(self, type: str = None, val: str = None, childs: List["QueryTree"] = None, parent:Optional["QueryTree"] = None):
        self.type = type
        self.val = val
        self.childs = []
        self.parent = parent

    def copy(self):
        return QueryTree(self.type, self.val,self.childs,self.parent)
    