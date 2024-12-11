
from OptimizerRule import OptimizerRule
from TreeManager import TreeManager
from Query import QueryTree

# RULES 6
class NaturalJoinCommutative(OptimizerRule):
    def apply_rule(self, querytree: QueryTree) -> QueryTree:
        if not querytree.childs:
            return querytree

        for i, child in enumerate(querytree.childs):
            querytree.childs[i] = self.apply_rule(child)

        if (querytree.type == "selection" or querytree.type == "projection") and querytree.childs[0].type == "join" and querytree.childs[0].val["natural"] == True:
            tables = []
            joins = []
            for child in querytree.childs[0].childs :
                if child.type == "join" and child.val["natural"] == True:
                    joins.append(child)
                else:
                    tables.append(child)
            all_tables = []
            for join in joins:
                for child in join.childs:
                    all_tables.append(child)
            all_tables.extend(tables)  
            querytree.childs = self._rebuild_joins(all_tables)

        return querytree

    def _rebuild_joins(self, tables: list[QueryTree]) -> list[QueryTree]:
        if len(tables) == 1:
            return tables  

        node = QueryTree()
        node.val = {}
        node.val["natural"] = True
        join_node = QueryTree("join", node.val, None, node)
        node.childs.append(join_node)
        join_node.childs.append(tables[0])
        

        join_node2 = QueryTree("join", node.val, None, node)
        for table in tables[1:]:
            join_node2.childs.append(table)

        join_node.childs.append(join_node2)

        return node.childs 