from OptimizerRule import OptimizerRule
from Query import QueryTree

class SelectionCommutative(OptimizerRule):
    def apply_rule(self, query_tree: QueryTree) -> QueryTree:
        if not query_tree.childs:
            return query_tree

        for i, child in enumerate(query_tree.childs):
            query_tree.childs[i] = self.apply_rule(child)

        if query_tree.type == "selection":
            conditions = query_tree.val.get("conditions", [])

            if conditions:
                flipped_conditions = []
                for condition in conditions:
                    flipped_condition = condition[::-1] 
                    flipped_conditions.append(flipped_condition)
                
                query_tree.val["conditions"] = flipped_conditions

        return query_tree
