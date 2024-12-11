from queryOptimizer.classes.OptimizerRule import OptimizerRule
from queryOptimizer.classes.Query import QueryTree

# RULES 5
class ThetaJoinCommutative(OptimizerRule):
    def apply_rule(self, querytree: QueryTree) -> QueryTree:
        if not querytree.childs:
            return querytree

        for i, child in enumerate(querytree.childs):
            querytree.childs[i] = self.apply_rule(child)

        if (querytree.type == "selection" or querytree.type == "projection") and querytree.childs[0].type == "join" and querytree.childs[0].val["natural"] == False:
            childs = []
            querytree.childs.reverse();
            for child in querytree.childs[0].childs:
                childs.append(child)

            childs.reverse()
            querytree.childs[0].childs = childs

        return querytree
