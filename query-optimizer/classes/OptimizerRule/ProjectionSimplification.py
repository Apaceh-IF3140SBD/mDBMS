from OptimizerRule import OptimizerRule
from Query import QueryTree

# RULES 3
class ProjectionSimplification(OptimizerRule):
    def apply_rule(self, querytree: QueryTree):
        if not querytree.childs:
            return querytree

        for i, child in enumerate(querytree.childs):
            querytree.childs[i] = self.apply_rule(querytree.childs[i])

        # cek projection
        if querytree.childs[0].type == "subquery" and querytree.type == 'projection':
            # ada sub query
            if len(querytree.childs[0].childs) == 1 and querytree.childs[0].childs[0].type == 'projection':  
                child = querytree.childs[0].childs[0]
                querytree.childs = child.childs
    
        if querytree.type == "projection":
            return querytree

        return querytree
