from queryOptimizer.classes.OptimizerRule import OptimizerRule
from queryOptimizer.classes.Query import QueryTree

class ProjectionDistribution(OptimizerRule):
    def __init__(self):
        self.global_projection = None  

    def collect_projections(self, querytree: QueryTree):
        for child in querytree.childs:
            self.collect_projections(child)

        if querytree.type == "projection":
            self.projection= querytree
            new_projection = querytree.val["attributes"]
            self.global_projection = new_projection

    def apply_rule(self, querytree: QueryTree) -> QueryTree:
        self.collect_projections(querytree)
        for i, child in enumerate(querytree.childs):
            querytree.childs[i] = self.apply_rule(child)

        if querytree.type == "join":
            nodes = {}
            projection_node = QueryTree("projection", nodes, None, querytree)
            selected_node = None
            for child in querytree.childs:
                if child.type == "table":
                    if child.val in self.global_projection:
                        nodes["attributes"] = {child.val: self.global_projection[child.val]}
                        projection_node.childs.append(child)
                        selected_node = child

                        if len(projection_node.childs) > 0:
                            querytree.childs.remove(selected_node)
                            querytree.childs.insert(0, projection_node)
                        nodes = {}
                        projection_node = QueryTree("projection", nodes, None, querytree)

        return querytree
