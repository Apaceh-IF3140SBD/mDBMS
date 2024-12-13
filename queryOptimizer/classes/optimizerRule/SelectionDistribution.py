from queryOptimizer.classes.OptimizerRule import OptimizerRule
from queryOptimizer.classes.Query import QueryTree

# RULES 7
class SelectionDistribution(OptimizerRule):
    def apply_rule(self, query_tree: QueryTree) -> QueryTree:
        if not query_tree.childs:
            return query_tree

        for i, child in enumerate(query_tree.childs):
            query_tree.childs[i] = self.apply_rule(child)

        if query_tree.type == "selection":
            conditions = query_tree.val.get("conditions", [])
            for child in query_tree.childs:
                if child.type in ["join"]:
                    nodes = {}
                    selected_node = None
                    projection_node = QueryTree(type="selection", val=nodes, parent=query_tree)
                    for child2 in child.childs: 
                        if len(conditions)>0:
                            for cond in conditions[0]:
                                table = cond[0].split('.')[0]
                                if child2.type =='table' and table == child2.val:
                                    nodes["conditions"] = [[cond]]
                                    projection_node.childs.append(child2)
                                    selected_node = child2
                                    conditions[0].remove(cond)
                            if len(projection_node.childs) > 0:
                                child.childs.remove(selected_node)
                                child.childs.insert(0, projection_node)
                                nodes = {}
                                projection_node = QueryTree(type="selection", val=nodes, parent=query_tree)
                    if len(query_tree.val["conditions"])>0 and len(query_tree.val["conditions"][0])==0:
                        del query_tree.val["conditions"][0]


        return query_tree
