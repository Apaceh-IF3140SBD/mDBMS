from queryOptimizer.classes.OptimizerRule import OptimizerRule
from queryOptimizer.classes.Query import QueryTree

class ConjungtiveSelection(OptimizerRule):
    def apply_rule(self, query_tree: QueryTree) -> QueryTree:
        if not query_tree.childs:
            return query_tree

        for i, child in enumerate(query_tree.childs):
            query_tree.childs[i] = self.apply_rule(child)

        if query_tree.type == "selection":
            conditions = query_tree.val.get("conditions", [])
            for child in query_tree.childs:
                if child.type in ["table"] and len(conditions[0])>1:
                    nodes = {}
                    selected_node = None
                    condremove = []
                    projection_node = QueryTree(type="selection", val=nodes, parent=query_tree)
                    for cond in conditions[0]:
                        
                        table = cond[0].split('.')[0]
                        if child.type =='table' and table == child.val:
                            if nodes:
                                column_list = nodes.get("conditions")
                                column_list[0].append(cond)
                                nodes.update({"conditions": column_list})
                            else:
                                nodes["conditions"] = [[cond]]  
                            selected_node = child
                            condremove.append(cond)
                    if nodes:
                        projection_node.childs.append(child)
                    if len(projection_node.childs) > 0:
                        query_tree.childs.remove(selected_node)
                        query_tree.childs.insert(0, projection_node)
                        nodes = {}
                        projection_node = QueryTree(type="selection", val=nodes, parent=query_tree)
                    for cond in condremove:
                        conditions[0].remove(cond)


        return query_tree
