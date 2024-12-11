from OptimizerRule import OptimizerRule
from Query import QueryTree

class ConjungtiveSelection(OptimizerRule):
    def apply_rule(self, query_tree: QueryTree):

        if not query_tree.childs:
            return query_tree
        
        for i, child in enumerate(query_tree.childs):
            query_tree.childs[i] = self.apply_rule(child)

        if query_tree.type == "selection":
            combined_conditions = []
            new_childs = []

            for child in query_tree.childs:
                if child.type == "condition":
                    combined_conditions.append(child.val)
                elif child.type == "operator" and child.val == "AND":
                    combined_conditions.append("AND")
                else:
                    new_childs.append(child)

            # Gabungkan kondisi menjadi satu ekspresi
            if combined_conditions:
                combined_expr = " ".join(combined_conditions)
                if "AND" in combined_expr:
                    # Pecah ekspresi berdasarkan AND
                    conditions = combined_expr.split(" AND ")
                    for condition in conditions:
                        new_condition_node = QueryTree(type="condition", val=condition.strip(), parent=query_tree)
                        new_childs.append(new_condition_node)
                else:
                    # Jika tidak ada AND, tambahkan kondisi seperti biasa
                    new_childs.append(QueryTree(type="condition", val=combined_expr, parent=query_tree))

            query_tree.childs = new_childs

        return query_tree
