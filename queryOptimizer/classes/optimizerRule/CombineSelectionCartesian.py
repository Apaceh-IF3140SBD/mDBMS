from queryOptimizer.classes.OptimizerRule import OptimizerRule
from queryOptimizer.classes.Query import QueryTree

# RULES 4
class CombineSelectionCartesian(OptimizerRule):
    def apply_rule(self, querytree: QueryTree, selection=None) -> QueryTree:
        if not querytree.childs:
            return querytree

        for i, child in enumerate(querytree.childs):
            querytree.childs[i] = self.apply_rule(child)

        if querytree.type == "selection":
            selection = []
            new_selection = querytree.val['conditions']
            selection.extend(new_selection)

            if querytree.childs[0].type == "join" and  querytree.childs[0].val['natural'] == False:
                querytree.childs[0] = self._rebuild_joins(querytree.childs[0], selection)
            if len(querytree.childs)>1 and querytree.childs[0].type == "table" and querytree.childs[1].type == "table":
                newchild = self._rebuild_joins2(querytree, selection)
                querytree.childs.clear()
                querytree.childs.append(newchild)
            if (len(querytree.val["conditions"])>0 and len(querytree.val["conditions"][0])==0):
                del querytree.val["conditions"][0]

        return querytree
    
    def _rebuild_joins(self, node, selection) -> list[QueryTree]:
        for child in  node.childs:
            if child.type == "table": 
                if len(selection)>0: 
                    for sel in selection[0]: 
                        value = sel[0].split('.')[0]
                        print(value, child.val)
                        if child.val == value:
                            node.val['conditions'][0].append(sel)
                            selection[0].remove(sel)
                        
        return node 
    
    # for cartesian products
    def _rebuild_joins2(self, node, selection) -> list[QueryTree]:
        nodes = {}
        nodes["natural"] = False
        join_node = QueryTree("join", nodes, None, node)
        
        for i in range(len(node.childs)):
            if node.childs[i].type == "table":
                for sel in selection[0]: 
                    value = sel[0].split('.')[0]
                    value2 = sel[2].split('.')[0]
                    if(i+1<len(node.childs)):

                        if (node.childs[i].val.strip() == value.strip() and node.childs[i+1].val.strip() == value2.strip()) or (node.childs[i+1].val.strip() == value.strip() and node.childs[i].val.strip() == value2.strip()):
                            join_node.val['conditions']=sel
                            selection[0].remove(sel)
                            join_node.childs.append(node.childs[i])
                            join_node.childs.append(node.childs[i+1])
                            i+=1
                        i+=1
       
        return join_node 
