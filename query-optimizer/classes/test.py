from Query import ParsedQuery, QueryTree
from TreeManager import TreeManager
from OptimizationEngine import OptimizationEngine

query = "SELECT ayam, bebek, ikan FROM (Pegawai NATURAL JOIN Departemen) NATURAL JOIN Proyek WHERE X = 13 AND Y > 100 ORDER BY A LIMIT 10;"

parser = OptimizationEngine()
query_tree = QueryTree()
tree_mng = TreeManager()

tree = parser.parse_query(query)
# print("=====================================")
# print(tree.val)

tree_mng.display_tree(tree, 0)
# print(tree.childs)
