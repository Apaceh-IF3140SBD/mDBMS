from Query import ParsedQuery
from TreeManager import TreeManager
from GeneticOptimizer import GeneticOptimizer
from optimizerrule.ProjectionSimplification import ProjectionSimplification
from optimizerrule.NaturalJoinCommutative import NaturalJoinCommutative
from optimizerrule.CombineSelectionCartesian import CombineSelectionCartesian
from optimizerrule.ThetaJoinCommutative import ThetaJoinCommutative
from optimizerrule.ProjectionDistribution import ProjectionDistribution
from optimizerrule.SelectionCommutative import SelectionCommutative
import re

rules = [
    ProjectionSimplification(),
    NaturalJoinCommutative(),
    CombineSelectionCartesian(),
    ProjectionDistribution(),
    ThetaJoinCommutative(),
    SelectionCommutative()
]


class OptimizationEngine:
    def __init__(self,storageEngine, schemas):
        self.storage = storageEngine
        self.schemas = schemas

    def parse_query(self, query: str)->ParsedQuery:
        query = re.sub(r'([();])', r' \1 ', query)
        clear_query = query.replace(",", " ")
        parsed_query = re.findall(r'\S+', clear_query)
        # print(parsed_query)
        tree = TreeManager(self.storage,self.schemas)
        query = tree.parse_tree_to_dict(parsed_query)
        return tree.convert_tree(query)
    
    def optimize_query(self, query: str)->ParsedQuery:
        query_tree = self.parse_query(query)

        print('before optimize query')
        tree = TreeManager(self.storage,self.schemas)
        tree.display_tree(query_tree, 0)

        optimizer = GeneticOptimizer(
            query_tree=query_tree,
            rules=rules,
            population_size=20,
            max_generations=50,
            mutation_rate=0.1
        )

        best_solution = optimizer.optimize()
        for rule in best_solution:
            rule.apply_rule(query_tree)

        print('after optimized query')

        # projection_simplification = SelectionCommutative()
        # optimized_query = projection_simplification.apply_rule(query_tree)
        tree.display_tree(query_tree, 0)
        return query_tree
    
    def get_cost(self, query)->int:

        return 0
    