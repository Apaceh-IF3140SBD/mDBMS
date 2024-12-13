from queryOptimizer.classes.Query import ParsedQuery
from queryOptimizer.classes.TreeManager import TreeManager
from queryOptimizer.classes.GeneticOptimizer import GeneticOptimizer
from queryOptimizer.classes.optimizerRule import ProjectionSimplification
from queryOptimizer.classes.optimizerRule import NaturalJoinCommutative
from queryOptimizer.classes.optimizerRule import CombineSelectionCartesian
from queryOptimizer.classes.optimizerRule import ThetaJoinCommutative
from queryOptimizer.classes.optimizerRule import ProjectionDistribution
from queryOptimizer.classes.optimizerRule import SelectionCommutative
from queryOptimizer.classes.optimizerRule import ConjungtiveSelection
from queryOptimizer.classes.optimizerRule import SelectionDistribution
import re

rules = [
    ProjectionSimplification(),
    NaturalJoinCommutative(),
    CombineSelectionCartesian(),
    ProjectionDistribution(),
    ThetaJoinCommutative(),
    SelectionCommutative(),
    ConjungtiveSelection(),
    SelectionDistribution()
]


class OptimizationEngine:
    def __init__(self,storageEngine, schemas):
        self.storage = storageEngine
        self.schemas = schemas

    def parse_query(self, query: str)->ParsedQuery:
        query = re.sub(r'(<>|[<>]=?|=|[();*])', r' \1 ', query)
        query = re.sub(r'\s{2,}', ' ', query).strip() 


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
            # print(rule)
            # tree.display_tree(query_tree, 0)

        print('after optimized query')

        # projection_simplification = SelectionDistribution()
        # optimized_query = projection_simplification.apply_rule(query_tree)
        tree.display_tree(query_tree, 0)
        return query_tree
    
    def get_cost(self, query)->int:

        return 0
    