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
from queryOptimizer.classes.QueryCost import QueryCost
import re

rules = [
    ProjectionSimplification.ProjectionSimplification(),
    NaturalJoinCommutative.NaturalJoinCommutative(),
    CombineSelectionCartesian.CombineSelectionCartesian(),
    ProjectionDistribution.ProjectionDistribution(),
    ThetaJoinCommutative.ThetaJoinCommutative(),
    SelectionCommutative.SelectionCommutative(),
    ConjungtiveSelection.ConjungtiveSelection(),
    SelectionDistribution.SelectionDistribution()
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
        # print(query)
        return tree.convert_tree(query)
    
    def optimize_query(self, query: str)->ParsedQuery:
        query_tree = self.parse_query(query)

        tree = TreeManager(self.storage,self.schemas)
        # tree.display_tree(query_tree, 0)
        # cost = self.get_cost(query_tree)

        optimizer = GeneticOptimizer(
            query_tree=query_tree,
            rules=rules,
            population_size=20,
            max_generations=10,
            mutation_rate=0.1,
            schemas=self.schemas,
            storage=self.storage
        )

        best_solution = optimizer.optimize()
        for rule in best_solution:
            rule.apply_rule(query_tree)


        # tree.display_tree(query_tree, 0)
        # cost = self.get_cost(query_tree)
        # print(cost)
        return query_tree
    
    def get_cost(self, parsed_query: ParsedQuery) -> int:
        cost = QueryCost(self.schemas,self.storage)
        return cost.get_cost(parsed_query)
