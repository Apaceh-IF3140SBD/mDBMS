import random

class GeneticOptimizer():
    def __init__(self, query_tree, rules, population_size, max_generations, mutation_rate):
        self.population_size = population_size
        self.max_generations = max_generations
        self.mutation_rate = mutation_rate
        self.query_tree = query_tree  
        self.rules = rules            

    def initialize_population(self):
        return [random.sample(self.rules, len(self.rules)) for _ in range(self.population_size)]

    def calculate_fitness(self, individual):
        tree_copy = self.query_tree.copy()
        for rule in individual:
            tree_copy = rule.apply_rule(tree_copy)
        return len(tree_copy.childs)

    def selection(self, population):
        population.sort(key=self.calculate_fitness, reverse=True)
        return population[:self.population_size // 2]

    def crossover(self, parent1, parent2):
        point = random.randint(1, len(parent1) - 1)
        child1 = parent1[:point] + [rule for rule in parent2 if rule not in parent1[:point]]
        child2 = parent2[:point] + [rule for rule in parent1 if rule not in parent2[:point]]
        return child1, child2

    def mutate(self, individual):
        i, j = random.sample(range(len(individual)), 2)
        individual[i], individual[j] = individual[j], individual[i]
        return individual

    def optimize(self):
        population = self.initialize_population()

        for i in range(self.max_generations):
            selected_population = self.selection(population)

            next_population = []
            for _ in range(self.population_size // 2):
                parent1 = random.choice(selected_population)
                parent2 = random.choice(selected_population)
                child1, child2 = self.crossover(parent1, parent2)
                next_population.extend([child1, child2])

            population = [
                self.mutate(ind) if random.random() < self.mutation_rate else ind
                for ind in next_population
            ]

        best_individual = max(population, key=self.calculate_fitness)
        return best_individual