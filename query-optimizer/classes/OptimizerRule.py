from abc import ABC, abstractmethod

class OptimizerRule(ABC):
    @abstractmethod
    def apply_rule(self):
        pass