
"""
Ranker module for the investment strategy.
"""

from abc import ABC, abstractmethod
from typing import List
import random
from data import MemData


class Ranker(ABC):
    """class abstract Ranker"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        """
        Constructor for the Ranker class, 
        which defines default parameters for the investment strategy.

        :param parameters: Optional dictionary of parameters for the strategy.
        :param interval: List of two strings representing the start and end dates of the data to be used.
        :param data: Data instance to be used for the strategy.
            If not provided, the current date will be used.
        """
        self.interval = interval

        self.data = data
        self.parameters = parameters or {}

    @abstractmethod
    def rank(self, date: str = None) -> List[str]:
        """
        Abstract method that must be implemented by subclasses.

        :return: List of ranked stock symbols.
        """


class RandomRanker(Ranker):
    """RandomRanker class"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        """
        Constructor for the RandomRanker class, allowing for an optional seed for reproducibility.

        :param parameters: Optional dictionary of parameters for the strategy.
        :param date: List of two strings representing the start and end dates of the data to be used.
        :param data: Data instance to be used for the strategy.
        :param seed: Optional seed for randomization.
        """
        super().__init__(parameters, interval, data)
        self.seed = self.parameters.get("SEED", 42)

    def rank(self, date: str = None) -> List[str]:
        """
        Generates a random ranking of symbols based on the data retrieved from the `Data` instance.

        :return: List of symbols in random order.
        """
        symbols = self.data.get_assets()

        if self.seed is not None:
            random.seed(self.seed)

        random.shuffle(symbols)

        return symbols


def test_random_ranker():
    """
    Função simples para testar o funcionamento do RandomRanker.
    """
    interval = ["2024-01-10", "2024-11-10"]
    data = MemData(interval=interval)

    parameters = {"SEED": 42}

    ranker = RandomRanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank()

    print("Símbolos ranqueados aleatoriamente:", ranked_symbols)


if __name__ == "__main__":
    test_random_ranker()
