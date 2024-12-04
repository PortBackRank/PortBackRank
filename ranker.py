
"""
Ranker module for the investment strategy.
"""

from abc import ABC
from datetime import datetime
from typing import List
import random
from data import Data


class Ranker(ABC):
    """class abstract Ranker"""

    def __init__(self, parameters: dict = None, date: str = None):
        """
        Constructor for the Ranker class, 
        which defines default parameters for the investment strategy.

        :param parameters: Optional dictionary of parameters for the strategy.
        :param date: Date as a string (format 'YYYY-MM-DD').
            If not provided, the current date will be used.
        """
        self.data = Data()
        self.date = date or datetime.now().strftime('%Y-%m-%d')
        self.parameters = parameters or {}

    def rank(self) -> List[str]:
        """
        Abstract method that must be implemented by subclasses.

        :return: List of ranked stock symbols.
        """


class RandomRanker(Ranker):
    """RandomRanker class"""

    def __init__(self, parameters: dict = None, date: str = None, seed: int = 42):
        """
        Constructor for the RandomRanker class, allowing for an optional seed for reproducibility.

        :param parameters: Optional dictionary of parameters for the strategy.
        :param date: Date as a string (format 'YYYY-MM-DD').
            If not provided, the current date will be used.
        :param seed: Optional seed for randomization.
        """
        super().__init__(parameters, date)
        self.seed = seed

    def rank(self) -> List[str]:
        """
        Generates a random ranking of symbols based on the data retrieved from the `Data` instance.

        :return: List of symbols in random order.
        """
        symbols = self.data.list_symbols()

        if self.seed is not None:
            random.seed(self.seed)

        random.shuffle(symbols)

        return symbols


def test_random_ranker():
    """
    Função simples para testar o funcionamento do RandomRanker.
    """
    ranker = RandomRanker(seed=42)
    ranked_symbols = ranker.rank()

    print("Símbolos ranqueados aleatoriamente:", ranked_symbols)


if __name__ == "__main__":
    test_random_ranker()
