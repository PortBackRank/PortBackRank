from abc import ABC

from datetime import datetime

from typing import List
import random
from data import Data


class Ranker(ABC):
    def __init__(self, parametros: dict = None, date: str = None):
        """
        Construtor da classe Ranker que define parâmetros padrões para a estratégia de investimento.

        :param parametros: Dicionário de parâmetros opcionais para a estratégia.
        :param date: Data como string (formato 'YYYY-MM-DD'). Caso não seja fornecida, será usada a data atual.
        """

        self.data = Data()

        self.date = date or datetime.now().strftime('%Y-%m-%d')

        self.parametros = parametros or {}

    def rank(self) -> List[str]:
        """
        Método abstrato que deve ser implementado pelas classes filhas.

        :param dados: Lista de dicionários com os dados das ações.
        :return: Lista de ações ranqueadas.
        """
        pass


class RandomRanker(Ranker):
    def rank(self) -> List[str]:
        """
        Gera um ranking aleatório de símbolos com base nos dados obtidos da instância `Data`.

        :return: Lista de símbolos em ordem aleatória.
        """

        simbolos = self.data.list_symbols()

        random.shuffle(simbolos)

        return simbolos[:10]


ranker = RandomRanker()
ranking = ranker.rank()
print(f"Ranking gerado em {ranker.date}: {ranking}")
