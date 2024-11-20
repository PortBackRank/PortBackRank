from abc import ABC, abstractmethod
from typing import List
from data import Data


class Ranker(ABC):
    def __init__(self, lucro_percentual=0.1, compra_percentual=0.2, venda_percentual=0.2):
        """
        Construtor da classe Ranker que define parâmetros padrões para a estratégia de investimento.

        :param lucro_percentual: Percentual de lucro esperado para a estratégia.
        :param compra_percentual: Percentual do valor para comprar.
        :param venda_percentual: Percentual do valor para vender.
        """
        self.lucro_percentual = lucro_percentual
        self.compra_percentual = compra_percentual
        self.venda_percentual = venda_percentual

        # Instancia da classe Data
        self.data = Data()

    @abstractmethod
    def rank_ativos(self, dados: List[dict]) -> List[dict]:
        """
        Método abstrato que deve ser implementado nas subclasses para classificar os ativos.
        """
        pass

    def rank(self, dados: List[dict]) -> List[dict]:
        """
        Método para realizar o ranking dos ativos com base nos dados fornecidos e na estratégia.

        :param dados: Lista de dicionários representando ativos.
        :return: Lista de ativos ordenados conforme a estratégia definida na subclasse.
        """
        return self.rank_ativos(dados)
