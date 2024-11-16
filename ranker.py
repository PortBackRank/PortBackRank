from abc import ABC, abstractmethod
from typing import Callable, Dict, Any


class Ranker(ABC):
    def __init__(self):
        self._data = None
        self._algoritmo = None
        self._parametros = {}

    def adicionar_algoritmo(self, algoritmo: Callable[[Dict[str, list], Dict[str, Any]], Dict[str, float]], parametros: Dict[str, Any]):
        """
        Adiciona um algoritmo de ranqueamento e seus parâmetros padrão.
        :param algoritmo: Função que executa o ranqueamento.
        :param parametros: Parâmetros padrão do algoritmo.
        """
        if not callable(algoritmo):
            raise TypeError("O algoritmo deve ser uma função ou callable.")
        self._algoritmo = algoritmo
        self._parametros = parametros

    def carregar_dados(self, dados: Dict[str, list]):
        """
        Carrega os dados para o ranqueamento.
        :param dados: Dicionário contendo listas de valores históricos por ativo.
        """
        if not isinstance(dados, dict) or not all(isinstance(v, list) for v in dados.values()):
            raise ValueError("Os dados devem ser um dicionário com listas como valores.")
        self._data = dados

    def executar_algoritmo(self) -> Dict[str, float]:
        """
        Executa o algoritmo de ranqueamento com os dados e parâmetros atuais.
        :return: Resultados do ranqueamento.
        """
        if not self._algoritmo:
            raise ValueError("Nenhum algoritmo foi adicionado ao Ranker.")
        if not self._data:
            raise ValueError("Nenhum dado foi carregado.")
        
        print("Executando algoritmo de ranqueamento...")
        return self._algoritmo(self._data, self._parametros)

    @abstractmethod
    def exibir_resultados(self, resultados: Dict[str, float]):
        """
        Método abstrato para exibir os resultados do ranqueamento.
        Deve ser implementado por subclasses.
        :param resultados: Dicionário com os resultados do ranqueamento.
        """
        pass


class RankerSimples(Ranker):
    def exibir_resultados(self, resultados: Dict[str, float]):
        print("Resultados do Ranqueamento:")
        for ativo, pontuacao in sorted(resultados.items(), key=lambda item: item[1], reverse=True):
            print(f"{ativo}: {pontuacao:.2f}")


def algoritmo_exemplo(data: Dict[str, list], params: Dict[str, Any]) -> Dict[str, float]:
    return {ativo: sum(valores) / len(valores) for ativo, valores in data.items()}


ranker = RankerSimples()

ranker.adicionar_algoritmo(algoritmo_exemplo, {"peso": 1.0})

dados = {"ativo_a": [1, 2, 3], "ativo_b": [8, 6, 4], "ativo_c": [5, 6]}
ranker.carregar_dados(dados)

resultados = ranker.executar_algoritmo()

ranker.exibir_resultados(resultados)
