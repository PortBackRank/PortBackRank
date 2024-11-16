from abc import ABC, abstractmethod
from typing import Callable, Dict, Any
from data import Data
import pandas as pd

class Ranker(ABC):
    def __init__(self, data=None):
        self._data = data if data is not None else Data()

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


class RankerMediasMoveis(Ranker):
    def exibir_resultados(self, resultados: Dict[str, float]):
        print("Resultados do Ranqueamento (Médias Móveis):")
        results = []
        for ativo, media in sorted(resultados.items(), key=lambda item: item[1], reverse=True):
            results.append(f"{ativo}: {media:.2f}")

        df_resultados = pd.DataFrame(list(resultados.items()), columns=['Ativo', 'MediaMovel'])
        df_resultados.to_csv('resultados.csv', index=False)

        df_carregado = pd.read_csv('resultados.csv')
        print(df_carregado)
            
def medias_moveis(data, params: Dict[str, Any]) -> Dict[str, float]:
    periodo = params.get("periodo", 5)
    resultados = {}

    for ativo in data.extrair_simbolos():
        historico = data.buscar_historico_filtro([ativo], filtro_coluna="Close")
        # print(historico)
        close_values = historico["Close"]

        media_movel = close_values.iloc[-periodo:].mean()
        resultados[ativo] = media_movel

    return resultados
# 
ranker = RankerMediasMoveis()

ranker.adicionar_algoritmo(medias_moveis, {"periodo": 3})

resultados = ranker.executar_algoritmo()

ranker.exibir_resultados(resultados)


# class RankerSimples(Ranker):
#     def exibir_resultados(self, resultados: Dict[str, float]):
#         print("Resultados do Ranqueamento:")
#         for ativo, pontuacao in sorted(resultados.items(), key=lambda item: item[1], reverse=True):
#             print(f"{ativo}: {pontuacao:.2f}")


# def algoritmo_exemplo(data: Dict[str, list], params: Dict[str, Any]) -> Dict[str, float]:
#     return {ativo: sum(valores) / len(valores) for ativo, valores in data.items()}


# ranker = RankerSimples()

# ranker.adicionar_algoritmo(algoritmo_exemplo, {"peso": 1.0})

# dados = {"ativo_a": [1, 2, 3], "ativo_b": [8, 6, 4], "ativo_c": [5, 6]}
# ranker.carregar_dados(dados)

# resultados = ranker.executar_algoritmo()

# ranker.exibir_resultados(resultados)
