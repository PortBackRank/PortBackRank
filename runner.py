from ranker import RandomRanker
from typing import List, Dict
from datetime import datetime, timedelta
from data import Data
from ranker import Ranker


def gerar_intervalo_de_tempo(data_inicio: str, data_fim: str) -> List[str]:
    """
        Gera uma lista de datas entre `data_inicio` e `data_fim`.

        :return: Lista de strings representando as datas no intervalo.
    """

    start_date = datetime.strptime(data_inicio, "%Y-%m-%d")
    end_date = datetime.strptime(data_fim, "%Y-%m-%d")
    delta = timedelta(days=1)

    dates = []
    while start_date <= end_date:
        dates.append(start_date.strftime("%Y-%m-%d"))
        start_date += delta

    return dates


class Runner:
    def __init__(
        self,
        ranker: Ranker,
        data_inicio: str,
        data_fim: str,
        lucro_percentual: float,
        compra_percentual: float,
        venda_percentual: float,
        caixa_inicial: float = 100000.0,
    ):
        """
        Inicializa o Runner com os parâmetros necessários para simular a estratégia.

        :param ranker: Instância da classe Ranker que será usada para ranquear ações.
        :param data_inicio: Data de início da simulação (formato 'YYYY-MM-DD').
        :param data_fim: Data de término da simulação (formato 'YYYY-MM-DD').
        :param lucro_percentual: Percentual mínimo para realizar uma venda.
        :param compra_percentual: Percentual do caixa usado em cada compra.
        :param venda_percentual: Percentual do portfólio a ser vendido em cada venda.
        :param caixa_inicial: Quantia inicial disponível em caixa.
        """
        self.ranker = ranker
        self.data_inicio = data_inicio
        self.data_fim = data_fim
        self.lucro_percentual = lucro_percentual
        self.compra_percentual = compra_percentual
        self.venda_percentual = venda_percentual
        self.caixa = caixa_inicial

        # representando as ações compradas (símbolo, quantidade, preço médio, etc.)
        self.__portfolio: List[Dict[str, float]] = []

        self.parametros_estrategia = ranker.parametros

        self.data = Data()

    def simular(self):
        """
        Executa a simulação de compras e vendas entre a data de início e a data de fim.
        """
        print(f"Iniciando simulação de {self.data_inicio} até {self.data_fim}")

        dados_historicos = self.data.get_history_interval(
            assets=self.data.list_symbols(),
            start_date=self.data_inicio,
            end_date=self.data_fim,
        )

        print(f"Datas disponíveis: {dados_historicos.keys()}")

        for date, dados_do_dia in dados_historicos.items():
            print(f"Processando data: {date}")
            ranking = self.ranker.rank()
            print(f"Ranking : {ranking}")
            self._processar_compras(ranking, dados_do_dia)
            self._processar_vendas(ranking, dados_do_dia)

        print("Simulação finalizada.")
        print(f"Caixa final: {self.caixa}")
        print(f"Portfólio final: {self.__portfolio}")

    def _processar_compras(self, ranking: List[str], dados: List[Dict[str, float]]):
        """
        Realiza as compras de ações com base no ranking e nos dados do dia.

        :param ranking: Lista de símbolos ranqueados.
        :param dados: Lista de dicionários com os dados do mercado.
        """
        pass

    def _processar_vendas(self, ranking: List[str], dados: List[Dict[str, float]]):
        """
        Realiza as vendas de ações com base nos dados do dia e no portfólio.

        :param ranking: Lista de símbolos ranqueados.
        :param dados: Lista de dicionários com os dados do mercado.
        """
        pass


ranker = RandomRanker(
    date="2024-11-04", seed=42)

runner = Runner(
    ranker=ranker,
    data_inicio="2024-01-01",
    data_fim="2024-11-04",
    lucro_percentual=5.0,
    compra_percentual=0.10,
    venda_percentual=0.50,
    caixa_inicial=100000.0,
)

runner.simular()
