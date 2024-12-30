'''
    class Runner
'''

from typing import List, Dict, Type
import itertools

from datetime import datetime
import pandas as pd
from ranker import Ranker
from ranker import RandomRanker
from data import MemData


class Runner:
    def __init__(self, profit, loss, diversification, ranker: Type[Ranker], ranker_ranges, data: MemData):
        """
        Inicializa a classe Runner com os parâmetros fornecidos.

        :param profit: Lucro alvo para venda (porcentagem).
        :param loss: Limite de perda para venda (porcentagem).
        :param diversification: Porcentagem máxima para cada setor (porcentagem).
        :param ranker: Classe do ranker a ser utilizada.
        :param ranker_ranges: Faixas de valores para cada parâmetro do ranker.
                              Exemplo: {'SEED': [0, 1, 42]}
        """
        self.profit = profit
        self.loss = loss
        self.diversification = diversification

        self.ranker = ranker
        self.ranker_ranges = ranker_ranges

        # representando as ações compradas (símbolo, quantidade, preço médio, etc.)
        self.__portfolio: List[Dict[str, float]] = []

        self.data = data

        self.caixa = 0

    def _gen_ranker_confs(self, ranker_ranges: Dict[str, List[float]]) -> List[Dict[str, float]]:
        """
        Gera todas as combinações possíveis de parâmetros para o ranker.

        :param ranker_ranges: Dicionário com os parâmetros e seus respectivos valores possíveis.
        :return: Lista de dicionários com todas as combinações possíveis de parâmetros.
        """

        param_names = list(ranker_ranges.keys())
        param_values = list(ranker_ranges.values())

        combinations = list(itertools.product(*param_values))

        return [dict(zip(param_names, comb)) for comb in combinations]

    def _single_run(self, interval: List[str], ranker_conf: Dict[str, float], capital: float) -> Dict:
        """
        Executa uma simulação para uma única configuração de ranker, mantendo o portfólio com a quantidade e o preço de compra dos ativos.

        :param interval: Lista com a data inicial e final da simulação.
        :param ranker_conf: Configuração do ranker a ser utilizada.
        :param capital: Capital inicial.
        :return: Estado final do portfólio.
        """

        ranker = self.ranker(parameters=ranker_conf, data=self.data)

        start_date, end_date = interval

        self.caixa = capital
        self.__portfolio = []

        for date in pd.date_range(start_date, end_date).strftime('%Y-%m-%d'):
            self._sell(date)
            self._buy(date, ranker)

        return {'caixa': self.caixa, 'portfolio': self.__portfolio}

    def _sell(self, date: str):
        """
        Vende ativos que atingiram o percentual de lucro ou perda,
        respeitando a ordem FIFO e verificando o volume diário.

        :param date: Data atual para verificar se algum ativo atendeu ao critério de venda.
        """
        dados_historicos = self.data.get_all_history()

        historicos_ativos = {}

        # TODO: Pensar em uma forma de otimizar isso
        for simbolo in [item['simbolo'] for item in self.__portfolio]:
            historico = dados_historicos.get(simbolo)
            if historico is not None and not historico.empty:
                historico_data = historico.loc[
                    historico['Date'].dt.date == datetime.strptime(
                        date, '%Y-%m-%d').date()
                ]
                if not historico_data.empty:
                    historicos_ativos[simbolo] = {
                        'preco_atual': historico_data['Close'].iloc[0],
                        'volume_diario': historico_data['Volume'].iloc[0]
                    }

        novos_portfolio = []
        for item in self.__portfolio:
            simbolo = item['simbolo']
            preco_compra = item['preco_compra']
            quantidade = item['quantidade']
            data_compra = item['data_compra']

            if simbolo not in historicos_ativos:
                novos_portfolio.append(item)
                continue

            preco_atual = historicos_ativos[simbolo]['preco_atual']
            volume_diario = historicos_ativos[simbolo]['volume_diario']

            percentual_variacao = (preco_atual - preco_compra) / preco_compra

            if percentual_variacao >= self.profit or percentual_variacao <= -self.loss:
                quantidade_vender = min(quantidade, volume_diario)
                valor_venda = preco_atual * quantidade_vender
                self.caixa += valor_venda

                if quantidade > quantidade_vender:
                    novos_portfolio.append({
                        'simbolo': simbolo,
                        'quantidade': quantidade - quantidade_vender,
                        'preco_compra': preco_compra,
                        'data_compra': data_compra,
                        'sector': item['sector']
                    })

            else:
                novos_portfolio.append(item)

        self.__portfolio = novos_portfolio

    def _buy(self, date: str, ranker: Ranker):
        """
        Compra ativos com base no ranking, respeitando diversificação por setor
        e verificando o volume diário disponível.

        :param date: Data atual para comprar ativos.
        :param ranker: Instância do ranker a ser utilizado para definir os ativos.
        """
        ranked_symbols = ranker.rank()
        if not ranked_symbols:
            return

        dados_historicos = self.data.get_all_history()

        total_portfolio_value = sum(
            item['preco_compra'] * item['quantidade'] for item in self.__portfolio
        )

        setor_percentual = {}

        if total_portfolio_value > 0:
            for item in self.__portfolio:
                setor = item.get('sector')
                preco_compra = item.get('preco_compra', 0)
                quantidade = item.get('quantidade', 0)

                if not setor:
                    print(f"Setor não encontrado para o símbolo {
                          item['simbolo']}.")
                    continue

                valor_item = preco_compra * quantidade

                setor_percentual[setor] = setor_percentual.get(
                    setor, 0) + (valor_item / total_portfolio_value)

        caixa_disponivel = self.caixa

        for simbolo in ranked_symbols:
            if caixa_disponivel <= 2:  # Valor mínimo para comprar uma ação, mudar depois
                break

            ativo_info = self.data.get_info(simbolo)
            if ativo_info.empty:
                continue

            setor = ativo_info.iloc[0]['sector']

            max_investimento_setor = (
                caixa_disponivel * self.diversification if setor not in setor_percentual
                else total_portfolio_value * self.diversification -
                setor_percentual.get(setor, 0) * total_portfolio_value
            )

            historico = dados_historicos.get(simbolo)
            if historico is None or historico.empty:
                continue

            historico_data = historico.loc[
                historico['Date'].dt.date == datetime.strptime(
                    date, '%Y-%m-%d').date()
            ]

            if historico_data.empty:
                continue

            preco_atual = historico_data['Close'].iloc[0]

            volume_diario = historico_data['Volume'].iloc[0]

            if pd.isna(preco_atual) or pd.isna(volume_diario):
                continue

            quantidade_max = int(caixa_disponivel // preco_atual)
            quantidade_setor = int(max_investimento_setor // preco_atual)
            quantidade_comprar = min(
                quantidade_max, quantidade_setor, volume_diario)

            if quantidade_comprar <= 0:
                continue

            self.__portfolio.append({
                'simbolo': simbolo,
                'quantidade': quantidade_comprar,
                'preco_compra': preco_atual,
                'data_compra': date,
                'sector': setor
            })

            caixa_disponivel -= quantidade_comprar * preco_atual
            total_portfolio_value += quantidade_comprar * preco_atual
            setor_percentual[setor] = setor_percentual.get(setor, 0) + (
                quantidade_comprar * preco_atual / total_portfolio_value
            )

        self.caixa = caixa_disponivel

    def _run(self, interval: List[str], capital: float) -> List[Dict]:
        """
        Executa a simulação para todas as configurações do ranker.

        :param interval: Intervalo de datas para a simulação.
        :param capital: Capital inicial.        
        :return: Lista com os resultados de todas as simulações.
        """
        ranker_confs = self._gen_ranker_confs(self.ranker_ranges)
        results = []

        start_time = datetime.now()
        for ranker_conf in ranker_confs:
            # {'SEED': 0}
            result = self._single_run(interval, ranker_conf, capital)

            results.append(result)

        end_time = datetime.now()
        print(f"Total time for running all configurations: {
              end_time - start_time}")

        return results


def test_runner():
    interval = ["2024-01-10", "2024-11-10"]
    capital = 10000
    ranker_ranges = {"SEED": [0, 1, 42]}

    runner = Runner(
        profit=0.1,
        loss=0.05,
        diversification=0.2,
        ranker=RandomRanker,
        ranker_ranges=ranker_ranges,
        data=MemData(interval)
    )

    try:
        results = runner._run(interval, capital)

        assert len(results) > 0, "Nenhum resultado gerado"
        print("Execução do Runner bem sucedida")
    except Exception as e:
        print(f"Erro durante o teste: {e}")


if __name__ == "__main__":
    test_runner()
