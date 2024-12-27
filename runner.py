'''
    class Runner
'''

from typing import List, Dict
import itertools

from datetime import datetime
import pandas as pd
from ranker import Ranker
from ranker import RandomRanker
from data import MemData


class Runner:
    def __init__(self, profit, loss, diversification, ranker_ranges, data: MemData):
        """
        Inicializa a classe Runner com os parâmetros fornecidos.

        :param profit: Lucro alvo para venda (porcentagem).
        :param loss: Limite de perda para venda (porcentagem).
        :param diversification: Porcentagem máxima para cada setor (porcentagem).
        :param ranker_ranges: Faixas de valores para cada parâmetro do ranker.
                              Exemplo: {'SEED': [0, 1, 42]}
        """
        self.profit = profit
        self.loss = loss
        self.diversification = diversification
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

    def _single_run(self, interval: List[str], ranker_conf: Dict[str, float], capital: float, log: str) -> Dict:
        """
        Executa uma simulação para uma única configuração de ranker, mantendo o portfólio com a quantidade e o preço de compra dos ativos.

        :param interval: Lista com a data inicial e final da simulação.
        :param ranker_conf: Configuração do ranker a ser utilizada.
        :param capital: Capital inicial.
        :param log: Arquivo de log para registrar todas as compras, vendas e saldo de capital.
        :return: Estado final do portfólio.
        """

        ranker = RandomRanker(parameters=ranker_conf)

        start_date, end_date = interval

        self.caixa = capital
        self.__portfolio = []

        for date in pd.date_range(start_date, end_date).strftime('%Y-%m-%d'):
            self._sell(date)
            self._buy(date, ranker)

        return {'caixa': self.caixa, 'portfolio': self.__portfolio}

    def _sell(self, date: str):
        """
        Vende ativos que atingiram o percentual de lucro ou perda.

        :param date: Data atual para verificar se algum ativo atendeu ao critério de venda.
        """
        dados_historicos = self.data.get_all_history()
        novos_portfolio = []

        for item in self.__portfolio:
            simbolo = item['simbolo']
            preco_medio = item['preco_medio']
            quantidade = item['quantidade']

            historico = dados_historicos.get(simbolo)
            if historico is None or historico.empty:
                novos_portfolio.append(item)
                continue

            preco_atual = historico.loc[
                historico['Date'].dt.date == datetime.strptime(
                    date, '%Y-%m-%d').date(), 'Close'
            ]

            if preco_atual.empty:
                novos_portfolio.append(item)
                continue

            preco_atual = preco_atual.iloc[0]
            percentual_variacao = (preco_atual - preco_medio) / preco_medio

            if percentual_variacao >= self.profit or percentual_variacao <= -self.loss:
                valor_venda = preco_atual * quantidade
                self.caixa += valor_venda
            else:
                novos_portfolio.append(item)

        self.__portfolio = novos_portfolio

    def _buy(self, date: str, ranker: Ranker):
        """
        Compra ativos com base no ranking e na diversificação do portfólio.

        :param date: Data atual para comprar ativos.
        :param ranker: Instância do ranker a ser utilizado para definir os ativos.
        """
        ranked_symbols = ranker.rank()
        if not ranked_symbols:
            return

        dados_historicos = self.data.get_all_history()

        total_portfolio_value = sum(
            item['preco_medio'] * item['quantidade'] for item in self.__portfolio
        )

        setor_percentual = {}

        if total_portfolio_value > 0:
            for item in self.__portfolio:
                setor = item.get('sector')
                preco_medio = item.get('preco_medio', 0)
                quantidade = item.get('quantidade', 0)

                if not setor:
                    print(f"Setor não encontrado para o símbolo {
                          item['simbolo']}.")
                    continue

                valor_item = preco_medio * quantidade

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

            preco_atual = historico.loc[
                historico['Date'].dt.date == datetime.strptime(
                    date, '%Y-%m-%d').date(), 'Close'
            ]

            if preco_atual.empty:
                continue

            preco_atual = preco_atual.iloc[0]

            quantidade_max = int(caixa_disponivel // preco_atual)

            # TODO: limitar a quantidade também pelo volume disponível do ativo
            quantidade_comprar = min(quantidade_max, int(
                max_investimento_setor // preco_atual))

            if quantidade_comprar <= 0:
                continue

            self.__portfolio.append({
                'simbolo': simbolo,
                'quantidade': quantidade_comprar,
                'preco_medio': preco_atual,
                'sector': setor
            })
            caixa_disponivel -= quantidade_comprar * preco_atual
            total_portfolio_value += quantidade_comprar * preco_atual
            setor_percentual[setor] = setor_percentual.get(setor, 0) + (
                quantidade_comprar * preco_atual / total_portfolio_value
            )

        self.caixa = caixa_disponivel

    def _run(self, interval: List[str], capital: float, ranker_ranges: Dict[str, List[float]], log: str) -> List[Dict]:
        """
        Executa a simulação para todas as configurações do ranker.

        :param interval: Intervalo de datas para a simulação.
        :param capital: Capital inicial.
        :param ranker_ranges: Faixas de parâmetros para gerar as configurações do ranker.
        :param log: Arquivo de log para registrar as simulações.
        :return: Lista com os resultados de todas as simulações.
        """
        ranker_confs = self._gen_ranker_confs(ranker_ranges)
        results = []

        start_time = datetime.now()
        for ranker_conf in ranker_confs:
            result = self._single_run(interval, ranker_conf, capital, log)
            results.append(result)
        end_time = datetime.now()
        print(f"Total time for running all configurations: {
              end_time - start_time}")

        return results


def test_runner():
    import tempfile
    import os

    fd, log_file_path = tempfile.mkstemp()

    try:
        os.close(fd)

        interval = ["2024-01-10", "2024-11-10"]
        capital = 10000
        ranker_ranges = {"SEED": [0, 1, 42]}
        start_date, end_date = interval

        runner = Runner(
            profit=0.1,
            loss=0.05,
            diversification=0.2,
            ranker_ranges=ranker_ranges,
            data=MemData(start_date, end_date)
        )

        try:
            results = runner._run(
                interval, capital, ranker_ranges, log_file_path)
            assert len(results) > 0, "Nenhum resultado gerado"
            assert all("caixa" in res and "portfolio" in res for res in results), \
                "Resultados não possuem os campos esperados"

            print("Execução do Runner bem sucedida")
        except Exception as e:
            print(f"Erro durante o teste: {e}")
        finally:
            with open(log_file_path, 'r') as log_file:
                print(log_file.read())

    finally:
        os.remove(log_file_path)


if __name__ == "__main__":
    test_runner()
