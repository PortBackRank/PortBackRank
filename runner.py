'''
    class Runner
'''

from typing import List, Dict
import itertools

from datetime import datetime
import pandas as pd
from ranker import Ranker
from ranker import RandomRanker
from data import Data


class Runner:
    def __init__(self, profit, loss, diversification, ranker_ranges):
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

        self.data = Data()

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

        data_inicio, data_fim = interval

        ativos = self.data.list_symbols()
        dados_historicos = self.data.get_history_interval(
            assets=ativos,
            start_date=data_inicio,
            end_date=data_fim,
        )
        self.caixa = capital
        self.__portfolio = []

        for date in pd.date_range(data_inicio, data_fim).strftime('%Y-%m-%d'):
            self._buy(date, ranker, dados_historicos)
            self._sell(date, dados_historicos)

        return {'caixa': self.caixa, 'portfolio': self.__portfolio}

    def _sell(self, date: str, dados_historicos: pd.DataFrame):
        """
        Vende ativos que atingiram o percentual de lucro ou perda.

        :param date: Data atual para verificar se algum ativo atendeu ao critério de venda.
        :param dados_historicos: DataFrame contendo os dados históricos de preços.
        """
        # print(f"Portfólio antes da VENDA: {self.__portfolio}")
        novos_portfolio = []
        total_portfolio_value = sum(
            item['preco_medio'] * item['quantidade'] for item in self.__portfolio)

        for item in self.__portfolio:
            simbolo = item['simbolo']
            preco_medio = item['preco_medio']
            quantidade = item['quantidade']

            historico = None
            for h in dados_historicos:
                if h.get('symbol') == simbolo:
                    historico = h.get('data')
                    break

            if historico is None or not isinstance(historico, pd.DataFrame):
                print(f"Histórico de {simbolo} não encontrado ou inválido.")
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
                print(f"Vendendo {quantidade} de {simbolo} a {
                      preco_atual} para {valor_venda}.")
            else:
                novos_portfolio.append(item)

        self.__portfolio = novos_portfolio

        total_portfolio_value = sum(
            item['preco_medio'] * item['quantidade'] for item in self.__portfolio)

        # print(f"Portfólio após venda: {self.__portfolio}")
        print(f"Caixa restante após venda: {self.caixa}")
        print(f"Total do portfólio após venda: {total_portfolio_value}")

    def _buy(self, date: str, ranker: Ranker, dados_historicos: pd.DataFrame):
        """
        Compra ativos com base no ranking e na diversificação do portfólio.

        :param date: Data atual para comprar ativos.
        :param ranker: Instância do ranker a ser utilizado para definir os ativos.
        :param dados_historicos: DataFrame contendo os dados históricos de preços.
        """
        ranked_symbols = ranker.rank()
        if not ranked_symbols:
            return

        symbols_portfolio = [item['simbolo'] for item in self.__portfolio]
        print("symbols_portfolio", symbols_portfolio)
        portfolio_info = self.data.get_asset_info(symbols_portfolio)
        print("portfolio_info", portfolio_info)

        total_portfolio_value = sum(
            item['preco_medio'] * item['quantidade'] for item in self.__portfolio)

        print("total_portfolio_value", total_portfolio_value)

        if total_portfolio_value == 0:
            print("Primeira compra.")
            setor_percentual = {}
            caixa_disponivel = self.caixa
        else:
            setor_percentual = {}

            for item in self.__portfolio:
                simbolo = item['simbolo']

                setor = None
                for info in portfolio_info:
                    if info.get('simbolo') == simbolo:
                        setor = info.get('sector')
                        break

                if setor:
                    setor_percentual[setor] = setor_percentual.get(setor, 0) + (
                        item['preco_medio'] * item['quantidade'] /
                        total_portfolio_value
                    )
            caixa_disponivel = self.caixa

        for simbolo in ranked_symbols[:40]:
            if caixa_disponivel <= 0:
                break

            print("setor_percentual", setor_percentual)
            print("caixa", caixa_disponivel)

            ativo_info = self.data.get_asset_info([simbolo])
            if not ativo_info:
                continue

            setor = ativo_info[0]['sector']
            if isinstance(setor, pd.Series):
                setor = setor.iloc[0]
            print("setor:", setor)

            # Se for a primeira compra ou o setor ainda não foi adicionado, não limitamos a compra
            if total_portfolio_value == 0 or setor not in setor_percentual:
                # Aplica a diversificação de 20% do caixa disponível para a compra do ativo
                max_investimento_setor = caixa_disponivel * self.diversification
            else:
                setor_atual = setor_percentual.get(
                    setor, 0) * total_portfolio_value
                max_investimento_setor = total_portfolio_value * \
                    self.diversification - setor_atual

            print(f"max_investimento_setor para {
                  setor}: {max_investimento_setor}")

            historico = None
            for item in dados_historicos:
                if item.get('symbol') == simbolo:
                    historico = item.get('data')
                    break

            if historico is None or not isinstance(historico, pd.DataFrame):
                print(f"Histórico de {simbolo} não encontrado ou inválido.")
                continue

            if historico is None:
                continue

            preco_atual = historico.loc[
                historico['Date'].dt.date == datetime.strptime(
                    date, '%Y-%m-%d').date(), 'Close'
            ]

            if preco_atual.empty:
                continue

            preco_atual = preco_atual.iloc[0]
            print(f"Preço atual para {simbolo} em {date}: {preco_atual}")

            quantidade_max = int(caixa_disponivel // preco_atual)
            if quantidade_max <= 0:
                continue

            quantidade_comprar = min(quantidade_max, int(
                max_investimento_setor // preco_atual))

            if quantidade_comprar <= 0:
                continue

            print(f"Comprando {quantidade_comprar} de {
                  simbolo} a {preco_atual}.")

            self.__portfolio.append({
                'simbolo': simbolo,
                'quantidade': quantidade_comprar,
                'preco_medio': preco_atual,
                'sector': setor
            })
            caixa_disponivel -= quantidade_comprar * preco_atual

            total_portfolio_value += quantidade_comprar * preco_atual
            setor_percentual[setor] = setor_percentual.get(setor, 0) + (
                quantidade_comprar * preco_atual / total_portfolio_value)

            # print(f"Portfólio atualizado: {self.__portfolio}")
            self.caixa = caixa_disponivel
            print(f"Caixa restante: {self.caixa}")

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

        for ranker_conf in ranker_confs:
            result = self._single_run(interval, ranker_conf, capital, log)
            results.append(result)

        return results


def test_runner():
    import tempfile
    import os

    fd, log_file_path = tempfile.mkstemp()

    try:
        os.close(fd)

        interval = ["2024-09-20", "2024-10-10"]
        capital = 10000
        ranker_ranges = {"SEED": [0, 1, 42]}

        runner = Runner(
            profit=0.1,
            loss=0.05,
            diversification=0.2,
            ranker_ranges=ranker_ranges,
        )

        try:
            results = runner._run(
                interval, capital, ranker_ranges, log_file_path)
            assert len(results) > 0, "Nenhum resultado gerado"
            assert all("caixa" in res and "portfolio" in res for res in results), \
                "Resultados não possuem os campos esperados"

            print("Teste bem-sucedido.")
        except Exception as e:
            print(f"Erro durante o teste: {e}")
        finally:
            with open(log_file_path, 'r') as log_file:
                print(log_file.read())

    finally:
        os.remove(log_file_path)


if __name__ == "__main__":
    test_runner()
