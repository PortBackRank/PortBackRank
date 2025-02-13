'''
    Class Backtesting
'''

from itertools import product
from typing import List, Dict
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from ranker import MARanker, RandomRanker
from runner import Runner
from utils import generate_filename, save_json, generate_performance_plot


def save_results(results):
    """
    Recebe os resultados das execuções paralelizadas e salva os arquivos.
    """
    for result in results:
        start_date, end_date = result['intervalo'].split(" - ")

        save_json(generate_filename('timeline', result, start_date,
                  end_date), result['shared_data']['timeline'])
        save_json(generate_filename('sell_buy_logs/sell_log',
                  result, start_date, end_date), result['sell_log'])
        save_json(generate_filename('sell_buy_logs/buy_log',
                  result, start_date, end_date), result['buy_log'])


class Backtesting:
    """ Classe para realizar backtesting de uma estratégia de investimento. """

    def __init__(self, ranker_cls, capital: float, interval: List[str], market_identifier: str = None):
        """
        Inicializa o backtesting com as informações básicas.

        :param ranker_cls: Classe do Ranker para criar instâncias.
        :param capital: Capital inicial para todas as simulações.
        :param interval: Lista com a data inicial e final da simulação.
        :param market_identifier: Sigla ou caminho dos ativos a serem usados.
        """
        self.ranker_cls = ranker_cls
        self.capital = capital
        self.interval = interval
        self.runner_cls = Runner
        self.data = MemData(interval, market_identifier)

    def run(
        self,
        parameter_grid: Dict[str, List[float]],
        ranker_grid: Dict[str, List[float]],
        n_jobs: int = -1
    ) -> pd.DataFrame:
        """
        Executa o backtesting variando os parâmetros do Runner e do ranker.

        :param parameter_grid: Dicionário com os parâmetros a variar e seus valores.
                               Exemplo: {'profit': [0.05, 0.1], 'loss': [0.05, 0.1]}.
        :param ranker_grid: Dicionário com os parâmetros do rankera variar.
                            Exemplo: {'SEED': [0, 1, 42]}.
        :param n_jobs: Número de processos paralelos (-1 usa todos os núcleos disponíveis).
        :return: DataFrame com os resultados das simulações.
        """
        runner_params = list(product(*parameter_grid.values()))
        ranker_params = list(product(*ranker_grid.values()))
        parameter_names = list(parameter_grid.keys())
        ranker_names = list(ranker_grid.keys())

        combinations = list(product(runner_params, ranker_params))

        def run_simulation(params):
            runner_values, ranker_values = params
            runner_config = dict(zip(parameter_names, runner_values))
            ranker_config = dict(zip(ranker_names, ranker_values))

            runner = self.runner_cls(
                profit=runner_config['profit'],
                loss=runner_config['loss'],
                diversification=runner_config['diversification'],
                ranker=self.ranker_cls,
                data=self.data
            )

            try:

                results_runner = []

                result = runner.single_run(
                    self.interval, ranker_config, self.capital)

                results_runner.append(result)

                return self._evaluate_results(results_runner, runner_config, ranker_config)
            except Exception as e:
                print(f"Erro ao rodar configuração {
                      runner_config} com ranker {ranker_config}: {e}")
                return None

        results = [
            res for res in Parallel(n_jobs=n_jobs)(
                delayed(run_simulation)(comb) for comb in combinations
            ) if res is not None
        ]

        # descomente para salvar os arquivos de timeline, caso use o MARanker
        save_results(results)

        for result in results:
            del result['shared_data']
            del result['sell_log']
            del result['buy_log']

        return pd.DataFrame(results)

    def _evaluate_results(
        self, result: List[Dict], runner_params: Dict, ranker_params: Dict
    ) -> Dict:
        """
        Calcula métricas de performance da simulação.

        :param result: Resultado da simulação (lista de dicionários).
        :param runner_params: Parâmetros usados na simulação para o Runner.
        :param ranker_params: Parâmetros usados na simulação para o Ranker.
        :return: Dicionário com as métricas calculadas.
        """
        caixa_final = result[-1]['balance'] if result else 0

        portfolio_value = sum(
            item['quantidade'] * item['preco_compra'] for item in result[-1]['portfolio']
        ) if result else 0

        retorno_total = (caixa_final + portfolio_value) / self.capital - 1
        retorno_total = round(retorno_total * 100, 2)

        shared_data = result[-1].get('shared_data', {}) if result else {}

        return {
            'intervalo': f"{self.interval[0]} - {self.interval[1]}",
            **runner_params,
            **ranker_params,
            'caixa_final': caixa_final,
            'portfolio_value': portfolio_value,
            'retorno_total': f"{retorno_total:.2f}%",
            'shared_data': shared_data,
            'sell_log': result[-1].get('sell_log', []),
            'buy_log': result[-1].get('buy_log', [])
        }


def test_bt_with_random():
    interval = ["2024-01-01", "2024-12-31"]

    backtester = Backtesting(RandomRanker, capital=10000, interval=interval)

    parameter_grid = {
        'profit': [0.06, 0.1],
        'loss': [0.04],
        'diversification': [0.2]
    }

    ranker_ranges = {"SEED": [0, 1, 42]}

    results = backtester.run(
        parameter_grid, ranker_grid=ranker_ranges, n_jobs=-1)

    print(results)


def test_bt_with_ma():
    interval = ["2024-01-01", "2024-12-31"]

    parameters = {"window": [[9, 21], [20, 50], [50, 200]]}

    backtester = Backtesting(MARanker, capital=10000,
                             interval=interval, market_identifier="IBOV")  # IBOV OU assets/hhh.csv

    parameter_grid = {
        'profit': [0.1, 0.15],
        'loss': [0.05],
        'diversification': [0.1, 0.2]
    }

    results = backtester.run(
        parameter_grid, ranker_grid=parameters, n_jobs=-1)

    generate_performance_plot()

    print(results)


if __name__ == "__main__":
    test_bt_with_ma()
