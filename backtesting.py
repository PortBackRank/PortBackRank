'''
    Class Backtesting
'''

from typing import List, Dict
from itertools import product
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from ranker import RandomRanker
from runner import Runner


class Backtesting:
    """ Classe para realizar backtesting de uma estratégia de investimento. """

    def __init__(self, runner_cls, ranker_cls, capital: float):
        """
        Inicializa o backtesting com as informações básicas.

        :param runner_cls: Classe do Runner para criar instâncias.
        :param ranker_cls: Classe do Ranker para criar instâncias.
        :param capital: Capital inicial para todas as simulações.
        """
        self.runner_cls = runner_cls
        self.ranker_cls = ranker_cls
        self.capital = capital

    def run(
        self,
        parameter_grid: Dict[str, List[float]],
        interval: List[str],
        ranker_grid: Dict[str, List[float]],
        n_jobs: int = -1
    ) -> pd.DataFrame:
        """
        Executa o backtesting variando os parâmetros do Runner e do ranker.

        :param parameter_grid: Dicionário com os parâmetros a variar e seus valores.
                               Exemplo: {'profit': [0.05, 0.1], 'loss': [0.05, 0.1]}.
        :param ranker_grid: Dicionário com os parâmetros do ranker a variar.
                            Exemplo: {'SEED': [0, 1, 42]}.
        :param interval: Lista com duas strings representando a data de início e fim do backtesting.
        :param n_jobs: Número de processos paralelos (-1 usa todos os núcleos disponíveis).
        :return: DataFrame com os resultados das simulações.
        """
        runner_params = list(product(*parameter_grid.values()))
        ranker_params = list(product(*ranker_grid.values()))
        parameter_names = list(parameter_grid.keys())
        ranker_names = list(ranker_grid.keys())

        combinations = list(product(runner_params, ranker_params))

        data = MemData(interval=interval)

        def run_simulation(params):
            runner_values, ranker_values = params
            runner_config = dict(zip(parameter_names, runner_values))
            ranker_config = dict(zip(ranker_names, ranker_values))

            runner = self.runner_cls(
                profit=runner_config['profit'],
                loss=runner_config['loss'],
                diversification=runner_config['diversification'],
                ranker=self.ranker_cls,
                ranker_ranges=ranker_config,
                data=data
            )

            try:
                result = runner._run(interval=interval, capital=self.capital)

                return self._evaluate_results(result, interval, runner_config, ranker_config)
            except Exception as e:
                print(f"Erro ao rodar configuração {
                      runner_config} com ranker {ranker_config}: {e}")
                return None

        results = Parallel(n_jobs=n_jobs)(
            delayed(run_simulation)(comb) for comb in combinations
        )

        results = [res for res in results if res is not None]
        return pd.DataFrame(results)

    def _evaluate_results(
        self, result: List[Dict], interval: List[str], runner_params: Dict, ranker_params: Dict
    ) -> Dict:
        """
        Calcula métricas de performance da simulação.

        :param result: Resultado da simulação (lista de dicionários).
        :param interval: Lista com duas strings representando a data de início e fim do backtesting.
        :param runner_params: Parâmetros usados na simulação para o Runner.
        :param ranker_params: Parâmetros usados na simulação para o Ranker.
        :return: Dicionário com as métricas calculadas.
        """
        caixa_final = result[-1]['caixa'] if result else 0

        portfolio_value = sum(
            item['quantidade'] * item['preco_compra'] for item in result[-1]['portfolio']
        ) if result else 0

        retorno_total = (caixa_final + portfolio_value) / self.capital - 1
        retorno_total = round(retorno_total, 4) * 100

        return {
            'intervalo': f"{interval[0]} - {interval[1]}",
            **runner_params,
            **ranker_params,
            'caixa_final': caixa_final,
            'portfolio_value': portfolio_value,
            'retorno_total': f"{retorno_total}%"
        }


if __name__ == "__main__":

    backtester = Backtesting(Runner, RandomRanker, capital=10000)

    parameter_grid = {
        'profit': [0.06, 0.1],
        'loss': [0.04, 0.05],
        'diversification': [0.2]
    }

    interval = ["2024-01-10", "2024-11-10"]

    ranker_ranges = {"SEED": [0, 1, 42]}

    results = backtester.run(parameter_grid, interval,
                             ranker_grid=ranker_ranges, n_jobs=-1)

    print(results)
