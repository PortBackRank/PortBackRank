'''
    Class Backtesting
'''

from typing import List, Dict
from itertools import product
from joblib import Parallel, delayed
import pandas as pd
from data import Data
from runner import Runner


class Backtesting:
    def __init__(self, runner_cls, data, capital: float):
        """
        Inicializa o backtesting com as informações básicas.

        :param runner_cls: Classe do Runner para criar instâncias.
        :param data: Instância da classe Data (para acessar dados financeiros).
        :param capital: Capital inicial para todas as simulações.
        """
        self.runner_cls = runner_cls
        self.data = data
        self.capital = capital

    def run(
        self,
        parameter_grid: Dict[str, List[float]],
        intervals: List[List[str]],
        ranker_ranges: Dict[str, List[float]],
        n_jobs: int = -1
    ) -> pd.DataFrame:
        """
        Executa o backtesting variando os parâmetros e intervalos.

        :param parameter_grid: Dicionário com os parâmetros a variar e seus valores.
                               Exemplo: {'profit': [0.05, 0.1], 'loss': [0.05, 0.1]}.
        :param intervals: Lista de intervalos de datas para os testes.
                          Exemplo: [["2023-01-01", "2023-06-01"], ["2023-06-01", "2023-12-31"]].
        :param ranker_ranges: Configurações do ranker a serem testadas.
        :param n_jobs: Número de processos paralelos (-1 usa todos os núcleos disponíveis).
        :return: DataFrame com os resultados das simulações.
        """
        parameter_names = list(parameter_grid.keys())
        parameter_values = list(parameter_grid.values())
        combinations = list(product(*parameter_values, intervals))

        def run_simulation(params_and_interval):
            params = dict(zip(parameter_names, params_and_interval[:-1]))
            interval = params_and_interval[-1]
            runner = self.runner_cls(
                profit=params['profit'],
                loss=params['loss'],
                diversification=params['diversification'],
                ranker_ranges=ranker_ranges,
            )
            try:
                result = runner._run(
                    interval, self.capital, ranker_ranges, log="")
                return self._evaluate_results(result, interval, params)
            except Exception as e:
                print(f"Erro ao rodar configuração {
                      params} no intervalo {interval}: {e}")
                return None

        results = Parallel(n_jobs=n_jobs)(
            delayed(run_simulation)(comb) for comb in combinations
        )

        results = [res for res in results if res is not None]
        return pd.DataFrame(results)

    def _evaluate_results(
        self, result: List[Dict], interval: List[str], params: Dict
    ) -> Dict:
        """
        Calcula métricas de performance da simulação.

        :param result: Resultado da simulação (lista de dicionários).
        :param interval: Intervalo de datas da simulação.
        :param params: Parâmetros usados na simulação.
        :return: Dicionário com as métricas calculadas.
        """
        caixa_final = result[-1]['caixa'] if result else 0
        portfolio_value = sum(
            item['quantidade'] * item['preco_medio'] for item in result[-1]['portfolio']
        ) if result else 0

        retorno_total = (caixa_final + portfolio_value) / self.capital - 1

        return {
            'intervalo': f"{interval[0]} - {interval[1]}",
            'profit': params['profit'],
            'loss': params['loss'],
            'diversification': params['diversification'],
            'caixa_final': caixa_final,
            'portfolio_value': portfolio_value,
            'retorno_total': retorno_total,
        }


if __name__ == "__main__":
    data = Data()

    backtester = Backtesting(Runner, data, capital=10000)

    parameter_grid = {
        'profit': [0.05, 0.1, 0.2],
        'loss': [0.05, 0.1],
        'diversification': [0.1, 0.2]
    }

    intervals = [["2024-05-01", "2024-06-01"], ["2024-06-02", "2024-07-02"]]

    ranker_ranges = {"SEED": [0, 1, 42]}

    results = backtester.run(parameter_grid, intervals,
                             ranker_ranges, n_jobs=-1)

    print(results)
