'''
    Class Backtesting
'''

from typing import List, Dict
from itertools import product
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from runner import Runner


class Backtesting:
    """ Classe para realizar backtesting de uma estratégia de investimento. """

    def __init__(self, runner_cls, capital: float):
        """
        Inicializa o backtesting com as informações básicas.

        :param runner_cls: Classe do Runner para criar instâncias.
        :param data: Instância da classe Data (para acessar dados financeiros).
        :param capital: Capital inicial para todas as simulações.
        """
        self.runner_cls = runner_cls
        self.capital = capital

    def run(
        self,
        parameter_grid: Dict[str, List[float]],
        start_date: str,
        end_date: str,
        ranker_config: Dict[str, List[float]],
        n_jobs: int = -1
    ) -> pd.DataFrame:
        """
        Executa o backtesting variando os parâmetros e intervalo único.

        :param parameter_grid: Dicionário com os parâmetros a variar e seus valores.
                               Exemplo: {'profit': [0.05, 0.1], 'loss': [0.05, 0.1]}.
        :param start_date: Data inicial do período de backtesting.
        :param end_date: Data final do período de backtesting.
        :param ranker_config: Configurações do ranker a serem testadas.
        :param n_jobs: Número de processos paralelos (-1 usa todos os núcleos disponíveis).
        :return: DataFrame com os resultados das simulações.
        """
        parameter_names = list(parameter_grid.keys())
        parameter_values = list(parameter_grid.values())
        combinations = list(product(*parameter_values))

        data = MemData(start_date, end_date)

        def run_simulation(params):
            params_dict = dict(zip(parameter_names, params))

            runner = self.runner_cls(
                profit=params_dict['profit'],
                loss=params_dict['loss'],
                diversification=params_dict['diversification'],
                ranker_ranges=ranker_config,
                data=data
            )

            try:
                result = runner._run(
                    [start_date, end_date], self.capital, ranker_config, log="")
                return self._evaluate_results(result, start_date, end_date, params_dict)
            except Exception as e:
                print(f"Erro ao rodar configuração {params_dict}")
                return None

        results = Parallel(n_jobs=n_jobs)(
            delayed(run_simulation)(comb) for comb in combinations
        )

        results = [res for res in results if res is not None]
        return pd.DataFrame(results)

    def _evaluate_results(
        self, result: List[Dict], start_date: str, end_date: str, params: Dict
    ) -> Dict:
        """
        Calcula métricas de performance da simulação.

        :param result: Resultado da simulação (lista de dicionários).
        :param start_date: Data inicial do período de simulação.
        :param end_date: Data final do período de simulação.
        :param params: Parâmetros usados na simulação.
        :return: Dicionário com as métricas calculadas.
        """
        caixa_final = result[-1]['caixa'] if result else 0

        portfolio_value = sum(
            item['quantidade'] * item['preco_medio'] for item in result[-1]['portfolio']
        ) if result else 0

        retorno_total = (caixa_final + portfolio_value) / self.capital - 1

        retorno_total = round(retorno_total, 4) * 100

        return {
            'intervalo': f"{start_date} - {end_date}",
            'profit': params['profit'],
            'loss': params['loss'],
            'diversification': params['diversification'],
            'caixa_final': caixa_final,
            'portfolio_value': portfolio_value,
            'retorno_total': f"{retorno_total}%"
        }


if __name__ == "__main__":

    backtester = Backtesting(Runner, capital=10000)

    parameter_grid = {
        'profit': [0.05, 0.1, 0.2],
        'loss': [0.04, 0.1],
        'diversification': [0.1, 0.2]
    }

    start_date = "2024-01-01"
    end_date = "2024-10-01"

    ranker_ranges = {"SEED": [0, 1, 42]}

    results = backtester.run(parameter_grid, start_date,
                             end_date, ranker_ranges, n_jobs=-1)

    print(results)
