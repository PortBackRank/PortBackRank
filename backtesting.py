'''
    Class Backtesting
'''

import os
import json
from itertools import product
from typing import List, Dict
import matplotlib.pyplot as plt
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from ranker import MARanker, RandomRanker
from runner import Runner


def save_results(results):
    """
    Recebe os resultados das execuções paralelizadas e salva os arquivos.
    """

    for result in results:
        shared_data = result['shared_data']

        intervalo = result['intervalo']

        start_date = intervalo.split(" - ")[0]
        end_date = intervalo.split(" - ")[1]

        filename = f'results/timeline_profit{result["profit"]}_loss{result["loss"]}_div{
            result["diversification"]}_short{result['short']}_long{result['long']}_{start_date}_to_{end_date}.json'
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, 'w') as file:
            json.dump(shared_data['timeline'], file, indent=4)


class Backtesting:
    """ Classe para realizar backtesting de uma estratégia de investimento. """

    def __init__(self, ranker_cls, capital: float, interval: List[str]):
        """
        Inicializa o backtesting com as informações básicas.

        :param ranker_cls: Classe do Ranker para criar instâncias.
        :param capital: Capital inicial para todas as simulações.
        :param interval: Lista com a data inicial e final da simulação.
        """
        self.ranker_cls = ranker_cls
        self.capital = capital
        self.interval = interval
        self.runner_cls = Runner
        self.data = MemData(interval)

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

        # save_results(results) # descomente para salvar os arquivos de timeline, caso use o MARanker

        for result in results:
            del result['shared_data']

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
            'shared_data': shared_data
        }

    def generate_metrics_and_plots(
        self, results: pd.DataFrame, output_prefix: str = "backtesting_output"
    ):
        """
        Gera arquivos de métricas detalhadas e um gráfico organizado em grid para cada configuração.

        :param results: DataFrame com os resultados das simulações.
        :param output_prefix: Prefixo para os nomes dos arquivos gerados.
        """

        metrics_filename = f"results/{output_prefix}_metrics.txt"

        with open(metrics_filename, "w") as file:
            for _, row in results.iterrows():
                file.write("Configuração:\n")
                file.write(f"  Intervalo: {row['intervalo']}\n")
                file.write(f"  Parâmetros do Runner: {row['profit']}, {
                    row['loss']}, {row['diversification']}\n")
                file.write(f"  Parâmetros do Ranker: _short{
                    row['short']}_long{row['long']}\n")
                file.write(f"  Caixa Final: {row['caixa_final']:.2f}\n")
                file.write(f"  Valor do Portfólio: {
                           row['portfolio_value']:.2f}\n")
                file.write(f"  Retorno Total: {row['retorno_total']}\n")
                file.write("-" * 50 + "\n")

        num_configs = len(results)
        cols = 3
        rows = (num_configs + cols - 1) // cols

        fig, axes = plt.subplots(rows, cols, figsize=(
            cols * 5, rows * 4), squeeze=False)

        for idx, (_, row) in enumerate(results.iterrows()):
            timeline_filename = (
                f"results/timeline_profit{row['profit']
                                          }_loss{row['loss']}_div{row['diversification']}_"
                f"short{row['short']}_long{row['long']}_{
                    self.interval[0]}_to_{self.interval[1]}.json"
            )

            try:
                with open(timeline_filename, "r") as timeline_file:
                    timeline = json.load(timeline_file)

                    dates = []
                    allocation_over_time = []

                    for entry in timeline:
                        dates.append(entry['date'])
                        allocation = sum(
                            item['quantidade'] * item['preco_compra'] for item in entry['portfolio']
                        )
                        allocation_over_time.append(allocation)

                    combined_data = sorted(
                        zip(dates, allocation_over_time), key=lambda x: x[0])
                    dates, allocation_over_time = zip(*combined_data)

                    ax = axes[idx // cols][idx % cols]
                    ax.plot(dates, allocation_over_time,
                            label="Alocação em Ativos", color="green")
                    ax.set_title(f"Profit={row['profit']}, Loss={row['loss']}, Div={
                        row['diversification']}, Short={row['short']}, Long={row['long']}")
                    ax.set_xlabel("Data")
                    ax.set_ylabel("Valor (R$)")
                    ax.legend()
                    ax.grid(True)

            except FileNotFoundError:
                print(f"Arquivo de linha do tempo não encontrado: {
                    timeline_filename}")
                continue

        plt.tight_layout()
        plot_filename = f"results/{output_prefix}_performance_grid.png"
        plt.savefig(plot_filename)
        plt.close()


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

    parameters = {"short": [10, 20], "long": [50, 100]}

    backtester = Backtesting(MARanker, capital=10000, interval=interval)

    parameter_grid = {
        'profit': [0.06, 0.1],
        'loss': [0.04],
        'diversification': [0.2]
    }

    results = backtester.run(
        parameter_grid, ranker_grid=parameters, n_jobs=-1)

    # backtester.generate_metrics_and_plots(results) # descomente para plotar, caso use o MARanker

    print(results)


if __name__ == "__main__":
    test_bt_with_ma()
