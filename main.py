from backtesting import Backtesting
from ranker import MARanker


def test_bt_with_ma():
    interval = ["2024-03-01", "2024-07-01"]

    parameters = {"window": [[9, 21], [20, 50], [50, 200]]}

    # QUANDO VAI PASSAR UM MERCADO TEM ALGUNS PRÉ DEFINIDOS OU PASSA O CAMINHO DO CSV
    # "SP500" ou "custom_teste.csv"
    backtester = Backtesting(MARanker, capital=10000,
                             interval=interval, market_identifier="custom_teste.csv")

    parameter_grid = {
        'profit': [0.1, 0.15],
        'loss': [0.05],
        'diversification': [0.1, 0.2]
    }

    results = backtester.run(
        parameter_grid, ranker_grid=parameters, n_jobs=-1)

    print(results)


if __name__ == "__main__":
    test_bt_with_ma()
