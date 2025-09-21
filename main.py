from backtesting import Backtesting
from ranker import MARanker
from data import Data        
from markets import MarketData
import pandas as pd

def test_bt_with_ma():
    interval = ["2024-01-01", "2024-12-31"]
    parameters = {"window": [[9, 21], [20, 50], [50, 200]]}

    # Inicializa MarketData e pega os ativos do mercado escolhido
    market_data = MarketData("SP500")
    assets = market_data.list_recent_symbols(market_data.market, force_update=False)

    # Só baixa históricos de ativos que ainda não possuem CSV
    assets_to_download = []
    for asset in assets:
        try:
            df = Data.get_asset_data_by_name(asset)
            if df.empty:
                assets_to_download.append(asset)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            assets_to_download.append(asset)


    if assets_to_download:
        print(f"Baixando {len(assets_to_download)} ativos que não possuem dados locais")
        Data.download_history(assets_to_download)
    else:
        print("Todos os ativos já possuem dados locais. Nenhum download necessário.")

    # Inicializa o backtester
    backtester = Backtesting(
        MARanker,
        capital=10000,
        interval=interval,
        market_identifier="SP500"
    )

    parameter_grid = {
        'profit': [0.1, 0.15],
        'loss': [0.05],
        'diversification': [0.1, 0.2]
    }

    # Executa o backtesting
    results = backtester.run(parameter_grid, ranker_grid=parameters, n_jobs=-1)

    print(results)


if __name__ == "__main__":
    test_bt_with_ma()
