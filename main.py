from backtesting import Backtesting
from ranker import MARanker, RSIRanker
from data import Data
from markets import MarketData
from utils import generate_filename
import pandas as pd
import json
import os


def _calc_allocation(entry):
    return entry["balance"] + sum(
        item["quantidade"] * item["preco_compra"] for item in entry["portfolio"]
    )


def _print_df_full(df: pd.DataFrame):
    try:
        old_max_cols = pd.get_option('display.max_columns')
        old_width = pd.get_option('display.width')
        old_max_colwidth = None
        try:
            old_max_colwidth = pd.get_option('display.max_colwidth')
        except Exception:
            pass

        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        try:
            pd.set_option('display.max_colwidth', None)
        except Exception:
            pd.set_option('display.max_colwidth', 0)

        print(df.to_string(index=False))
    finally:
        try:
            pd.set_option('display.max_columns', old_max_cols)
            pd.set_option('display.width', old_width)
            if old_max_colwidth is not None:
                pd.set_option('display.max_colwidth', old_max_colwidth)
        except Exception:
            pass


def print_monthly_results(results_df):
    for i, row in results_df.iterrows():
        try:
            interval_str = row["intervalo"]
            start_date, end_date = [s.strip() for s in interval_str.split(" - ")]

            result_dict = row.to_dict()
            timeline_path = generate_filename("timeline", result_dict, start_date, end_date)

            if not os.path.exists(timeline_path):
                print(f"Timeline não encontrada para a linha {i}: {timeline_path}")
                continue

            with open(timeline_path, "r") as f:
                timeline = json.load(f)

            tl_df = pd.DataFrame(timeline)
            if tl_df.empty:
                continue
            tl_df["date"] = pd.to_datetime(tl_df["date"])  # yyyy-mm-dd
            tl_df["allocation"] = [
                _calc_allocation(entry) for entry in timeline
            ]

            monthly = tl_df.resample("ME", on="date").last()[["allocation"]]
            monthly["ret_mes_%"] = monthly["allocation"].pct_change() * 100

            label_params = []
            if "window" in row:
                try:
                    short_long = row["window"]
                    label_params.append(f"MA={short_long}")
                except Exception:
                    pass
            if "period" in row:
                label_params.append(f"RSI={row['period']}")

            print(f"\nConfig {i} | profit={row['profit']} loss={row['loss']} div={row['diversification']} {' '.join(label_params)}")
            for idx, rec in monthly.iterrows():
                ym = idx.strftime("%Y-%m")
                val = 0.0 if pd.isna(rec["ret_mes_%"]) else rec["ret_mes_%"]
                print(f"  {ym}: {val:.2f}%")
        except Exception as e:
            print(f"Erro ao gerar resumo mensal para linha {i}: {e}")


def _ensure_market_assets(market_code: str = "SP500"):
    market_data = MarketData(market_code)
    assets = market_data.list_recent_symbols(market_data.market, force_update=False)

    assets_to_download = []
    for asset in assets:
        try:
            df = Data.get_asset_data_by_name(asset)
            if df.empty:
                assets_to_download.append(asset)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            assets_to_download.append(asset)

    if assets_to_download:
        print(f"Baixando {len(assets_to_download)} ativos sem dados locais.")
        Data.download_history(assets_to_download)
    else:
        print("Todos os ativos já possuem dados locais.")


def run_backtest_ma():
    interval = ["2024-01-01", "2024-12-31"]
    _ensure_market_assets("SP500")

    backtester = Backtesting(
        MARanker,
        capital=10000,
        interval=interval,
        market_identifier="SP500",
    )

    ranker_grid = {"window": [[9, 21], [20, 50], [50, 200]]}
    runner_grid = {
        "profit": [0.1, 0.15],
        "loss": [0.05],
        "diversification": [0.1, 0.2],
    }

    results = backtester.run(runner_grid, ranker_grid=ranker_grid, n_jobs=-1)
    _print_df_full(results)
    # print_monthly_results(results)


def run_backtest_rsi():
    interval = ["2024-01-01", "2024-12-31"]
    _ensure_market_assets("SP500")

    backtester = Backtesting(
        RSIRanker,
        capital=10000,
        interval=interval,
        market_identifier="SP500",
    )

    ranker_grid = {
        "window": [[9, 9], [14, 14], [21, 21]],
        "oversold": [30],
        "overbought": [70],
        "mode": ["mean_reversion"],
    }
    runner_grid = {
        "profit": [0.1, 0.15],
        "loss": [0.05],
        "diversification": [0.1, 0.2],
    }

    results = backtester.run(runner_grid, ranker_grid=ranker_grid, n_jobs=-1)
    _print_df_full(results)
    # print_monthly_results(results)


def main_menu():
    print("\n=== PortBackRank - Escolha o indicador ===")
    print("1) Médias Móveis (MA)")
    print("2) RSI (Índice de Força Relativa)")
    print("0) Sair")

    choice = input("Selecione uma opção: ").strip()
    if choice == "1":
        run_backtest_ma()
    elif choice == "2":
        run_backtest_rsi()
    elif choice == "0":
        print("Saindo")
    else:
        print("Opção inválida.")


if __name__ == "__main__":
    main_menu()
