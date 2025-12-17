from backtesting import Backtesting
from ranker import MARanker, RSIRanker
from data import Data
from markets import MarketData, list_recent_symbols
from utils import generate_filename
import argparse
import pandas as pd
import json
import os


def _calc_allocation(entry):
    return entry["balance"] + sum(
        item["quantidade"] * item["preco_compra"] for item in entry["portfolio"]
    )


def _print_df_full(df: pd.DataFrame):
    try:
        old_max_cols = pd.get_option("display.max_columns")
        old_width = pd.get_option("display.width")
        old_max_colwidth = None
        try:
            old_max_colwidth = pd.get_option("display.max_colwidth")
        except Exception:
            pass

        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)
        try:
            pd.set_option("display.max_colwidth", None)
        except Exception:
            pd.set_option("display.max_colwidth", 0)

        print(df.to_string(index=False))
    finally:
        try:
            pd.set_option("display.max_columns", old_max_cols)
            pd.set_option("display.width", old_width)
            if old_max_colwidth is not None:
                pd.set_option("display.max_colwidth", old_max_colwidth)
        except Exception:
            pass


def print_monthly_results(results_df):
    for i, row in results_df.iterrows():
        try:
            interval_str = row["intervalo"]
            start_date, end_date = [s.strip() for s in interval_str.split(" - ")]

            result_dict = row.to_dict()
            timeline_path = generate_filename(
                "timeline", result_dict, start_date, end_date
            )

            if not os.path.exists(timeline_path):
                print(f"Timeline não encontrada para a linha {i}: {timeline_path}")
                continue

            with open(timeline_path, "r", encoding="utf-8") as f:
                timeline = json.load(f)

            tl_df = pd.DataFrame(timeline)
            if tl_df.empty:
                continue
            tl_df["date"] = pd.to_datetime(tl_df["date"])
            tl_df["allocation"] = [_calc_allocation(entry) for entry in timeline]

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

            print(
                f"\nConfig {i} | profit={row['profit']} "
                f"loss={row['loss']} div={row['diversification']} "
                f"{' '.join(label_params)}"
            )
            for idx, rec in monthly.iterrows():
                ym = idx.strftime("%Y-%m")
                val = 0.0 if pd.isna(rec["ret_mes_%"]) else rec["ret_mes_%"]
                print(f"  {ym}: {val:.2f}%")
        except Exception as e:
            print(f"Erro ao gerar resumo mensal para linha {i}: {e}")


def _ensure_market_assets(market_code: str = "SP500"):
    """
    Garante que todos os ativos do mercado tenham histórico baixado.
    """
    market_data = MarketData(market_code)
    assets = market_data.list_recent_symbols(market_data.market, force_update=False)

    max_retries = 3
    missing = []

    for attempt in range(1, max_retries + 1):
        missing = []
        for asset in assets:
            try:
                df = Data.load_dataframe(f"{asset}.csv")
            except (FileNotFoundError, pd.errors.EmptyDataError):
                df = None

            if not isinstance(df, pd.DataFrame) or df.empty:
                missing.append(asset)

        if not missing:
            print("Todos os ativos possuem dados locais.")
            break

        print(
            f"Tentativa {attempt}/{max_retries}: "
            f"baixando {len(missing)} ativos sem dados locais."
        )
        print("Ativos faltando:", ", ".join(missing))
        Data.download_history(missing)

    if missing:
        print("Após as tentativas de download, ainda faltam dados históricos para:")
        for asset in missing:
            print(f"  - {asset}")


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

'''
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
'''

'''

Explicação dos comandos que devem ser feitos no terminal para rodar o código:

python main.py --config config.json --download-data all -> reconstrói o universo do índice + baixa tudo.
python main.py --config config.json --download-data missing -> confia no universo que já está em 
cache e só baixa histórico do que não tiver CSV
python main.py --config config.json --download-data none -> não faz nenhum download em lote antes do backtest

Se precisar alterar algum parâmetro mudar no arquivo config.json

'''
def _load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_grids(config: dict):
    params = config.get("ranker-params") or {}

    runner_grid = {
        "profit": params.get("profit", []),
        "loss": params.get("loss", []),
        "diversification": params.get("diversification", []),
    }

    ranker_grid = {}
    for key, value in params.items():
        if key not in runner_grid:
            ranker_grid[key] = value

    return runner_grid, ranker_grid


def _get_ranker_class(name: str):
    if name == "MARanker":
        return MARanker
    if name == "RSIRanker":
        return RSIRanker
    raise ValueError(f"Ranker '{name}' não suportado.")


def run_from_config(config_path: str, download_mode: str = "missing"):
    config = _load_config(config_path)

    market_identifier = config.get("id", "SP500")
    interval = config.get("interval", ["2024-01-01", "2024-12-31"])
    capital = config.get("capital", 10000)

    ranker_name = config.get("ranker", "MARanker")
    ranker_cls = _get_ranker_class(ranker_name)

    runner_grid, ranker_grid = _build_grids(config)

    mode = (download_mode or "missing").lower()
    if mode == "all":
        assets = list_recent_symbols(market_identifier, force_update=True)
        Data.download_history(assets)
    elif mode == "missing":
        _ensure_market_assets(market_identifier)
    elif mode == "none":
        pass

    backtester = Backtesting(
        ranker_cls,
        capital=capital,
        interval=interval,
        market_identifier=market_identifier,
    )

    results = backtester.run(
        runner_grid,
        ranker_grid=ranker_grid,
        n_jobs=-1,
    )
    _print_df_full(results)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--config", required=True)
    parser.add_argument(
        "-d",
        "--download-data",
        choices=["all", "missing", "none"],
        default="missing",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    run_from_config(args.config, download_mode=args.download_data)


if __name__ == "__main__":
    main()
