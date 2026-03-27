import argparse
import json
import os

import pandas as pd

from backtesting import Backtesting
from data import Data
from markets import MarketData, list_recent_symbols
from ranker import BollingerRanker, EMARanker, MACDRanker, MARanker, RSIRanker
from utils import format_result_label, generate_filename, generate_performance_plot


DEFAULT_INTERVAL = ["2025-01-01", "2025-12-31"]


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


def print_monthly_results(results_df: pd.DataFrame):
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

            with open(timeline_path, "r", encoding="utf-8") as file:
                timeline = json.load(file)

            tl_df = pd.DataFrame(timeline)
            if tl_df.empty:
                continue

            tl_df["date"] = pd.to_datetime(tl_df["date"])
            tl_df["allocation"] = [_calc_allocation(entry) for entry in timeline]

            monthly = tl_df.resample("ME", on="date").last()[["allocation"]]
            monthly["ret_mes_%"] = monthly["allocation"].pct_change() * 100

            print(f"\nConfig {i} | {format_result_label(result_dict)}")
            for idx, rec in monthly.iterrows():
                ym = idx.strftime("%Y-%m")
                val = 0.0 if pd.isna(rec["ret_mes_%"]) else rec["ret_mes_%"]
                print(f"  {ym}: {val:.2f}%")
        except Exception as exc:
            print(f"Erro ao gerar resumo mensal para linha {i}: {exc}")


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


def _load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def _resolve_interval(config: dict):
    interval = config.get("interval")
    if interval and len(interval) == 2:
        return interval
    return list(DEFAULT_INTERVAL)


def _build_grids(config: dict):
    params = config.get("ranker-params") or {}

    runner_grid = {
        "profit": params.get("profit", []),
        "loss": params.get("loss", []),
        "diversification": params.get("diversification", []),
    }

    ranker_grid = {}
    for key, value in params.items():
        if key not in {"profit", "loss", "diversification"}:
            ranker_grid[key] = value

    return runner_grid, ranker_grid


def _get_ranker_class(name: str):
    if name == "MARanker":
        return MARanker
    if name == "RSIRanker":
        return RSIRanker
    if name == "EMARanker":
        return EMARanker
    if name == "BollingerRanker":
        return BollingerRanker
    if name == "MACDRanker":
        return MACDRanker
    raise ValueError(f"Ranker '{name}' não suportado.")


def _run_download_step(market_identifier: str, download_mode: str):
    mode = (download_mode or "missing").lower()
    if mode == "all":
        assets = list_recent_symbols(market_identifier, force_update=True)
        Data.download_history(assets)
    elif mode == "missing":
        _ensure_market_assets(market_identifier)


def run_from_config(
    config_path: str,
    download_mode: str = "missing",
    print_monthly: bool = False,
    generate_plot: bool = True,
):
    config = _load_config(config_path)

    market_identifier = config.get("id", "SP500")
    interval = _resolve_interval(config)
    capital = config.get("capital", 10000)

    ranker_name = config.get("ranker", "MARanker")
    ranker_cls = _get_ranker_class(ranker_name)
    runner_grid, ranker_grid = _build_grids(config)

    _run_download_step(market_identifier, download_mode)

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

    if print_monthly:
        print_monthly_results(results)

    plot_path = None
    if generate_plot:
        plot_path = generate_performance_plot(
            results_df=results,
            indicator_name=ranker_name.replace("Ranker", ""),
            start_date=interval[0],
            end_date=interval[1],
            market_symbol=market_identifier,
        )
        print(f"\nGráfico salvo em: {plot_path}")

    return {
        "config_path": config_path,
        "ranker_name": ranker_name,
        "interval": interval,
        "results": results,
        "plot_path": plot_path,
    }


def run_all_configs(
    download_mode: str = "missing",
    print_monthly: bool = False,
    generate_plot: bool = True,
):
    config_paths = sorted(
        file_name
        for file_name in os.listdir(".")
        if file_name.startswith("config_") and file_name.endswith(".json")
    )

    if not config_paths:
        raise FileNotFoundError("Nenhum arquivo config_*.json foi encontrado.")

    executions = []
    downloaded_markets = set()

    for config_path in config_paths:
        config = _load_config(config_path)
        market_identifier = config.get("id", "SP500")
        effective_download_mode = download_mode

        if market_identifier in downloaded_markets and download_mode != "none":
            effective_download_mode = "none"
        elif download_mode != "none":
            downloaded_markets.add(market_identifier)

        print(f"\n{'=' * 80}")
        print(f"Executando {config_path}")
        print(f"{'=' * 80}")

        execution = run_from_config(
            config_path=config_path,
            download_mode=effective_download_mode,
            print_monthly=print_monthly,
            generate_plot=generate_plot,
        )
        executions.append(execution)

    return executions


def _parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    parser.add_argument(
        "--all-configs",
        action="store_true",
        help="Executa todos os arquivos config_*.json do diretório atual.",
    )
    parser.add_argument(
        "--download-data",
        choices=["all", "missing", "none"],
        default="missing",
    )
    parser.add_argument(
        "--print-monthly",
        action="store_true",
        help="Imprime também o retorno mensal de cada configuração.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)

    if args.all_configs:
        run_all_configs(
            download_mode=args.download_data,
            print_monthly=args.print_monthly,
        )
        return

    if not args.config:
        raise ValueError("Informe --config ou use --all-configs.")

    run_from_config(
        args.config,
        download_mode=args.download_data,
        print_monthly=args.print_monthly,
    )


if __name__ == "__main__":
    main()
