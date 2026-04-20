import json
import os

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    import yfinance as yf
except Exception:
    yf = None


def convert_numpy(obj):
    """Converte objetos numpy para tipos Python compatíveis com JSON."""
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def get_safe_int(value):
    """Garante que o valor seja um int, se aplicável."""
    return int(value) if isinstance(value, (int, np.integer)) else value


def _sanitize_token(value):
    text = str(value).strip().replace(" ", "")
    return "".join(ch for ch in text if ch.isalnum() or ch in ("-", ".", "_", "x"))


def _serialize_param_value(value):
    if isinstance(value, (list, tuple)):
        return "x".join(_sanitize_token(get_safe_int(item)) for item in value)
    if isinstance(value, np.ndarray):
        return "x".join(_sanitize_token(get_safe_int(item)) for item in value.tolist())
    return _sanitize_token(get_safe_int(value))


def _result_param_items(result):
    excluded_keys = {
        "intervalo",
        "caixa_final",
        "portfolio_value",
        "retorno_total",
        "shared_data",
        "sell_log",
        "buy_log",
    }
    runner_order = ["profit", "loss", "diversification"]

    items = []
    for key in runner_order:
        if key in result:
            label = "div" if key == "diversification" else key
            items.append((label, result[key]))

    for key in sorted(result.keys()):
        if key in excluded_keys or key in runner_order:
            continue
        items.append((key, result[key]))

    return items


def format_result_label(result):
    parts = []

    if "profit" in result:
        parts.append(f"Profit={result['profit']}")
    if "loss" in result:
        parts.append(f"Loss={result['loss']}")
    if "diversification" in result:
        parts.append(f"Div={result['diversification']}")

    is_rsi_style = (
        "window" in result
        and isinstance(result["window"], (list, tuple))
        and any(key in result for key in ("oversold", "overbought"))
    )

    if is_rsi_style:
        window = result["window"]
        if len(window) >= 1:
            parts.append(f"Period={window[0]}")
    elif "window" in result and isinstance(result["window"], (list, tuple)):
        window = result["window"]
        if len(window) >= 2:
            parts.append(f"Short={window[0]}")
            parts.append(f"Long={window[1]}")
        elif len(window) == 1:
            parts.append(f"Window={window[0]}")

    ordered_optional = [
        ("period", "Period"),
        ("std_dev", "StdDev"),
        ("oversold", "Oversold"),
        ("overbought", "Overbought"),
        ("fast", "Fast"),
        ("slow", "Slow"),
        ("signal", "Signal"),
        ("mode", "Mode"),
        ("SEED", "Seed"),
    ]
    for key, label in ordered_optional:
        if key in result:
            parts.append(f"{label}={_serialize_param_value(result[key])}")

    return ", ".join(parts)


def generate_filename(prefix, result, start_date, end_date):
    """Gera o nome do arquivo de forma centralizada."""
    base_parts = [
        f"{key}{_serialize_param_value(value)}"
        for key, value in _result_param_items(result)
    ]
    suffix = "_".join(base_parts)
    return f"results/{prefix}_{suffix}_{start_date}_to_{end_date}.json"


def save_json(filename, data):
    """Salva um dicionário como JSON."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, default=convert_numpy, ensure_ascii=False)


def _timeline_to_percentages(timeline, initial_value=10_000):
    if not timeline:
        return pd.Series(dtype="float64")

    df = pd.DataFrame(timeline)
    if df.empty:
        return pd.Series(dtype="float64")

    df["date"] = pd.to_datetime(df["date"])
    df["allocation"] = [
        entry["balance"] + sum(
            item["quantidade"] * item["preco_compra"]
            for item in entry["portfolio"]
        )
        for entry in timeline
    ]
    df["variation_pct"] = ((df["allocation"] - initial_value) / initial_value) * 100
    return df.set_index("date")["variation_pct"]


def _benchmark_symbol_from_market(market_symbol: str):
    mapping = {
        "SP500": "^GSPC",
        "IBOV": "^BVSP",
    }
    return mapping.get((market_symbol or "").upper())


def _load_benchmark_series(
    market_symbol: str,
    start_date: str,
    end_date: str,
    directory: str = "results",
):
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    symbol = (market_symbol or "").lower()

    candidate_paths = [
        os.path.join(directory, f"{symbol}_{start_date}_to_{end_date}.json"),
        os.path.join(directory, "benchmarks", f"{symbol}_{start_date}_to_{end_date}.json"),
        os.path.join("assets", "auxi", f"{symbol}.json"),
    ]

    for path in candidate_paths:
        if not os.path.exists(path):
            continue

        with open(path, "r", encoding="utf-8") as benchmark_file:
            data = json.load(benchmark_file)

        if not data:
            continue

        df = pd.DataFrame(data)
        if df.empty or "date" not in df.columns or "value" not in df.columns:
            continue

        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
        if df.empty:
            continue

        initial_value = df["value"].iloc[0]
        df["variation_pct"] = ((df["value"] - initial_value) / initial_value) * 100
        return df.set_index("date")["variation_pct"]

    benchmark_ticker = _benchmark_symbol_from_market(market_symbol)
    if benchmark_ticker and yf is not None:
        try:
            history_end = pd.to_datetime(end_date) + pd.Timedelta(days=1)
            history = yf.Ticker(benchmark_ticker).history(
                start=start_date,
                end=history_end.strftime("%Y-%m-%d"),
            )
            if not history.empty:
                history = history.reset_index()
                history["Date"] = pd.to_datetime(history["Date"]).dt.tz_localize(None)
                history = history[(history["Date"] >= start_dt) & (history["Date"] <= end_dt)]
                if not history.empty:
                    initial_value = history["Close"].iloc[0]
                    history["variation_pct"] = ((history["Close"] - initial_value) / initial_value) * 100

                    benchmark_data = [
                        {
                            "date": row["Date"].strftime("%Y-%m-%d"),
                            "value": round(float(row["Close"]), 4),
                        }
                        for _, row in history.iterrows()
                    ]
                    benchmark_path = os.path.join(
                        directory,
                        "benchmarks",
                        f"{symbol}_{start_date}_to_{end_date}.json",
                    )
                    save_json(benchmark_path, benchmark_data)
                    return history.set_index("Date")["variation_pct"]
        except Exception:
            pass

    return pd.Series(dtype="float64")


def generate_performance_plot(
    results_df: pd.DataFrame,
    indicator_name: str,
    start_date: str,
    end_date: str,
    directory: str = "results",
    market_symbol: str = "SP500",
):
    """
    Gera um gráfico comparando todas as configurações de um indicador com o benchmark do mercado.
    """
    plt.style.use("default")

    all_percentages = []
    color_palette = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    output_dir = os.path.join(directory, "plots")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir,
        f"{indicator_name.lower()}_{start_date}_to_{end_date}.png",
    )

    fig, ax = plt.subplots(figsize=(12.5, 5.3), dpi=100)

    color_index = 0
    for _, row in results_df.iterrows():
        result = row.to_dict()
        timeline_path = generate_filename("timeline", result, start_date, end_date)
        if not os.path.exists(timeline_path):
            print(f"Timeline não encontrada: {timeline_path}")
            continue

        with open(timeline_path, "r", encoding="utf-8") as timeline_file:
            timeline = json.load(timeline_file)

        timeline_series = _timeline_to_percentages(timeline)
        if timeline_series.empty:
            continue

        all_percentages.extend(timeline_series.tolist())
        ax.plot(
            timeline_series.index,
            timeline_series.values,
            label=format_result_label(result),
            color=color_palette[color_index % len(color_palette)],
            linewidth=1.1,
        )
        color_index += 1

    benchmark_series = _load_benchmark_series(
        market_symbol=market_symbol,
        start_date=start_date,
        end_date=end_date,
        directory=directory,
    )
    if not benchmark_series.empty:
        ax.plot(
            benchmark_series.index,
            benchmark_series.values,
            label=market_symbol.lower(),
            color="black",
            linestyle="dashed",
            linewidth=1.5,
        )
        all_percentages.extend(benchmark_series.tolist())

    if all_percentages:
        y_min = min(all_percentages)
        y_max = max(all_percentages)
        y_range = y_max - y_min
        margin = y_range * 0.10 if y_range else 1
        ax.set_ylim(y_min - margin, y_max + margin)

    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%Y"))

    for tick in ax.get_xticklabels():
        tick.set_rotation(45)
        tick.set_horizontalalignment("right")
        tick.set_fontsize(8)

    for tick in ax.get_yticklabels():
        tick.set_fontsize(8)

    ax.set_xlabel("Período", fontsize=9)
    ax.set_ylabel("Variação Percentual (%)", fontsize=10)
    ax.axhline(0, color="black", linestyle="--", linewidth=1)
    ax.grid(True, color="#b0b0b0", alpha=0.7, linewidth=0.8)
    ax.legend(
        loc="upper left",
        fontsize=6.5,
        frameon=True,
        fancybox=False,
        framealpha=1.0,
        borderpad=0.4,
        labelspacing=0.3,
        handlelength=1.8,
        handletextpad=0.5,
    )
    fig.tight_layout()

    fig.savefig(output_path, format="png")
    plt.close(fig)
    return output_path
