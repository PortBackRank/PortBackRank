"""
Gera figuras em linha de fechamento, sem candles, para explicar os
indicadores implementados no projeto usando um unico ativo brasileiro
entre 2025 e 2026.

O script replica a logica central dos indicadores de `ranker.py`:
- Media Movel Simples (MA)
- Media Movel Exponencial (EMA)
- RSI pelo metodo de Wilder
- Bandas de Bollinger
- MACD

Exemplos:
    python explain_indicators.py
    python explain_indicators.py --ticker VALE3.SA
    python explain_indicators.py --indicator rsi
    python explain_indicators.py --ticker ITUB4.SA --start 2025-01-01 --end 2026-12-31
    python explain_indicators.py --use-monograph-set
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import pandas as pd
import yfinance as yf


POPULAR_B3_TICKERS = {
    "PETR4": "PETR4.SA",
    "VALE3": "VALE3.SA",
    "ITUB4": "ITUB4.SA",
    "B3SA3": "B3SA3.SA",
    "WEGE3": "WEGE3.SA",
    "ABEV3": "ABEV3.SA",
    "BBAS3": "BBAS3.SA",
    "MGLU3": "MGLU3.SA",
}

OUTPUT_DIR = Path("results") / "indicator_explanations"
YF_CACHE_DIR = Path(".yfinance_cache")
MONOGRAPH_TICKERS = {
    "ma": "WEGE3.SA",
    "ema": "BBAS3.SA",
    "rsi": "PETR4.SA",
    "bollinger": "MGLU3.SA",
    "macd": "VALE3.SA",
}


@dataclass(frozen=True)
class PlotConfig:
    name: str
    title: str
    filename: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera graficos para explicar indicadores em um unico ativo brasileiro."
    )
    parser.add_argument("--ticker", default="PETR4.SA", help="Ticker do Yahoo Finance. Ex.: PETR4.SA")
    parser.add_argument("--start", default="2025-01-01", help="Data inicial no formato YYYY-MM-DD")
    parser.add_argument("--end", default="2026-12-31", help="Data final no formato YYYY-MM-DD")
    parser.add_argument(
        "--indicator",
        default="all",
        choices=["all", "ma", "ema", "rsi", "bollinger", "macd"],
        help="Indicador a gerar. 'all' cria todas as figuras.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Diretorio para salvar as figuras.",
    )
    parser.add_argument(
        "--use-monograph-set",
        action="store_true",
        help="Usa um ativo brasileiro diferente para cada indicador, conforme selecao sugerida para a monografia.",
    )
    return parser.parse_args()


def normalize_ticker(ticker: str) -> str:
    ticker = ticker.strip().upper()
    return POPULAR_B3_TICKERS.get(ticker, ticker)


def download_history(ticker: str, start: str, end: str) -> pd.DataFrame:
    YF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        yf.set_tz_cache_location(str(YF_CACHE_DIR.resolve()))
    except Exception:
        pass

    df = yf.download(
        ticker,
        start=start,
        end=end,
        auto_adjust=False,
        progress=False,
        actions=False,
    )
    if df.empty:
        raise ValueError(
            f"Nao foi possivel baixar dados para {ticker}. "
            "Verifique o ticker ou a conexao com a internet."
        )
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.rename_axis("Date").reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
    df = df[["Date", "Close", "Volume"]].copy()
    df = df.dropna(subset=["Close"]).sort_values("Date").reset_index(drop=True)
    return df


def compute_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    rsi = pd.Series(index=close.index, dtype="float64")
    if len(close) < period + 1:
        return rsi

    avg_gain = gain.iloc[1 : period + 1].mean()
    avg_loss = loss.iloc[1 : period + 1].mean()

    def calc_rsi(avg_g: float, avg_l: float) -> float:
        if avg_l == 0 and avg_g == 0:
            return 50.0
        if avg_l == 0:
            return 100.0
        if avg_g == 0:
            return 0.0
        rs = avg_g / avg_l
        return 100 - (100 / (1 + rs))

    rsi.iloc[period] = calc_rsi(avg_gain, avg_loss)
    for i in range(period + 1, len(close)):
        avg_gain = (avg_gain * (period - 1) + gain.iloc[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss.iloc[i]) / period
        rsi.iloc[i] = calc_rsi(avg_gain, avg_loss)

    return rsi


def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for short, long in ((9, 21), (20, 50), (50, 200)):
        out[f"ma_{short}"] = out["Close"].rolling(short).mean()
        out[f"ma_{long}"] = out["Close"].rolling(long).mean()
        out[f"ema_{short}"] = out["Close"].ewm(span=short, adjust=False).mean()
        out[f"ema_{long}"] = out["Close"].ewm(span=long, adjust=False).mean()

    out["rsi_14"] = compute_rsi(out["Close"], 14)

    period = 20
    std_dev = 2.0
    out["bb_mid_20"] = out["Close"].rolling(period).mean()
    out["bb_std_20"] = out["Close"].rolling(period).std()
    out["bb_upper_20_2"] = out["bb_mid_20"] + (std_dev * out["bb_std_20"])
    out["bb_lower_20_2"] = out["bb_mid_20"] - (std_dev * out["bb_std_20"])

    ema_fast = out["Close"].ewm(span=12, adjust=False).mean()
    ema_slow = out["Close"].ewm(span=26, adjust=False).mean()
    out["macd_12_26"] = ema_fast - ema_slow
    out["macd_signal_9"] = out["macd_12_26"].ewm(span=9, adjust=False).mean()
    out["macd_hist_12_26_9"] = out["macd_12_26"] - out["macd_signal_9"]

    out["close_var_pct"] = (out["Close"] / out["Close"].iloc[0] - 1.0) * 100.0
    return out


def crossover_points(df: pd.DataFrame, fast_col: str, slow_col: str) -> pd.DataFrame:
    prev_fast = df[fast_col].shift(1)
    prev_slow = df[slow_col].shift(1)
    crossed_up = (prev_fast <= prev_slow) & (df[fast_col] > df[slow_col])
    crossed_down = (prev_fast >= prev_slow) & (df[fast_col] < df[slow_col])
    signals = df.loc[crossed_up | crossed_down, ["Date", "Close", fast_col, slow_col]].copy()
    signals["signal"] = "Compra"
    signals.loc[crossed_down[crossed_down].index, "signal"] = "Venda"
    return signals


def rsi_signal_points(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    key = f"rsi_{period}"
    prev = df[key].shift(1)
    crossed_oversold = (prev <= 30) & (df[key] > 30)
    crossed_overbought = (prev >= 70) & (df[key] < 70)
    signals = df.loc[crossed_oversold | crossed_overbought, ["Date", "Close", key]].copy()
    signals["signal"] = "Saida da sobrevenda"
    signals.loc[crossed_overbought[crossed_overbought].index, "signal"] = "Saida da sobrecompra"
    return signals


def bollinger_signal_points(df: pd.DataFrame) -> pd.DataFrame:
    prev_close = df["Close"].shift(1)
    prev_lower = df["bb_lower_20_2"].shift(1)
    prev_upper = df["bb_upper_20_2"].shift(1)

    reentry_lower = (prev_close <= prev_lower) & (df["Close"] > df["bb_lower_20_2"])
    reentry_upper = (prev_close >= prev_upper) & (df["Close"] < df["bb_upper_20_2"])

    signals = df.loc[
        reentry_lower | reentry_upper,
        ["Date", "Close", "bb_upper_20_2", "bb_lower_20_2"],
    ].copy()
    signals["signal"] = "Reentrada pela banda inferior"
    signals.loc[reentry_upper[reentry_upper].index, "signal"] = "Retorno abaixo da banda superior"
    return signals


def macd_signal_points(df: pd.DataFrame) -> pd.DataFrame:
    prev_macd = df["macd_12_26"].shift(1)
    prev_signal = df["macd_signal_9"].shift(1)
    crossed_up = (
        (prev_macd <= prev_signal)
        & (df["macd_12_26"] > df["macd_signal_9"])
        & (df["macd_12_26"] > 0)
    )
    signals = df.loc[crossed_up, ["Date", "Close", "macd_12_26", "macd_signal_9"]].copy()
    signals["signal"] = "MACD acima do sinal em territorio positivo"
    return signals


def style_axis(ax: plt.Axes, title: str, ylabel: str) -> None:
    ax.set_title(title, fontsize=13, pad=10)
    ax.set_ylabel(ylabel)
    ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.5)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%Y"))
    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_horizontalalignment("right")


def style_tradingview_axis(
    ax: plt.Axes,
    title: str = "",
    xlabel: str = "Data",
    ylabel: str = "",
) -> None:
    if title:
        ax.set_title(title, fontsize=16, loc="left", pad=12)
    ax.set_facecolor("white")
    ax.grid(True, color="#d9e2ef", linewidth=0.6, alpha=0.55)
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position("right")
    ax.spines["left"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_color("#d9e2ef")
    ax.spines["bottom"].set_color("#d9e2ef")
    ax.tick_params(axis="both", labelsize=11, length=0, pad=8)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b/%Y"))
    ax.set_xlabel(xlabel, fontsize=12, labelpad=10)
    ax.set_ylabel(ylabel, fontsize=12, labelpad=14)


def add_last_value_label(ax: plt.Axes, x_value: pd.Timestamp, y_value: float, text: str, color: str) -> None:
    ax.annotate(
        text,
        xy=(x_value, y_value),
        xytext=(0, 3),
        textcoords="offset points",
        va="bottom",
        ha="center",
        fontsize=10.5,
        color="white",
        bbox={"boxstyle": "round,pad=0.25", "fc": color, "ec": color, "lw": 0.0},
        clip_on=False,
    )


def add_color_legend(ax: plt.Axes, items: list[tuple[str, str]]) -> None:
    handles = [
        Line2D([0], [0], color=color, lw=2.0, label=label)
        for label, color in items
    ]
    legend = ax.legend(
        handles=handles,
        loc="upper left",
        fontsize=10,
        frameon=True,
        facecolor="white",
        edgecolor="#d9e2ef",
        fancybox=True,
        framealpha=0.95,
        borderpad=0.6,
        labelspacing=0.6,
        handlelength=2.5,
    )
    legend.get_frame().set_linewidth(0.8)


def annotate_signals(ax: plt.Axes, signals: pd.DataFrame, y_col: str = "Close") -> None:
    if signals.empty:
        return
    buys = signals[signals["signal"].str.contains("Compra|sobrevenda|inferior|positivo", case=False)]
    sells = signals.drop(buys.index)

    if not buys.empty:
        ax.scatter(buys["Date"], buys[y_col], marker="^", s=60, color="#118ab2", label="Sinal de compra")
    if not sells.empty:
        ax.scatter(sells["Date"], sells[y_col], marker="v", s=60, color="#ef476f", label="Sinal de alerta")


def save_figure(fig: plt.Figure, output_dir: Path, filename: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_ma(df: pd.DataFrame, ticker: str, output_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(14, 7))
    price_color = "#2962FF"
    indicator_color = "#F57C00"
    ax.plot(df["Date"], df["Close"], color=price_color, linewidth=1.6)
    ax.plot(df["Date"], df["ma_21"], color=indicator_color, linewidth=0.9)
    style_tradingview_axis(ax, f"{ticker} - Fechamento + SMA 21", ylabel="Preço de fechamento (R$)")
    add_color_legend(ax, [("Preço de fechamento", price_color), ("SMA 21", indicator_color)])
    add_last_value_label(ax, df["Date"].iloc[-1], df["Close"].iloc[-1], f'{df["Close"].iloc[-1]:.2f}', price_color)
    add_last_value_label(ax, df["Date"].iloc[-1], df["ma_21"].iloc[-1], f'{df["ma_21"].iloc[-1]:.2f}', indicator_color)
    return save_figure(fig, output_dir, f"{ticker.lower()}_ma.png")


def plot_ema(df: pd.DataFrame, ticker: str, output_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(14, 7))
    price_color = "#2962FF"
    indicator_color = "#F57C00"
    ax.plot(df["Date"], df["Close"], color=price_color, linewidth=1.6)
    ax.plot(df["Date"], df["ema_21"], color=indicator_color, linewidth=0.9)
    style_tradingview_axis(ax, f"{ticker} - Fechamento + EMA 21", ylabel="Preço de fechamento (R$)")
    add_color_legend(ax, [("Preço de fechamento", price_color), ("EMA 21", indicator_color)])
    add_last_value_label(ax, df["Date"].iloc[-1], df["Close"].iloc[-1], f'{df["Close"].iloc[-1]:.2f}', price_color)
    add_last_value_label(ax, df["Date"].iloc[-1], df["ema_21"].iloc[-1], f'{df["ema_21"].iloc[-1]:.2f}', indicator_color)
    return save_figure(fig, output_dir, f"{ticker.lower()}_ema.png")


def plot_rsi(df: pd.DataFrame, ticker: str, output_dir: Path) -> Path:
    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(14, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1]},
    )

    ax1.plot(df["Date"], df["Close"], color="#2962FF", linewidth=1.6)
    style_tradingview_axis(ax1, f"{ticker} - Fechamento", xlabel="", ylabel="Preço de fechamento (R$)")
    add_color_legend(ax1, [("Preço de fechamento", "#2962FF")])
    add_last_value_label(ax1, df["Date"].iloc[-1], df["Close"].iloc[-1], f'{df["Close"].iloc[-1]:.2f}', "#2962FF")

    ax2.plot(df["Date"], df["rsi_14"], color="#F57C00", linewidth=1.2)
    ax2.axhline(70, color="#ef5350", linestyle="--", linewidth=0.9)
    ax2.axhline(30, color="#26a69a", linestyle="--", linewidth=0.9)
    ax2.fill_between(df["Date"], 70, 100, color="#ef5350", alpha=0.06)
    ax2.fill_between(df["Date"], 0, 30, color="#26a69a", alpha=0.06)
    style_tradingview_axis(ax2, "RSI 14", ylabel="Índice RSI")
    add_color_legend(
        ax2,
        [
            ("RSI 14", "#F57C00"),
            ("Sobrecompra (70)", "#ef5350"),
            ("Sobrevenda (30)", "#26a69a"),
        ],
    )
    ax2.set_ylim(0, 100)
    add_last_value_label(ax2, df["Date"].iloc[-1], df["rsi_14"].iloc[-1], f'{df["rsi_14"].iloc[-1]:.2f}', "#F57C00")
    return save_figure(fig, output_dir, f"{ticker.lower()}_rsi.png")


def plot_bollinger(df: pd.DataFrame, ticker: str, output_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(14, 7))
    close_color = "#2962FF"
    mid_color = "#F57C00"
    upper_color = "#8e24aa"
    lower_color = "#00acc1"
    ax.plot(df["Date"], df["Close"], color=close_color, linewidth=1.5)
    ax.plot(df["Date"], df["bb_mid_20"], color=mid_color, linewidth=0.9)
    ax.plot(df["Date"], df["bb_upper_20_2"], color=upper_color, linewidth=0.9, alpha=0.9)
    ax.plot(df["Date"], df["bb_lower_20_2"], color=lower_color, linewidth=0.9, alpha=0.9)
    ax.fill_between(
        df["Date"],
        df["bb_lower_20_2"],
        df["bb_upper_20_2"],
        color="#90caf9",
        alpha=0.10,
    )
    style_tradingview_axis(ax, f"{ticker} - Bollinger 20,2", ylabel="Preço de fechamento (R$)")
    add_color_legend(
        ax,
        [
            ("Preço de fechamento", close_color),
            ("Média movel 20", mid_color),
            ("Banda superior", upper_color),
            ("Banda inferior", lower_color),
        ],
    )
    add_last_value_label(ax, df["Date"].iloc[-1], df["Close"].iloc[-1], f'{df["Close"].iloc[-1]:.2f}', close_color)
    add_last_value_label(ax, df["Date"].iloc[-1], df["bb_mid_20"].iloc[-1], f'{df["bb_mid_20"].iloc[-1]:.2f}', mid_color)
    return save_figure(fig, output_dir, f"{ticker.lower()}_bollinger.png")


def plot_macd(df: pd.DataFrame, ticker: str, output_dir: Path) -> Path:
    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(14, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1]},
    )

    ax1.plot(df["Date"], df["Close"], color="#2962FF", linewidth=1.6)
    style_tradingview_axis(ax1, f"{ticker} - Fechamento", xlabel="", ylabel="Preço de fechamento (R$)")
    add_color_legend(ax1, [("Preço de fechamento", "#2962FF")])
    add_last_value_label(ax1, df["Date"].iloc[-1], df["Close"].iloc[-1], f'{df["Close"].iloc[-1]:.2f}', "#2962FF")

    ax2.plot(df["Date"], df["macd_12_26"], color="#2962FF", linewidth=1.2)
    ax2.plot(df["Date"], df["macd_signal_9"], color="#F57C00", linewidth=1.2)
    ax2.axhline(0, color="#222222", linestyle="--", linewidth=0.9)
    style_tradingview_axis(ax2, "MACD 12,26,9", ylabel="Valor do MACD")
    add_color_legend(
        ax2,
        [
            ("MACD", "#2962FF"),
            ("Linha de sinal", "#F57C00"),
            ("Linha zero", "#222222"),
        ],
    )
    add_last_value_label(ax2, df["Date"].iloc[-1], df["macd_12_26"].iloc[-1], f'{df["macd_12_26"].iloc[-1]:.2f}', "#2962FF")
    add_last_value_label(ax2, df["Date"].iloc[-1], df["macd_signal_9"].iloc[-1], f'{df["macd_signal_9"].iloc[-1]:.2f}', "#F57C00")
    return save_figure(fig, output_dir, f"{ticker.lower()}_macd.png")


def print_explanations(ticker: str) -> None:
    explanations = [
        f"Ativo escolhido: {ticker}",
        "MA: mede a tendencia por medias simples; o sinal principal ocorre quando a media curta cruza a longa.",
        "EMA: semelhante a MA, mas da peso maior aos precos mais recentes, reagindo mais rapido.",
        "RSI: mede forca relativa de 0 a 100; abaixo de 30 sugere sobrevenda e acima de 70 sugere sobrecompra.",
        "Bollinger: usa media movel e desvio padrao para formar bandas de volatilidade; reentradas nas bandas ajudam a ilustrar reversao.",
        "MACD: compara duas EMAs e sua linha de sinal; cruzamentos positivos acima de zero destacam momentos de tendencia.",
    ]
    print("\n".join(explanations))


def print_monograph_set() -> None:
    lines = [
        "Conjunto sugerido para o texto da monografia:",
        "MA: WEGE3.SA",
        "EMA: BBAS3.SA",
        "RSI: PETR4.SA",
        "Bollinger: MGLU3.SA",
        "MACD: VALE3.SA",
    ]
    print("\n".join(lines))


def selected_plots(indicator: str) -> Iterable[PlotConfig]:
    plots = {
        "ma": PlotConfig("ma", "Media Movel Simples", "ma"),
        "ema": PlotConfig("ema", "Media Movel Exponencial", "ema"),
        "rsi": PlotConfig("rsi", "RSI", "rsi"),
        "bollinger": PlotConfig("bollinger", "Bandas de Bollinger", "bollinger"),
        "macd": PlotConfig("macd", "MACD", "macd"),
    }
    if indicator == "all":
        return plots.values()
    return [plots[indicator]]


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)

    generated_paths: list[Path] = []

    if args.use_monograph_set:
        print_monograph_set()
        for plot_cfg in selected_plots(args.indicator):
            ticker = MONOGRAPH_TICKERS[plot_cfg.name]
            df = enrich_indicators(download_history(ticker, args.start, args.end))
            if plot_cfg.name == "ma":
                generated_paths.append(plot_ma(df, ticker, output_dir))
            elif plot_cfg.name == "ema":
                generated_paths.append(plot_ema(df, ticker, output_dir))
            elif plot_cfg.name == "rsi":
                generated_paths.append(plot_rsi(df, ticker, output_dir))
            elif plot_cfg.name == "bollinger":
                generated_paths.append(plot_bollinger(df, ticker, output_dir))
            elif plot_cfg.name == "macd":
                generated_paths.append(plot_macd(df, ticker, output_dir))
    else:
        ticker = normalize_ticker(args.ticker)
        df = enrich_indicators(download_history(ticker, args.start, args.end))
        for plot_cfg in selected_plots(args.indicator):
            if plot_cfg.name == "ma":
                generated_paths.append(plot_ma(df, ticker, output_dir))
            elif plot_cfg.name == "ema":
                generated_paths.append(plot_ema(df, ticker, output_dir))
            elif plot_cfg.name == "rsi":
                generated_paths.append(plot_rsi(df, ticker, output_dir))
            elif plot_cfg.name == "bollinger":
                generated_paths.append(plot_bollinger(df, ticker, output_dir))
            elif plot_cfg.name == "macd":
                generated_paths.append(plot_macd(df, ticker, output_dir))
        print_explanations(ticker)

    print("\nArquivos gerados:")
    for path in generated_paths:
        print(path)


if __name__ == "__main__":
    main()
