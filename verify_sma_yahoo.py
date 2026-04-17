from pathlib import Path

import pandas as pd
import yfinance as yf


TICKERS = ["CSNA3.SA"]
START_DATE = "2025-01-01"
END_DATE = "2025-12-31"
OUTPUT_DIR = Path("results") / "yahoo_checks"


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for ticker in TICKERS:
    df = yf.download(
        ticker,
        start=START_DATE,
        end=END_DATE,
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        print(f"Nenhum dado encontrado para {ticker}.")
        continue

    df = df[["Close"]].copy().reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    output_file = OUTPUT_DIR / f"{ticker.replace('.SA', '').lower()}_close_2025.csv"
    df.to_csv(output_file, index=False)
    print(f"Arquivo salvo: {output_file}")
