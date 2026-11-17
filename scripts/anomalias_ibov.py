import csv
import os
import time

import pandas as pd
import yfinance as yf

INPUT = os.path.join("assets", "IBOV.csv")
REPORT = os.path.join("reports", "anomalias_ibov.csv")
META_REPORT = os.path.join("reports", "anomalias_ibov_meta.csv")


def analyze_ticker(sym, period="5y"):
    ysym = sym + ".SA"
    t = yf.Ticker(ysym)
    h = t.history(period=period, auto_adjust=True)
    anomalies = []
    meta = {"ticker": sym, "ysymbol": ysym, "rows": 0, "first_date": "", "last_date": "", "error": ""}
    if h is None or len(h) == 0:
        meta["error"] = "vazio (sem dados no yfinance)"
        return anomalies, meta
    meta["rows"] = int(len(h))
    try:
        meta["first_date"] = h.index.min().strftime("%Y-%m-%d")
        meta["last_date"] = h.index.max().strftime("%Y-%m-%d")
    except Exception:
        meta["first_date"] = str(h.index.min())
        meta["last_date"] = str(h.index.max())

    h = h.copy()
    h["Close_prev"] = h["Close"].shift(1)
    h["med20"] = h["Close"].shift(1).rolling(window=20, min_periods=5).median()
    has_splits = "Stock Splits" in h.columns

    for i in range(1, len(h)):
        idx = h.index[i]
        cur = h["Close"].iloc[i]
        prev = h["Close_prev"].iloc[i]
        if pd.isna(prev) or prev == 0 or pd.isna(cur):
            continue
        ratio = cur / prev
        reasons = []
        # (a) razão Close_t / Close_{t-1} > 10 ou < 0.1
        if ratio > 10 or ratio < 0.1:
            reasons.append("a_razao_extrema(>10 ou <0.1)")
        # (b) mudança brusca de ordem de grandeza vs mediana janela 20 pregões anteriores
        med20 = h["med20"].iloc[i]
        if not pd.isna(med20) and med20 != 0:
            r_med = cur / med20
            if r_med > 10 or r_med < 0.1:
                reasons.append("b_ordem_grandeza_vs_mediana20(>10x ou <0.1x)")
        if reasons:
            split_flag = "não"
            # (c) coincidência com evento em Stock Splits
            if has_splits:
                try:
                    sv = h["Stock Splits"].iloc[i]
                    sv_prev = h["Stock Splits"].iloc[i - 1]
                    if (not pd.isna(sv) and sv != 0) or (not pd.isna(sv_prev) and sv_prev != 0):
                        split_flag = "sim"
                        reasons.append("c_evento_stock_splits_possivel_evento_corporativo")
                except Exception:
                    pass
            anomalies.append(
                {
                    "ticker": sym,
                    "data": idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx),
                    "preco_anterior": float(prev),
                    "preco_atual": float(cur),
                    "razao": float(ratio),
                    "criterio_que_disparou": ";".join(reasons),
                    "possivel_evento_corporativo": split_flag,
                }
            )
    return anomalies, meta


def main():
    os.makedirs("reports", exist_ok=True)
    with open(INPUT, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="|")
        tickers = [r["symbol"].strip() for r in reader]

    all_anom = []
    all_meta = []
    for i, sym in enumerate(tickers):
        try:
            an, meta = analyze_ticker(sym)
        except Exception as e:
            an = []
            meta = {"ticker": sym, "ysymbol": sym + ".SA", "rows": 0, "first_date": "", "last_date": "", "error": repr(e)}
        all_anom.extend(an)
        all_meta.append(meta)
        print(f"[{i+1}/{len(tickers)}] {sym} rows={meta['rows']} first={meta['first_date']} anom={len(an)} err={meta['error']}")
        time.sleep(0.2)

    with open(REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["ticker", "data", "preco_anterior", "preco_atual", "razao", "criterio_que_disparou", "possivel_evento_corporativo"],
        )
        w.writeheader()
        for r in all_anom:
            w.writerow(r)

    with open(META_REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ticker", "ysymbol", "rows", "first_date", "last_date", "error"])
        w.writeheader()
        for r in all_meta:
            w.writerow(r)

    signaled = sorted({r["ticker"] for r in all_anom})
    none = [m["ticker"] for m in all_meta if m["ticker"] not in set(signaled)]
    print("TOTAL_TICKERS", len(tickers))
    print("SINALIZADOS", len(signaled))
    print("SEM_ANOMALIA", len(none))
    print("ANOMALIAS_TOTAL", len(all_anom))
    print("SINALIZADOS_LISTA", signaled)
    for t in signaled:
        dates = [r["data"] for r in all_anom if r["ticker"] == t]
        print("TICKER_SINALIZADO", t, "N_DATAS", len(dates), "DATAS", dates)
    print("---META_POR_TICKER---")
    for m in all_meta:
        print(m["ticker"], "rows=", m["rows"], "first_date=", m["first_date"], "error=", m["error"])


if __name__ == "__main__":
    main()
