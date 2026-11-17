import csv
import os
import time
import traceback
from datetime import datetime

import yfinance as yf

INPUT = os.path.join("assets", "IBOV.csv")
OUT = os.path.join("reports", "inventario_ibov.csv")

def main():
    os.makedirs("reports", exist_ok=True)
    with open(INPUT, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="|")
        rows = list(reader)
    tickers = [r["symbol"].strip() for r in rows]
    results = []
    for i, sym in enumerate(tickers):
        ysym = sym + ".SA"
        res = {
            "symbol": sym,
            "ysymbol": ysym,
            "status": "",
            "mensagem_erro_exata": "",
            "primeira_data": "",
            "ultima_data": "",
            "num_linhas": 0,
            "preco_min": "",
            "preco_max": "",
        }
        try:
            t = yf.Ticker(ysym)
            h = t.history(period="5y", auto_adjust=True)
            if h is None or len(h) == 0:
                res["status"] = "vazio"
                res["mensagem_erro_exata"] = "Sem dados retornados por yfinance (history vazio)"
                results.append(res)
            else:
                res["status"] = "ok"
                res["mensagem_erro_exata"] = ""
                idx = h.index
                try:
                    res["primeira_data"] = idx.min().strftime("%Y-%m-%d")
                    res["ultima_data"] = idx.max().strftime("%Y-%m-%d")
                except Exception:
                    res["primeira_data"] = str(idx.min())
                    res["ultima_data"] = str(idx.max())
                res["num_linhas"] = int(len(h))
                try:
                    res["preco_min"] = float(h["Close"].min())
                    res["preco_max"] = float(h["Close"].max())
                except Exception:
                    res["preco_min"] = ""
                    res["preco_max"] = ""
                results.append(res)
        except Exception as e:
            res["status"] = "erro"
            res["mensagem_erro_exata"] = repr(e)
            results.append(res)
        if i % 10 == 0 and i > 0:
            time.sleep(1)
        else:
            time.sleep(0.2)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "ysymbol", "status", "mensagem_erro_exata", "primeira_data", "ultima_data", "num_linhas", "preco_min", "preco_max"])
        w.writeheader()
        for r in results:
            w.writerow(r)
    print("DONE", OUT)
    from collections import Counter
    c = Counter(r["status"] for r in results)
    print("COUNTS:", dict(c))
    bad = [r for r in results if r["status"] != "ok"]
    print("BAD_COUNT:", len(bad))
    for r in bad:
        print(r["symbol"], r["ysymbol"], r["status"], r["mensagem_erro_exata"])

if __name__ == "__main__":
    main()
