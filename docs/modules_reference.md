# Modules Reference

This page provides an in-depth view of the responsibilities and essential methods of the core classes in the PortBackRank system.

## 1. Module `data.py` (Data Management)

- **`Data`**: Utility class responsible for downloading (via scraping or external libraries like `yfinance`) stock histories and saving them to disk as `.csv` files.
- **`MemData`**: Loads historical data into memory. Its most important feature is the construction of the **MegaDataFrame**, an optimized structure using multi-indexes (`Date`, `Symbol`) for fast daily access during simulations, eliminating repeated and slow reads. Supports different price types (`price_type`).

## 2. Module `ranker.py` (Evaluation Strategies)

Defines the strategies that choose which assets to buy.

- **`Ranker` (Base Class)**: Defines the methods every strategy must have, especially the abstract method `rank(date)`.
- **`MARanker`**: Implementation based on Moving Averages Crossover. It has a `prepare()` method that pre-calculates all moving averages in a vectorized manner on the MegaDataFrame before the simulation starts.
- **`RSIRanker`**: Implementation based on the Relative Strength Index.

> [!TIP]
> **Creating a Custom Ranker:** Create a new class inheriting from `Ranker`. Implement `prepare()` (if you can vectorize indicator calculations) and `rank(date)`, returning a list of strings with the `symbols` of the top-ranked assets for that specific date.

## 3. Module `runner.py` (Portfolio Execution Engine)

- **`Runner`**: The central portfolio control class.
  - `_sell(date)`: Checks if any asset in the portfolio has hit the target profitability (`profit`) or the maximum tolerable risk (`loss`). Sells while respecting the daily trading volume limit of the asset (`volume`).
  - `_buy(date)`: Based on the `Ranker`'s recommendations, buys new assets until available cash is depleted, respecting sector diversification limits (`diversification`).
  - `single_run(...)`: Main loop that steps day-by-day through the specified time range, sequentially calling `_sell` and `_buy`.

## 4. Module `backtesting.py` (Coordination)

- **`Backtesting`**: Coordinates the `Runner`. It generates combinations grids for the ranker parameters (e.g., windows 9 and 21, windows 50 and 200) combined with runner parameters (e.g., profit 0.1 and 0.15) and collects the consolidated final results, allowing for systematic comparisons.
