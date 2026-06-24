# Experiments and Tracking

This section details how PortBackRank was structured to meet academic and research needs, specifically the execution of varied experiments and the logging (tracking) for auditing and result tabulation.

## 1. Parameter Grid (Grid Search)

In related studies, it's common to evaluate how different time window sizes, volume limits, or profit percentages (take profit) affect the final return. PortBackRank uses Parameter Grids to automatically explore these combinations.

In `config.json`, the numerical vectors defined in `ranker_params` are combined through Cartesian product:
- `profit`: `[0.10, 0.15]`
- `loss`: `[0.05]`
- `window`: `[9, 21]`

The `Backtesting` module will execute the `Runner` for **each** possible combination and output a final table (`pd.DataFrame`) comparing the initial and final balances of each scenario.

## 2. Price Types

Academic simulations require sensitivity regarding the exact price of a stock on a given day. PortBackRank supports running multiple parallel (or sequential) simulations iterating over different daily calculation types, configurable in the root of `config.json` under `price-type`:
- **`O` (Open)**: Opening price.
- **`H` (High)**: Maximum price.
- **`L` (Low)**: Minimum price.
- **`C` (Close)**: Closing price (Default).
- **`HL2`**: Simple Average (High + Low) / 2.
- **`HLC3`**: (High + Low + Close) / 3.
- **`OHLC4`**: (Open + High + Low + Close) / 4.

## 3. `tracking/` Directory

Full simulation tracking is enabled using the `--trace` or `-t` command-line flag.

This instructs the `Runner` to generate granular files containing the total history of movements, which are essential for writing and validating data for a scientific paper.
For each combination executed in the Grid, the `Runner` generates a unique subfolder based on the parameters of that experiment, for example:

`tracking/sp500-MARanker-9-21-P01-L005-D01/`

In this folder, two CSV files will be written:
1. **`trades.csv`**: Logs every trade made.
   - Columns: `date|symbol|operation|quantity|price|balance`
2. **`portfolio.csv`**: A snapshot of the portfolio on days when changes occur.
   - Columns: `date|symbol|sector|quantity|buy_price|price`

The pipe-separated `|` data structure ensures there are no escape issues with international formatting commas. These files form the basis for creating spreadsheets and charts for final academic research results.
