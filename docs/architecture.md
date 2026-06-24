# System Architecture

PortBackRank is structured into independent modules that interact sequentially to manage data, rank assets, and simulate investment portfolios.

## Execution Flow

1. **Initialization (`main.py`)**: 
   Reads the JSON configuration, parses arguments, and initializes the simulation. Determines the parameter grid configurations (Runner Grid and Ranker Grid) and the price type (`price-type`).
2. **Data Fetching (`data.py`)**:
   - Resolves the market (identifier, symbol list).
   - Downloads or loads historical data via the **MegaDataFrame** (Multi-Indexed DataFrame by `Symbol` and `Date`).
3. **Backtesting (`backtesting.py`)**:
   - Runs the simulation for each parameter grid combination using `ParameterGrid`.
   - Can run simulations in parallel if multi-execution is enabled.
4. **Ranking (`ranker.py`)**:
   - Processes the MegaDataFrame to pre-calculate indicators (e.g., Moving Averages in the `prepare()` method).
   - Provides daily lists of assets ordered by their strategy.
5. **Runner (`runner.py`)**:
   - The core portfolio engine.
   - Every day, it simulates the sale of assets that hit the Stop Loss or Take Profit (based on the configured maximum trade volume).
   - Simulates the purchase of the best assets indicated by the `Ranker`, applying the maximum sector diversification rule.
   - Generates the trace logs (trade log and portfolio log) if the `trace` parameter is enabled.

## Class Diagram

*(You can consult the original `class_diagram.drawio` file in the respective folder to edit the visual model, if available).*

The primary relationships between classes are:

- `MemData` aggregates a massive `pd.DataFrame`.
- `Backtesting` is composed of `Runner`s and `Ranker`s.
- `Runner` uses the base interface of a generic `Ranker` and accesses the history maintained by `MemData`.
