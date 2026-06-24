# Getting Started

This guide will walk you through the installation, configuration, and execution of your first simulation in PortBackRank.

## 1. Installation

The project is built in Python. Using a virtual environment (venv, conda) is recommended.
Clone the repository and install the dependencies:

```bash
git clone https://github.com/PortBackRank/PortBackRank.git
cd PortBackRank
pip install -r requirements.txt
```

## 2. Configuring the Simulation (`config.json`)

Every PortBackRank simulation is primarily controlled by the `config.json` file and command-line arguments.
A basic configuration example:

```json
{
    "id": "sp500",
    "interval": ["2015-01-01", "2024-01-01"],
    "capital": 100000.0,
    "ranker": "MARanker",
    "price-type": ["C", "HL2"],
    "ranker_params": {
        "window": [9, 21, 50],
        "profit": [0.1, 0.15],
        "loss": [0.05, 0.08],
        "diversification": [0.05, 0.1],
        "volume": [0.1]
    }
}
```

### Main Parameters
- **`id`**: Market or asset set identifier (e.g., `sp500`).
- **`interval`**: Simulation start and end dates `[start, end]`.
- **`capital`**: Initial capital available for the simulation.
- **`ranker`**: Ranking algorithm to be used (e.g., `MARanker`, `RSIRanker`).
- **`price-type`**: Price types to consider. Can be a single string or a list. Options: `O` (Open), `H` (High), `L` (Low), `C` (Close), `HL2`, etc.
- **`ranker_params`**: Parameter grids for the backtest. Arrays here will generate multiple combinations (Grid Search).

## 3. Running the Simulation

To execute, run `main.py` passing the configuration file:

```bash
python main.py -c config.json
```

**Useful command-line arguments:**
- `-d` or `--download-data`: Download strategy (`all`, `missing`, `none`). Default is `missing`.
- `-p` or `--price-type`: Overrides the price type defined in the configuration file.
- `-t` or `--trace`: Enables trace mode (generates detailed logs in the `tracking/` folder).

```bash
# Example with trace enabled and forcing price type "C"
python main.py -c config.json -t -p C
```
