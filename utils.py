import json
import os
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np


def convert_numpy(obj):
    """Convert numpy objects to JSON-serializable Python types."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def get_safe_int(value):
    """Ensure the value is an integer if applicable."""
    return int(value) if isinstance(value, (int, np.integer)) else value


def generate_filename(prefix, result, start_date, end_date):
    """
    Generate filenames in a centralized manner.
    
    Args:
        prefix: Directory prefix path.
        result: Dictionary containing profit, loss, diversification, and window parameters.
        start_date: Starting date for the simulation.
        end_date: Ending date for the simulation.
    
    Returns:
        Full path to the generated filename.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Split the prefix into parts
    prefix_parts = prefix.split('/')
    
    # The last element is the base filename
    file_prefix = prefix_parts[-1] if len(prefix_parts) > 0 else prefix
    
    # Previous elements are subdirectories
    subdirs = prefix_parts[:-1] if len(prefix_parts) > 1 else []
    
    # Build the filename with parameters
    window = result.get('window', [0, 0]) 
    filename = f"{file_prefix}_profit{...}_short{get_safe_int(window[0])}_long{get_safe_int(window[1])}.."

    return os.path.join(project_root, 'results', *subdirs, filename)


def save_json(filename, data):
    """
    Save a dictionary as JSON.
    
    Args:
        filename: Output file path.
        data: Dictionary to serialize.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, default=convert_numpy)


def generate_performance_plot(directory: str = 'results', output_prefix: str = 'performance_comparison', market_symbol: str = 'IBrA'):
    """
    Generate a plot showing all simulation lines from JSON files in a directory.
    
    This function reads simulation timeline data from JSON files and plots the performance
    of each simulation strategy alongside the market benchmark.

    Args:
        directory: Folder where the JSON files are located. Defaults to 'results'.
        output_prefix: Prefix for the output plot filename. Defaults to 'performance_comparison'.
        market_symbol: Market index symbol to compare against (e.g., 'IBrA', 'IBOV', 'S&P500'). Defaults to 'IBrA'.
    """
    INITIAL_VALUE = 10_000
    all_percentages = []
    dates = []

    color_palette = plt.cm.get_cmap('tab20').colors[:12]

    plt.figure(figsize=(12, 6))

    color_index = 0
    for filename in os.listdir(directory):
        if filename.endswith('.json') and not filename.startswith('sp500') and not filename.startswith('ibra') and not filename.startswith('ibov'):
            try:
                with open(os.path.join(directory, filename), 'r') as timeline_file:
                    timeline = json.load(timeline_file)

                    # Extract parameters from filename
                    params = filename.replace('timeline_', '').replace('.json', '')
                    labels = params.split('_')

                    if len(labels) < 5:
                        print(f'Unexpected filename format: {filename}')
                        continue

                    profit = labels[0].replace('profit', '')
                    loss = labels[1].replace('loss', '')
                    div = labels[2].replace('div', '')
                    short = labels[3].replace('short', '')
                    long = labels[4].replace('long', '')

                    # Calculate total allocation value over time
                    allocation_over_time = [
                        entry['balance'] + sum(item['quantity'] * item['purchase_price']
                                               for item in entry['portfolio'])
                        for entry in timeline
                    ]

                    if not allocation_over_time:
                        continue

                    # Convert to percentage change from initial value
                    allocation_percent = [
                        (value - INITIAL_VALUE) / INITIAL_VALUE * 100 for value in allocation_over_time]
                    all_percentages.extend(allocation_percent)

                    # Extract dates from timeline (only once)
                    if not dates:
                        dates = [datetime.strptime(entry['date'], '%Y-%m-%d') for entry in timeline]

                    # Plot simulation line
                    plt.plot(dates, allocation_percent, label=f'Profit={profit}, Loss={loss}, '
                             f'Div={div}, Short={short}, Long={long}', color=color_palette[color_index])

                    color_index = (color_index + 1) % len(color_palette)

            except FileNotFoundError:
                print(f'File not found: {filename}')
                continue

    # Load and plot market benchmark
    market_values = []
    symbol = market_symbol.lower()
    print(symbol)
    try:
        with open(os.path.join(directory, symbol + '.json'), 'r') as market_file:
            market_data = json.load(market_file)
            market_values = [entry['value'] for entry in market_data]
            market_dates = [datetime.strptime(entry['date'], '%Y-%m-%d') for entry in market_data]

            market_initial = market_values[0]
            market_percent = [(value - market_initial) / market_initial * 100 for value in market_values]

            plt.plot(market_dates, market_percent, label=market_symbol,
                     color='black', linestyle='dashed', linewidth=2)

            all_percentages.extend(market_percent)
    except FileNotFoundError:
        print(f'{market_symbol} data file not found. Benchmark line not added.')

    # Set y-axis limits with margin
    if all_percentages:
        y_min = min(all_percentages)
        y_max = max(all_percentages)
        y_range = y_max - y_min
        margin = y_range * 0.10
        plt.ylim(y_min - margin, y_max + margin)

    # Format x-axis
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%Y'))
    plt.xticks(rotation=45, fontsize=12)

    # Labels and formatting
    plt.xlabel('Period', fontsize=12)
    plt.ylabel('Percent Change (%)', fontsize=14)

    plt.axhline(0, color='black', linestyle='--', linewidth=1)
    plt.legend(loc='upper left', fontsize=8.5)
    plt.grid(True)
    plt.tight_layout()

    # Save and display
    plt.savefig(f'{directory}/{output_prefix}.png', format='png')
    plt.show()

    plt.close()

# Example usage:
# generate_performance_plot(market_symbol='IBrA')
