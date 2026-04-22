import json
import os
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np


def convert_numpy(obj):
    '''
    Converts NumPy data types to JSON-compatible Python types.
    
    Used as default serializer for json.dump() when saving results.
    '''
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def get_safe_int(value):
    '''
    Safely converts values to integers when possible.
    
    Returns original value if not an integer type.
    Handles np.integer types properly.
    '''
    return int(value) if isinstance(value, (int, np.integer)) else value


def generate_filename(prefix, result, start_date, end_date):
    '''
    Generates standardized result filenames from configuration.
    
    Creates consistent naming scheme: 
    results/{prefix}_profit{X}_loss{Y}_div{Z}_short{A}_long{B}_{start}_{end}.json
    '''
    return f'results/{prefix}_profit{get_safe_int(result["profit"])}_loss{get_safe_int(result["loss"])}_div{get_safe_int(result["diversification"])}_short{get_safe_int(result["window"][0])}_long{get_safe_int(result["window"][1])}_{start_date}_to_{end_date}.json'


def save_json(filename, data):
    '''
    Persists simulation results to JSON file.
    
    Creates output directory if needed and saves data with
    NumPy type conversion for compatibility.
    
    :param filename: Output file path
    :param data: Data structure to serialize
    '''
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, default=convert_numpy)


def generate_performance_plot(directory: str = 'results', output_prefix: str = 'performance_comparison', market_symbol: str = 'IBrA'):
    '''
    Generates performance comparison visualization for all simulations.
    
    Creates line plot showing portfolio value evolution over time
    for all backtesting scenarios and compares against market benchmark.

    :param directory: Directory containing result JSON files
    :param output_prefix: Name prefix for output PNG file
    :param market_symbol: Benchmark market symbol (e.g., 'SP500', 'IBrA')
    '''

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

                    params = filename.replace(
                        'timeline_', '').replace('.json', '')
                    labels = params.split('_')

                    if len(labels) < 5:
                        print(
                            f'Unexpected filename format: {filename}')
                        continue

                    profit = labels[0].replace('profit', '')
                    loss = labels[1].replace('loss', '')
                    div = labels[2].replace('div', '')
                    short = labels[3].replace('short', '')
                    long = labels[4].replace('long', '')

                    allocation_over_time = [
                        entry['balance'] + sum(item['quantity'] * item['purchase_price']
                                               for item in entry['portfolio'])
                        for entry in timeline
                    ]

                    if not allocation_over_time:
                        continue

                    allocation_percent = [
                        (value - INITIAL_VALUE) / INITIAL_VALUE * 100 for value in allocation_over_time]
                    all_percentages.extend(allocation_percent)

                    if not dates:
                        dates = [datetime.strptime(
                            entry['date'], '%Y-%m-%d') for entry in timeline]

                    plt.plot(dates, allocation_percent, label=f'Profit={profit}, Loss={loss}, '
                             f'Div={div}, Short={short}, Long={long}', color=color_palette[color_index])

                    color_index = (color_index + 1) % len(color_palette)

            except FileNotFoundError:
                print(f'File not found: {filename}')
                continue

    sp500_values = []
    symbol = market_symbol.lower()
    print(symbol)
    try:
        with open(os.path.join(directory, symbol+'.json'), 'r') as sp500_file:
            sp500_data = json.load(sp500_file)
            sp500_values = [entry['value'] for entry in sp500_data]
            sp500_dates = [datetime.strptime(
                entry['date'], '%Y-%m-%d') for entry in sp500_data]

            sp500_initial = sp500_values[0]
            sp500_percent = [(value - sp500_initial) /
                             sp500_initial * 100 for value in sp500_values]

            plt.plot(sp500_dates, sp500_percent, label=market_symbol,
                     color='black', linestyle='dashed', linewidth=2)

            all_percentages.extend(sp500_percent)
    except FileNotFoundError:
        print('S&P 500 data file not found. Line not added.')

    if all_percentages:
        y_min = min(all_percentages)
        y_max = max(all_percentages)
        y_range = y_max - y_min
        margin = y_range * 0.10
        plt.ylim(y_min - margin, y_max + margin)

    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%Y'))
    plt.xticks(rotation=45, fontsize=12)

    plt.xlabel('Period', fontsize=12)
    plt.ylabel('Percentage Change (%)', fontsize=14)

    plt.axhline(0, color='black', linestyle='--', linewidth=1)
    plt.legend(loc='upper left', fontsize=8.5)
    plt.grid(True)
    plt.tight_layout()

    plt.savefig(f'{directory}/{output_prefix}.png', format='png')
    plt.show()

    plt.close()

# DOES NOT WORK VERY WELL
# generate_performance_plot(market_symbol='IBrA')
