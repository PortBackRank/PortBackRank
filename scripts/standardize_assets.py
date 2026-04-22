'''
Asset CSV Standardizer - Unified Market Data Format Handler

Purpose:
This module standardizes all market asset CSV files to a consistent format
with columns: symbol, name, industry, sector. This ensures uniform data
structure across different market sources (S&P 500, Brazilian B3 indices).

File Formats Handled:
1. SP500.csv: Already in correct format (symbol, name, industry, sector)
2. IBOV.csv, SMLL.csv, IBXX.csv, IFIX.csv: Portuguese format (Código;Ação;...)
3. IBRA.csv, custom_teste.csv: Single column with symbols only

Output Format:
All files are converted to CSV with columns: symbol, name, industry, sector
- symbol: Market ticker identifier
- name: Company/asset name
- industry: Industry classification
- sector: Sector classification
'''

import pandas as pd
import os
from pathlib import Path

# Define base directory for asset files
ASSETS_DIR = Path(__file__).parent.parent / 'assets'

# Dictionary mapping for raw B3 asset info (symbol -> name)
# This will be populated when reading source files
B3_ASSET_NAMES = {}

def standardize_sp500():
    '''
    Process S&P 500 CSV file (already in correct format).
    
    Ensures column order is correct: symbol, name, industry, sector
    SP500.csv is sourced from standard format and may have correct columns,
    but we reorder them to ensure consistency.
    
    File: assets/SP500.csv
    Rows: ~500 companies from S&P 500 index
    '''
    filepath = ASSETS_DIR / 'SP500.csv'
    
    try:
        df = pd.read_csv(filepath)
        
        # Verify required columns exist
        required_cols = ['symbol', 'name', 'industry', 'sector']
        if not all(col in df.columns for col in required_cols):
            print(f'Warning: SP500.csv missing expected columns')
            return False
        
        # Reorder columns to standard format
        df = df[required_cols]
        df.to_csv(filepath, index=False)
        print(f'✓ SP500.csv standardized - {len(df)} rows')
        return True
        
    except Exception as e:
        print(f'✗ Error processing SP500.csv: {str(e)}')
        return False

def standardize_b3_market(filename, encoding='ISO-8859-1', sep=';'):
    '''
    Process Brazilian B3 market index CSV files.
    
    Handles standardization of IBOV, SMLL, IBXX, and IFIX files which use
    Portuguese column headers and semicolon delimiters. Extracts symbol and name
    from the original format and creates standardized output.
    
    Parameters:
    -----------
    filename : str
        CSV filename in assets directory (e.g., 'IBOV.csv')
    encoding : str
        File encoding, default ISO-8859-1 for Portuguese text support
    sep : str
        Column delimiter, default semicolon for B3 files
    
    Process:
    1. Read CSV with proper Brazilian encoding
    2. Extract symbol and name from first two columns
    3. Set industry and sector to 'Unknown' (can be enriched later)
    4. Save in standardized format
    
    Files Processed:
    - IBOV.csv: Bovespa Index (~85 companies)
    - SMLL.csv: Small Cap Index (~115 companies)
    - IBXX.csv: Mid Cap Index (~100 companies)
    - IFIX.csv: Real Estate Funds Index (~115 funds)
    '''
    filepath = ASSETS_DIR / filename
    
    try:
        # Read B3 format CSV (Portuguese headers, semicolon separator)
        df = pd.read_csv(filepath, encoding=encoding, sep=sep)
        
        # Get all column names and clean them
        df.columns = df.columns.str.strip()
        columns = df.columns.tolist()
        
        # Verify we have at least 2 columns
        if len(columns) < 2:
            print(f'✗ {filename}: Expected at least 2 columns, found {len(columns)}')
            return False
        
        # Initialize output dataframe with standard columns
        standardized = pd.DataFrame()
        
        # Extract symbol from first column (always index 0)
        standardized['symbol'] = df[columns[0]].astype(str).str.strip()
        
        # Extract name from second column (always index 1)
        standardized['name'] = df[columns[1]].astype(str).str.strip()
        
        # Add industry and sector columns (set to Unknown for B3 files)
        # These can be enriched later with external data sources
        standardized['industry'] = 'Unknown'
        standardized['sector'] = 'Unknown'
        
        # Remove any rows with empty symbols
        standardized = standardized[standardized['symbol'].str.len() > 0]
        
        # Write standardized CSV file
        standardized.to_csv(filepath, index=False)
        print(f'✓ {filename} standardized - {len(standardized)} rows')
        return True
        
    except Exception as e:
        print(f'✗ Error processing {filename}: {str(e)}')
        return False

def standardize_symbol_only(filename, column_name='Codigo'):
    '''
    Process CSV files containing only asset symbols.
    
    Handles simplified formats (IBRA.csv, custom_teste.csv) that contain
    only a single column with symbol identifiers. These files are converted
    to the standard 4-column format.
    
    Parameters:
    -----------
    filename : str
        CSV filename in assets directory (e.g., 'IBRA.csv')
    column_name : str
        Expected header name for the symbol column
    
    Process:
    1. Read single-column CSV file
    2. Extract all symbols
    3. Create standardized dataframe with 4 required columns
    4. Use symbol as placeholder for name field
    5. Set industry and sector to 'Unknown' for enrichment later
    
    Files Processed:
    - IBRA.csv: Brazilian wide index (~175 companies)
    - custom_teste.csv: Custom test dataset (~15 companies)
    
    Note:
    For these files, the 'name' field uses the symbol value as a placeholder.
    Industry and sector are set to 'Unknown' awaiting enrichment from
    external data sources or manual curation.
    '''
    filepath = ASSETS_DIR / filename
    
    try:
        # Read CSV file with single column
        df = pd.read_csv(filepath)
        
        # Get the first column (handles header variations)
        col = df.columns[0].strip()
        
        # Extract and clean symbols
        symbols = df[col].str.strip().tolist()
        
        # Initialize standardized dataframe
        standardized = pd.DataFrame()
        standardized['symbol'] = symbols
        standardized['name'] = symbols  # Use symbol as name placeholder
        standardized['industry'] = 'Unknown'
        standardized['sector'] = 'Unknown'
        
        # Write standardized CSV file
        standardized.to_csv(filepath, index=False)
        print(f'✓ {filename} standardized - {len(standardized)} rows')
        return True
        
    except Exception as e:
        print(f'✗ Error processing {filename}: {str(e)}')
        return False

def main():
    '''
    Main execution function for asset CSV standardization.
    
    Process Flow:
    1. Display initialization message
    2. Process SP500.csv (already formatted)
    3. Process B3 market index files (IBOV, SMLL, IBXX, IFIX)
    4. Process symbol-only files (IBRA, custom_teste)
    5. Display completion summary
    
    Output:
    All files are standardized to the format:
    symbol, name, industry, sector
    
    Success is indicated by checkmark (✓) and row count.
    Errors are indicated by cross (✗) and error message.
    '''
    print('=' * 70)
    print('Asset CSV Standardizer - Market Data Unification Tool')
    print('=' * 70)
    print('\nProcessing asset CSV files to standard format...\n')
    
    results = {
        'success': 0,
        'failed': 0
    }
    
    # Process S&P 500 (already in correct format, just reorganize columns)
    print('Processing S&P 500 index...')
    if standardize_sp500():
        results['success'] += 1
    else:
        results['failed'] += 1
    
    # Process Brazilian market indices with Portuguese format
    print('\nProcessing Brazilian (B3) market indices...')
    b3_files = ['IBOV.csv', 'SMLL.csv', 'IBXX.csv', 'IFIX.csv']
    for filename in b3_files:
        if standardize_b3_market(filename):
            results['success'] += 1
        else:
            results['failed'] += 1
    
    # Process simplified symbol-only files
    print('\nProcessing symbol-only market files...')
    symbol_files = [
        ('IBRA.csv', 'Codigo'),
        ('custom_teste.csv', 'Codigo')
    ]
    for filename, col_name in symbol_files:
        if standardize_symbol_only(filename, col_name):
            results['success'] += 1
        else:
            results['failed'] += 1
    
    # Display completion summary
    print('\n' + '=' * 70)
    print('Standardization Complete')
    print('=' * 70)
    print(f'Successfully processed: {results["success"]} files')
    if results['failed'] > 0:
        print(f'Failed: {results["failed"]} files')
    
    print('\nAll asset CSV files now use standard format:')
    print('  Columns: symbol, name, industry, sector')
    print('  Usage: Ready for backtesting and analysis')
    print('=' * 70)

if __name__ == '__main__':
    main()
