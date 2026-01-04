"""
Debug script to inspect HMIS health data Excel file structure
Reads the first available .xls file and displays its structure and column names
"""

import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import FILE_PATHS
from src.utils import setup_logger

# Setup logger
logger = setup_logger(__name__, log_type="processing")


def debug_hmis_file():
    """
    Inspect the structure of the first available Excel file in data/raw/health_2019_20/
    """
    # Define data directory
    data_dir = Path("data/raw/health_2019_20")
    
    if not data_dir.exists():
        logger.error(f"Health data directory not found: {data_dir}")
        print(f"\n❌ Error: Directory not found: {data_dir}")
        print("Please ensure the .xls files are in data/raw/health_2019_20/")
        return
    
    # Find the first .xls file
    xls_files = sorted(list(data_dir.glob("*.xls")))
    
    if not xls_files:
        logger.error(f"No .xls files found in {data_dir}")
        print(f"\n❌ Error: No .xls files found in {data_dir}")
        return
    
    first_file = xls_files[0]
    logger.info(f"Inspecting file: {first_file.name}")
    print("\n" + "="*80)
    print(f"INSPECTING FILE: {first_file.name}")
    print("="*80)
    
    # Read using pd.read_html (since we know it is HTML)
    try:
        html_tables = pd.read_html(str(first_file))
        
        if not html_tables:
            logger.error(f"No tables found in HTML file: {first_file.name}")
            print(f"\n❌ Error: No tables found in HTML file")
            return
        
        print(f"\nFound {len(html_tables)} table(s) in the HTML file")
        
        # Find the table with District column
        df = None
        table_idx = 0
        
        for idx, table in enumerate(html_tables):
            # Check if this table has a column containing 'District'
            has_district = False
            for col in table.columns:
                col_str = str(col).lower()
                if 'district' in col_str:
                    has_district = True
                    break
            
            # Also check first few rows in case columns aren't properly detected
            if not has_district and len(table) > 0:
                for row_idx in range(min(5, len(table))):
                    row_str = ' '.join([str(val).lower() for val in table.iloc[row_idx].values[:15] if pd.notna(val)])
                    if 'district' in row_str:
                        has_district = True
                        break
            
            if has_district:
                df = table
                table_idx = idx
                print(f"✓ Selected table {idx + 1} (contains 'District' column)")
                break
        
        # If no table with District found, use the first table
        if df is None:
            print(f"⚠ Warning: No table with 'District' column found, using first table")
            df = html_tables[0]
        
        print(f"\nTable shape: {df.shape} (rows, columns)")
        print(f"Table index: {table_idx + 1} of {len(html_tables)}")
        
        # Print all column names
        print("\n" + "="*80)
        print("ALL COLUMN NAMES:")
        print("="*80)
        for idx, col in enumerate(df.columns):
            print(f"  [{idx:2d}] {col}")
        
        # Print first 10 rows
        print("\n" + "="*80)
        print("FIRST 10 ROWS:")
        print("="*80)
        print(df.head(10).to_string())
        
        # Additional info: Show data types
        print("\n" + "="*80)
        print("COLUMN DATA TYPES:")
        print("="*80)
        for col in df.columns:
            dtype = df[col].dtype
            non_null_count = df[col].notna().sum()
            print(f"  {col:50s} | {str(dtype):15s} | {non_null_count:5d} non-null values")
        
        # Show sample values for each column
        print("\n" + "="*80)
        print("SAMPLE VALUES (first non-null value) FOR EACH COLUMN:")
        print("="*80)
        for col in df.columns:
            sample_val = df[col].dropna().iloc[0] if df[col].notna().any() else "N/A"
            print(f"  {col:50s} | {str(sample_val)[:60]}")
        
        print("\n" + "="*80)
        print(f"✓ Inspection complete for: {first_file.name}")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"Error reading file {first_file.name}: {e}", exc_info=True)
        print(f"\n❌ Error reading file: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    debug_hmis_file()

