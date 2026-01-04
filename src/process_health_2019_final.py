"""
Process HMIS health data from HTML-based Excel files (2019-2020)
Handles MultiIndex header structure and extracts diarrhoea inpatient data
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import re
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import FILE_PATHS
from src.utils import setup_logger

# Setup logger
logger = setup_logger(__name__, log_type="processing")


def extract_state_from_filename(filename: str) -> str:
    """
    Extract state name from filename.
    Example: 'Assam.xls' -> 'Assam'
    
    Args:
        filename: Name of the file
    
    Returns:
        State name
    """
    state_name = Path(filename).stem
    return state_name.strip()


def process_health_file_final(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Process a single health data Excel file with MultiIndex header structure.
    
    Args:
        file_path: Path to the Excel file
    
    Returns:
        DataFrame with extracted data in long format, or None if processing fails
    """
    try:
        logger.info(f"Processing file: {file_path.name}")
        
        # Read file using pd.read_html with MultiIndex header
        try:
            html_tables = pd.read_html(str(file_path), header=[0, 1])
            
            if not html_tables:
                logger.error(f"No tables found in HTML file: {file_path.name}")
                return None
            
            # Use the first table
            df = html_tables[0]
            logger.debug(f"Read table with shape {df.shape} and MultiIndex columns")
            
        except Exception as e:
            logger.error(f"Failed to read {file_path.name} as HTML: {e}", exc_info=True)
            return None
        
        # Flatten MultiIndex columns for easier access
        # Create a mapping from original MultiIndex to flat column names
        df_flat = df.copy()
        
        # Flatten the MultiIndex columns
        flat_columns = []
        for col in df.columns:
            if isinstance(col, tuple):
                # Combine both levels with underscore, or use first level if second is empty
                if col[1] and str(col[1]).strip():
                    flat_col = f"{col[0]}_{col[1]}"
                else:
                    flat_col = str(col[0])
            else:
                flat_col = str(col)
            flat_columns.append(flat_col)
        
        df_flat.columns = flat_columns
        
        # Rename columns: Column 0 to District_Name, Column 1 to Indicator
        # Use position-based renaming (columns 0 and 1)
        if len(df_flat.columns) >= 2:
            rename_dict = {
                df_flat.columns[0]: 'District_Name',
                df_flat.columns[1]: 'Indicator'
            }
            df_flat = df_flat.rename(columns=rename_dict)
        else:
            logger.error(f"Expected at least 2 columns, found {len(df_flat.columns)} in {file_path.name}")
            return None
        
        # Filter rows: Keep rows where Indicator contains "Diarrhoea" AND "Inpatient" (case-insensitive)
        if 'Indicator' not in df_flat.columns:
            logger.error(f"Indicator column not found in {file_path.name}")
            logger.debug(f"Available columns: {list(df_flat.columns)}")
            return None
        
        # Filter for Diarrhoea and Inpatient (keep all columns for monthly data extraction)
        mask = (
            df_flat['Indicator'].astype(str).str.lower().str.contains('diarrhoea', na=False) &
            df_flat['Indicator'].astype(str).str.lower().str.contains('inpatient', na=False)
        )
        df_filtered = df_flat[mask].copy()
        
        if df_filtered.empty:
            logger.warning(f"No rows found with 'Diarrhoea' and 'Inpatient' in Indicator for {file_path.name}")
            return None
        
        logger.debug(f"Filtered to {len(df_filtered)} rows with Diarrhoea Inpatient data")
        
        # Extract monthly data
        # Months: April to March (April-Dec = 2019, Jan-March = 2020)
        months = ['April', 'May', 'June', 'July', 'August', 'September', 
                  'October', 'November', 'December', 'January', 'February', 'March']
        
        # Year mapping
        year_mapping = {
            'April': 2019, 'May': 2019, 'June': 2019, 'July': 2019,
            'August': 2019, 'September': 2019, 'October': 2019,
            'November': 2019, 'December': 2019,
            'January': 2020, 'February': 2020, 'March': 2020
        }
        
        # Find columns for each month
        # Pattern: (Month, 'Total [(A+B) or (C+D)]')
        monthly_data = []
        
        for month in months:
            # Look for columns that match the month and have 'Total' in the flattened column name
            month_cols = []
            
            for col in df_flat.columns:
                col_str = str(col).lower()
                # Check if column contains month name and 'Total' with pattern
                if month.lower() in col_str and 'total' in col_str:
                    # Check for the pattern (A+B) or (C+D)
                    if '(a+b)' in col_str or '(c+d)' in col_str or 'a+b' in col_str or 'c+d' in col_str:
                        month_cols.append(col)
            
            # If not found with exact pattern, try simpler match (just month + total)
            if not month_cols:
                for col in df_flat.columns:
                    col_str = str(col).lower()
                    if month.lower() in col_str and 'total' in col_str:
                        month_cols.append(col)
                        break
            
            if month_cols:
                month_col = month_cols[0]  # Take first match
                
                # Extract data for this month from filtered dataframe
                # df_filtered has all columns including monthly columns
                for idx, row in df_filtered.iterrows():
                    district = row['District_Name']
                    # Get cases from the filtered dataframe (which has all columns)
                    if month_col in df_filtered.columns:
                        cases = row[month_col]
                    else:
                        # Fallback: use original index to get from df_flat
                        cases = df_flat.loc[idx, month_col] if idx in df_flat.index else 0
                    
                    monthly_data.append({
                        'District_Name': district,
                        'Month': month,
                        'Year': year_mapping[month],
                        'Cases': cases
                    })
            else:
                logger.warning(f"Column for month {month} not found in {file_path.name}")
                logger.debug(f"Available columns: {[c for c in df_flat.columns if month.lower() in str(c).lower()][:5]}")
        
        if not monthly_data:
            logger.warning(f"No monthly data extracted from {file_path.name}")
            return None
        
        # Create DataFrame from monthly data
        df_monthly = pd.DataFrame(monthly_data)
        
        # Clean Data: Convert 'Cases' to numeric (coerce errors to 0)
        df_monthly['Cases'] = pd.to_numeric(df_monthly['Cases'], errors='coerce').fillna(0)
        
        # Remove rows with invalid district names
        df_monthly = df_monthly[df_monthly['District_Name'].notna()]
        df_monthly = df_monthly[df_monthly['District_Name'].astype(str).str.strip() != '']
        
        # Add State: Extract State Name from filename
        state_name = extract_state_from_filename(file_path.name)
        df_monthly['State'] = state_name
        
        logger.info(f"  Extracted {len(df_monthly)} rows from {file_path.name}")
        
        return df_monthly
        
    except Exception as e:
        logger.error(f"Error processing file {file_path.name}: {e}", exc_info=True)
        return None


def process_all_health_files_final(data_dir: Path) -> pd.DataFrame:
    """
    Process all .xls files in the specified directory.
    
    Args:
        data_dir: Directory containing the Excel files
    
    Returns:
        Concatenated DataFrame with all processed data
    """
    logger.info(f"Starting to process health data files from: {data_dir}")
    
    # Find all .xls files
    xls_files = list(data_dir.glob("*.xls"))
    
    if not xls_files:
        logger.warning(f"No .xls files found in {data_dir}")
        return pd.DataFrame()
    
    logger.info(f"Found {len(xls_files)} .xls files")
    
    # Process each file
    all_dataframes = []
    successful_files = 0
    failed_files = 0
    
    for file_path in sorted(xls_files):
        # Skip certain files if needed
        if file_path.name.lower() in ['all_india.xls', 'all india.xls']:
            logger.info(f"Skipping {file_path.name} (aggregate file)")
            continue
        
        df = process_health_file_final(file_path)
        
        if df is not None and not df.empty:
            all_dataframes.append(df)
            successful_files += 1
        else:
            failed_files += 1
            logger.warning(f"Failed to process {file_path.name}")
    
    # Concatenate all dataframes
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Successfully processed {successful_files} files, {failed_files} failed")
        logger.info(f"Combined dataset: {len(combined_df)} rows, {len(combined_df.columns)} columns")
        return combined_df
    else:
        logger.error("No files were successfully processed")
        return pd.DataFrame()


def main():
    """Main function to process health data and save to CSV"""
    logger.info("Starting health data processing for 2019-2020 (Final Version)")
    
    # Define data directory
    data_dir = Path("data/raw/health_2019_20")
    
    if not data_dir.exists():
        logger.error(f"Health data directory not found: {data_dir}")
        logger.info("Please ensure the .xls files are in data/raw/health_2019_20/")
        raise FileNotFoundError(f"Health data directory not found: {data_dir}")
    
    logger.info(f"Using data directory: {data_dir}")
    
    # Process all files
    combined_df = process_all_health_files_final(data_dir)
    
    if combined_df.empty:
        logger.error("No data was extracted. Please check the file structure.")
        return
    
    # Save to processed data directory
    output_file = Path(FILE_PATHS["data"]["processed"]) / "health_2019_cleaned.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    combined_df.to_csv(output_file, index=False, encoding='utf-8')
    logger.info(f"Saved processed data to: {output_file}")
    logger.info(f"Final dataset shape: {combined_df.shape}")
    
    # Print summary
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    print(f"Total rows: {len(combined_df):,}")
    print(f"Total columns: {len(combined_df.columns)}")
    print(f"States processed: {combined_df['State'].nunique()}")
    print(f"Districts: {combined_df['District_Name'].nunique()}")
    print(f"Months: {combined_df['Month'].nunique()}")
    print(f"\nColumns: {list(combined_df.columns)}")
    print(f"\nFirst few rows:")
    print(combined_df.head(10).to_string())
    
    print(f"\n✓ Health data processing complete!")
    print(f"  Output file: {output_file}")


if __name__ == "__main__":
    main()

