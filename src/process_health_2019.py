"""
Process HMIS health data from Excel files (2019-2020)
Reads .xls files, extracts diarrhoea data, and consolidates into a single dataset
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import re
from typing import Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import FILE_PATHS
from src.utils import setup_logger

# Setup logger
logger = setup_logger(__name__, log_type="processing")


def find_header_row(file_path: Path, max_rows_to_check: int = 10) -> Optional[int]:
    """
    Inspect the Excel file to find the correct header row.
    Looks for rows containing both 'District' and 'Diarrhoea' keywords.
    Tries Excel format first, then falls back to HTML if needed.
    
    Args:
        file_path: Path to the Excel file
        max_rows_to_check: Maximum number of rows to check (default: 10)
    
    Returns:
        Row index (0-based) of the header row, or None if not found
    """
    try:
        # First, try to read as Excel using xlrd
        try:
            import xlrd
            workbook = xlrd.open_workbook(file_path)
            sheet = workbook.sheet_by_index(0)
            
            for skip_rows in range(min(max_rows_to_check, sheet.nrows - 1)):
                try:
                    # Read row values
                    row_values = [str(sheet.cell_value(skip_rows, col_idx)).lower() 
                                 for col_idx in range(min(15, sheet.ncols))]
                    row_str = ' '.join([val for val in row_values if val and val != 'nan'])
                    
                    # Check if this row contains both 'District' and 'Diarrhoea'
                    has_district = 'district' in row_str
                    has_diarrhoea = 'diarrhoea' in row_str or 'diarrhea' in row_str
                    
                    if has_district and has_diarrhoea:
                        logger.debug(f"Found header row at index {skip_rows} in {file_path.name}")
                        return skip_rows
                        
                except Exception as e:
                    logger.debug(f"Error checking row {skip_rows} in {file_path.name}: {e}")
                    continue
        except ImportError:
            logger.debug("xlrd not available for header detection, trying pandas")
        except (ValueError, Exception) as e:
            # Check if error is related to HTML format
            error_str = str(e).lower()
            is_html_error = 'html' in error_str or '<html' in error_str or "found b'<html" in error_str
            
            # Try to import XLRDError if available
            try:
                import xlrd
                XLRDError = xlrd.XLRDError
            except (ImportError, AttributeError):
                XLRDError = None
            
            is_xlrd_error = XLRDError and isinstance(e, XLRDError)
            
            if not (is_html_error or is_xlrd_error):
                logger.debug(f"xlrd failed with non-HTML error: {e}")
        
        # If Excel reading failed or didn't find header, try HTML format
        try:
            html_tables = pd.read_html(str(file_path))
            if html_tables:
                # Find table with District column
                df_temp = None
                for table in html_tables:
                    for col in table.columns:
                        if 'district' in str(col).lower():
                            df_temp = table
                            break
                    if df_temp is not None:
                        break
                
                # If no table with District found, use first table
                if df_temp is None:
                    df_temp = html_tables[0]
                
                # Check first few rows for header
                for i in range(min(max_rows_to_check, len(df_temp))):
                    row_str = ' '.join([str(val).lower() for val in df_temp.iloc[i].values[:15] if pd.notna(val)])
                    has_district = 'district' in row_str
                    has_diarrhoea = 'diarrhoea' in row_str or 'diarrhea' in row_str
                    if has_district and has_diarrhoea:
                        logger.debug(f"Found header row at index {i} in {file_path.name} (HTML format)")
                        return i
        except Exception as e:
            logger.debug(f"Not HTML format or error reading HTML: {e}")
        
        logger.warning(f"Could not find header row with both 'District' and 'Diarrhoea' in {file_path.name}")
        return None
        
    except Exception as e:
        logger.error(f"Error inspecting file {file_path.name}: {e}")
        return None


def find_column_names(df: pd.DataFrame, target_keywords: list) -> dict:
    """
    Find column names that match target keywords (case-insensitive, partial match).
    
    Args:
        df: DataFrame to search
        target_keywords: List of keywords to search for
    
    Returns:
        Dictionary mapping keyword to column name found
    """
    column_mapping = {}
    df_columns_lower = [str(col).lower() for col in df.columns]
    
    for keyword in target_keywords:
        keyword_lower = keyword.lower()
        for idx, col_lower in enumerate(df_columns_lower):
            if keyword_lower in col_lower:
                column_mapping[keyword] = df.columns[idx]
                break
    
    return column_mapping


def extract_state_from_filename(filename: str) -> str:
    """
    Extract state name from filename.
    Example: 'Assam.xls' -> 'Assam'
    
    Args:
        filename: Name of the file
    
    Returns:
        State name
    """
    # Remove file extension
    state_name = Path(filename).stem
    return state_name.strip()


def read_excel_file(file_path: Path, header_row: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Read Excel file, handling both .xls and HTML formats.
    First tries pd.read_excel, then falls back to pd.read_html if the file is actually HTML.
    
    Args:
        file_path: Path to the Excel file
        header_row: Row index to use as header (None to auto-detect)
    
    Returns:
        DataFrame or None if reading fails
    """
    # First, try pd.read_excel (keep existing logic)
    try:
        if header_row is None:
            header_row = 0
        df = pd.read_excel(file_path, engine='xlrd', header=header_row)
        logger.debug(f"Successfully read {file_path.name} using pd.read_excel with xlrd")
        return df
    except Exception as e:
        # Check if error is related to HTML format (e.g., "found b'<html xm'")
        error_str = str(e).lower()
        is_html_error = 'html' in error_str or '<html' in error_str or "found b'<html" in error_str
        
        # Try to import XLRDError if available
        try:
            import xlrd
            XLRDError = xlrd.XLRDError
        except (ImportError, AttributeError):
            XLRDError = None
        
        # Check if it's an XLRDError or HTML-related ValueError
        is_xlrd_error = XLRDError and isinstance(e, XLRDError)
        
        if is_html_error or is_xlrd_error:
            logger.debug(f"File appears to be HTML format, trying pd.read_html: {e}")
            
            # Exception Handling: If that fails, try pd.read_html(file_path)
            try:
                html_tables = pd.read_html(str(file_path))
                
                if not html_tables:
                    logger.error(f"No tables found in HTML file: {file_path.name}")
                    return None
                
                # pd.read_html returns a list of tables. Select the table that contains 'District' or 'District Name'
                df = None
                for table in html_tables:
                    # Check if this table has a column containing 'District' or 'District Name'
                    has_district = False
                    for col in table.columns:
                        col_str = str(col).lower()
                        if 'district' in col_str or 'district name' in col_str:
                            has_district = True
                            break
                    
                    # Also check first few rows in case columns aren't properly detected
                    if not has_district and len(table) > 0:
                        for idx in range(min(5, len(table))):
                            row_str = ' '.join([str(val).lower() for val in table.iloc[idx].values[:15] if pd.notna(val)])
                            if 'district' in row_str:
                                has_district = True
                                break
                    
                    if has_district:
                        df = table
                        logger.debug(f"Selected table with District column from {file_path.name}")
                        break
                
                # If no table with District found, use the first table
                if df is None:
                    logger.warning(f"No table with District column found, using first table from {file_path.name}")
                    df = html_tables[0]
                
                # If header_row is specified, use that row as header
                if header_row is not None and header_row < len(df):
                    # Set the specified row as column names
                    df.columns = df.iloc[header_row]
                    df = df.iloc[header_row + 1:].reset_index(drop=True)
                    logger.debug(f"Applied header row {header_row} to HTML-parsed dataframe")
                
                logger.info(f"Successfully read {file_path.name} as HTML format")
                return df
                
            except Exception as html_error:
                logger.error(f"Failed to read {file_path.name} as HTML: {html_error}")
                return None
        else:
            # Not an HTML error, re-raise or log
            logger.error(f"Failed to read {file_path.name} with pd.read_excel: {e}")
            return None
    
    # If we get here, all methods failed
    logger.error(f"Failed to read {file_path.name} with all methods")
    return None


def process_health_file(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Process a single health data Excel file.
    
    Args:
        file_path: Path to the Excel file
    
    Returns:
        DataFrame with extracted data, or None if processing fails
    """
    try:
        logger.info(f"Processing file: {file_path.name}")
        
        # Find the header row
        header_row = find_header_row(file_path, max_rows_to_check=10)
        
        if header_row is None:
            logger.warning(f"Could not find header row in {file_path.name}, trying default (row 0)")
            header_row = 0
        
        # Read the Excel file
        df = read_excel_file(file_path, header_row=header_row)
        
        if df is None or df.empty:
            logger.error(f"Could not read data from {file_path.name}")
            return None
        
        # Clean column names (remove extra whitespace, newlines)
        df.columns = df.columns.str.strip().str.replace('\n', ' ').str.replace('\r', ' ')
        
        logger.debug(f"Columns found in {file_path.name}: {list(df.columns)[:10]}...")
        
        # Find required columns
        # Target: 'District Name', 'Month', and diarrhoea-related column
        target_keywords = {
            'district': ['district name', 'district', 'dist'],
            'month': ['month', 'period', 'time'],
            'diarrhoea': ['diarrhoea', 'diarrhea', 'childhood diseases']
        }
        
        column_mapping = {}
        
        # Find district column - prefer 'District Name'
        district_cols = find_column_names(df, target_keywords['district'])
        if district_cols:
            # Prefer 'District Name' if available
            if 'district name' in [k.lower() for k in district_cols.keys()]:
                column_mapping['district'] = district_cols.get('district name') or list(district_cols.values())[0]
            else:
                column_mapping['district'] = list(district_cols.values())[0]
        else:
            logger.warning(f"District column not found in {file_path.name}")
            logger.debug(f"Available columns: {list(df.columns)}")
            return None
        
        # Find month column (optional)
        month_cols = find_column_names(df, target_keywords['month'])
        if month_cols:
            column_mapping['month'] = list(month_cols.values())[0]
        
        # Find diarrhoea column - look for exact match first, then patterns
        # User specified: 'Childhood Diseases - Diarrhoea treated in Inpatients'
        diarrhoea_col = None
        
        # First, try exact match (case-insensitive)
        exact_match = 'Childhood Diseases - Diarrhoea treated in Inpatients'
        for col in df.columns:
            if str(col).strip().lower() == exact_match.lower():
                diarrhoea_col = col
                logger.debug(f"Found exact match for diarrhoea column: {col}")
                break
        
        # If not found, try pattern matching
        if not diarrhoea_col:
            diarrhoea_keywords = [
                'childhood diseases.*diarrhoea.*inpatient',
                'childhood diseases.*diarrhoea.*treated',
                'diarrhoea.*inpatient',
                'diarrhoea.*treated',
                'diarrhoea',
                'diarrhea'
            ]
            
            for keyword in diarrhoea_keywords:
                for col in df.columns:
                    col_lower = str(col).lower()
                    if re.search(keyword.lower(), col_lower):
                        diarrhoea_col = col
                        logger.debug(f"Found pattern match for diarrhoea column: {col}")
                        break
                if diarrhoea_col:
                    break
        
        if not diarrhoea_col:
            logger.warning(f"Diarrhoea column not found in {file_path.name}")
            logger.debug(f"Available columns: {list(df.columns)}")
            return None
        
        column_mapping['diarrhoea'] = diarrhoea_col
        
        # Extract required columns
        cols_to_extract = [column_mapping['district'], column_mapping['diarrhoea']]
        if 'month' in column_mapping:
            cols_to_extract.append(column_mapping['month'])
        
        # Create subset dataframe
        df_extracted = df[cols_to_extract].copy()
        
        # Rename columns to standard names
        rename_dict = {
            column_mapping['district']: 'District_Name',
            column_mapping['diarrhoea']: 'Diarrhoea_Cases'
        }
        if 'month' in column_mapping:
            rename_dict[column_mapping['month']] = 'Month'
        
        df_extracted = df_extracted.rename(columns=rename_dict)
        
        # Add state column from filename
        state_name = extract_state_from_filename(file_path.name)
        df_extracted['State'] = state_name
        
        # Remove rows where district is missing or diarrhoea data is missing/invalid
        df_extracted = df_extracted.dropna(subset=['District_Name'])
        
        # Convert diarrhoea to numeric, handling any text values
        df_extracted['Diarrhoea_Cases'] = pd.to_numeric(
            df_extracted['Diarrhoea_Cases'], 
            errors='coerce'
        )
        
        # Remove rows with invalid diarrhoea data
        df_extracted = df_extracted.dropna(subset=['Diarrhoea_Cases'])
        
        logger.info(f"  Extracted {len(df_extracted)} rows from {file_path.name}")
        
        return df_extracted
        
    except Exception as e:
        logger.error(f"Error processing file {file_path.name}: {e}", exc_info=True)
        return None


def process_all_health_files(data_dir: Path) -> pd.DataFrame:
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
        # Skip certain files if needed (e.g., All_India.xls might have different structure)
        if file_path.name.lower() in ['all_india.xls', 'all india.xls']:
            logger.info(f"Skipping {file_path.name} (aggregate file)")
            continue
        
        df = process_health_file(file_path)
        
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
    logger.info("Starting health data processing for 2019-2020")
    
    # Define data directory - user specified data/raw/health_2019_20/
    data_dir = Path("data/raw/health_2019_20")
    
    if not data_dir.exists():
        logger.error(f"Health data directory not found: {data_dir}")
        logger.info("Please ensure the .xls files are in data/raw/health_2019_20/")
        raise FileNotFoundError(f"Health data directory not found: {data_dir}")
    
    logger.info(f"Using data directory: {data_dir}")
    
    # Process all files
    combined_df = process_all_health_files(data_dir)
    
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
    print(f"\nColumns: {list(combined_df.columns)}")
    print(f"\nFirst few rows:")
    print(combined_df.head(10).to_string())
    
    print(f"\n✓ Health data processing complete!")
    print(f"  Output file: {output_file}")


if __name__ == "__main__":
    main()
