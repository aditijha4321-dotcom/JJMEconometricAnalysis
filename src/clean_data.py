"""
Data cleaning script for Jal Jeevan Mission (JJM) FHTC data
Implements bias detection logic to identify suspicious data patterns
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Tuple
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import FILE_PATHS
from src.utils import setup_logger

# Setup logger
logger = setup_logger(__name__, log_type="processing")


def identify_coverage_column(df: pd.DataFrame) -> Optional[str]:
    """
    Identify the FHTC coverage column from the dataframe.
    Looks for common column names related to coverage.
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        str: Name of the coverage column, or None if not found
    """
    # Common column names for FHTC coverage
    coverage_keywords = [
        'coverage', 'fhtc', 'fhtc_coverage', 'coverage_percent', 
        'coverage_pct', 'coverage_percentage', 'percent_coverage',
        'household_coverage', 'tap_coverage', 'connection_coverage'
    ]
    
    df_columns_lower = [col.lower() for col in df.columns]
    
    for keyword in coverage_keywords:
        for idx, col_lower in enumerate(df_columns_lower):
            if keyword in col_lower:
                logger.info(f"Identified coverage column: {df.columns[idx]}")
                return df.columns[idx]
    
    logger.warning("Could not automatically identify coverage column")
    return None


def identify_date_column(df: pd.DataFrame) -> Optional[str]:
    """
    Identify the date/period column from the dataframe.
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        str: Name of the date column, or None if not found
    """
    date_keywords = [
        'date', 'period', 'month', 'year', 'time', 'timestamp',
        'reporting_date', 'reporting_period', 'month_year'
    ]
    
    df_columns_lower = [col.lower() for col in df.columns]
    
    for keyword in date_keywords:
        for idx, col_lower in enumerate(df_columns_lower):
            if keyword in col_lower:
                logger.info(f"Identified date column: {df.columns[idx]}")
                return df.columns[idx]
    
    logger.warning("Could not automatically identify date column")
    return None


def identify_district_column(df: pd.DataFrame) -> Optional[str]:
    """
    Identify the district identifier column from the dataframe.
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        str: Name of the district column, or None if not found
    """
    district_keywords = [
        'district', 'district_code', 'district_name', 'dist_code', 'dist_name'
    ]
    
    df_columns_lower = [col.lower() for col in df.columns]
    
    for keyword in district_keywords:
        for idx, col_lower in enumerate(df_columns_lower):
            if keyword in col_lower:
                logger.info(f"Identified district column: {df.columns[idx]}")
                return df.columns[idx]
    
    logger.warning("Could not automatically identify district column")
    return None


def calculate_month_on_month_change(
    df: pd.DataFrame,
    coverage_col: str,
    date_col: str,
    district_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Calculate month-on-month change in FHTC coverage.
    
    Args:
        df (pd.DataFrame): Input dataframe
        coverage_col (str): Name of the coverage column
        date_col (str): Name of the date column
        district_col (str, optional): Name of the district column for grouping
    
    Returns:
        pd.DataFrame: Dataframe with month-on-month change column added
    """
    df = df.copy()
    
    # Convert date column to datetime if not already
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        except Exception as e:
            logger.warning(f"Could not convert {date_col} to datetime: {e}")
    
    # Sort by date (and district if provided)
    sort_cols = [district_col, date_col] if district_col else [date_col]
    df = df.sort_values(by=sort_cols).reset_index(drop=True)
    
    # Calculate month-on-month change
    if district_col:
        # Group by district and calculate change within each district
        df['coverage_previous_month'] = df.groupby(district_col)[coverage_col].shift(1)
    else:
        # Calculate change across all data
        df['coverage_previous_month'] = df[coverage_col].shift(1)
    
    # Calculate absolute change
    df['coverage_change_absolute'] = df[coverage_col] - df['coverage_previous_month']
    
    # Calculate percentage change
    df['coverage_change_percent'] = (
        (df[coverage_col] - df['coverage_previous_month']) / 
        df['coverage_previous_month'].replace(0, np.nan) * 100
    )
    
    # Fill NaN values (first month for each district or when previous month is 0)
    df['coverage_change_percent'] = df['coverage_change_percent'].fillna(0)
    
    logger.info("Calculated month-on-month coverage changes")
    
    return df


def detect_bias(df: pd.DataFrame, coverage_col: str) -> pd.DataFrame:
    """
    Detect bias in the data by flagging suspicious patterns.
    
    Args:
        df (pd.DataFrame): Input dataframe with coverage data
        coverage_col (str): Name of the coverage column
    
    Returns:
        pd.DataFrame: Dataframe with bias detection flags added
    """
    df = df.copy()
    
    # Flag 1: Suspicious spike (>15% increase in a single month)
    # This is an improbable engineering feat
    df['suspicious_spike'] = df['coverage_change_percent'] > 15.0
    
    # Flag 2: Reporting error (coverage > 100% or < 0%)
    df['reporting_error'] = (df[coverage_col] > 100.0) | (df[coverage_col] < 0.0)
    
    logger.info(f"Bias detection completed:")
    logger.info(f"  - Suspicious spikes detected: {df['suspicious_spike'].sum()}")
    logger.info(f"  - Reporting errors detected: {df['reporting_error'].sum()}")
    
    return df


def clean_jjm_data(
    input_file: str = None,
    output_file: str = None,
    coverage_col: str = None,
    date_col: str = None,
    district_col: str = None
) -> Tuple[pd.DataFrame, dict]:
    """
    Main function to clean JJM FHTC data with bias detection.
    
    Args:
        input_file (str, optional): Input CSV file path. Defaults to data/raw/jjm_raw.csv
        output_file (str, optional): Output CSV file path. Defaults to data/processed/jjm_cleaned.csv
        coverage_col (str, optional): Name of coverage column. Auto-detected if None
        date_col (str, optional): Name of date column. Auto-detected if None
        district_col (str, optional): Name of district column. Auto-detected if None
    
    Returns:
        tuple: (cleaned_dataframe, summary_report_dict)
    """
    logger.info("Starting JJM data cleaning process")
    
    # Set default file paths
    if input_file is None:
        input_file = Path(FILE_PATHS["data"]["raw"]) / "jjm_raw.csv"
    else:
        input_file = Path(input_file)
    
    if output_file is None:
        output_file = Path(FILE_PATHS["data"]["processed"]) / "jjm_cleaned.csv"
    else:
        output_file = Path(output_file)
    
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Load raw data
    try:
        logger.info(f"Loading data from {input_file}")
        df = pd.read_csv(input_file)
        logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file}")
        raise
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
        raise
    
    # Auto-detect columns if not provided
    if coverage_col is None:
        coverage_col = identify_coverage_column(df)
        if coverage_col is None:
            raise ValueError("Could not identify coverage column. Please specify coverage_col parameter.")
    
    if date_col is None:
        date_col = identify_date_column(df)
        if date_col is None:
            raise ValueError("Could not identify date column. Please specify date_col parameter.")
    
    if district_col is None:
        district_col = identify_district_column(df)
        # District column is optional, so we don't raise an error if not found
    
    # Validate that required columns exist
    if coverage_col not in df.columns:
        raise ValueError(f"Coverage column '{coverage_col}' not found in dataframe")
    if date_col not in df.columns:
        raise ValueError(f"Date column '{date_col}' not found in dataframe")
    
    # Convert coverage to numeric, handling any non-numeric values
    df[coverage_col] = pd.to_numeric(df[coverage_col], errors='coerce')
    
    # Calculate month-on-month change
    df = calculate_month_on_month_change(
        df, 
        coverage_col=coverage_col,
        date_col=date_col,
        district_col=district_col
    )
    
    # Detect bias
    df = detect_bias(df, coverage_col=coverage_col)
    
    # Store original row count
    original_rows = len(df)
    
    # Filter out rows with reporting errors
    df_cleaned = df[~df['reporting_error']].copy()
    filtered_rows = original_rows - len(df_cleaned)
    
    logger.info(f"Filtered out {filtered_rows} rows with reporting errors")
    logger.info(f"Remaining rows: {len(df_cleaned)}")
    
    # Generate summary report
    summary = {
        'original_rows': original_rows,
        'filtered_rows': filtered_rows,
        'cleaned_rows': len(df_cleaned),
        'suspicious_spikes_total': df['suspicious_spike'].sum(),
        'reporting_errors_total': df['reporting_error'].sum(),
        'districts_with_suspicious_spikes': 0,
        'districts_flagged': []
    }
    
    # Count districts with suspicious spikes
    if district_col and district_col in df.columns:
        districts_with_spikes = df[df['suspicious_spike']][district_col].unique()
        summary['districts_with_suspicious_spikes'] = len(districts_with_spikes)
        summary['districts_flagged'] = districts_with_spikes.tolist()
    else:
        # If no district column, count unique identifiers or use row count
        summary['districts_with_suspicious_spikes'] = df['suspicious_spike'].sum()
    
    # Save cleaned data
    try:
        df_cleaned.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Saved cleaned data to {output_file}")
    except Exception as e:
        logger.error(f"Error saving cleaned data: {e}", exc_info=True)
        raise
    
    return df_cleaned, summary


def print_summary_report(summary: dict):
    """
    Print a formatted summary report of the data cleaning process.
    
    Args:
        summary (dict): Summary dictionary from clean_jjm_data
    """
    print("\n" + "="*60)
    print("JJM DATA CLEANING SUMMARY REPORT")
    print("="*60)
    print(f"\nOriginal Data Rows:        {summary['original_rows']:,}")
    print(f"Rows with Reporting Errors: {summary['reporting_errors_total']:,}")
    print(f"Filtered Rows Removed:      {summary['filtered_rows']:,}")
    print(f"Cleaned Data Rows:          {summary['cleaned_rows']:,}")
    print(f"\nSuspicious Spikes Detected: {summary['suspicious_spikes_total']:,}")
    print(f"Districts Flagged for Suspicious Spikes: {summary['districts_with_suspicious_spikes']:,}")
    
    if summary['districts_flagged']:
        print(f"\nDistricts with Suspicious Spikes:")
        for district in summary['districts_flagged'][:10]:  # Show first 10
            print(f"  - {district}")
        if len(summary['districts_flagged']) > 10:
            print(f"  ... and {len(summary['districts_flagged']) - 10} more")
    
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    logger.info("Running JJM data cleaning script")
    
    try:
        # Clean the data
        df_cleaned, summary = clean_jjm_data()
        
        # Print summary report
        print_summary_report(summary)
        
        logger.info("Data cleaning completed successfully")
        
    except Exception as e:
        logger.error(f"Data cleaning failed: {e}", exc_info=True)
        raise

