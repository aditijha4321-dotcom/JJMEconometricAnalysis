"""
Data ingestion script for Jal Jeevan Mission (JJM) FHTC data
Fetches district-level Functional Household Tap Connection data from JJM IMIS API
Configured for financial year 2019-2020
"""

import requests
import pandas as pd
import json
import time
from typing import List, Dict, Optional
from pathlib import Path

import sys
from pathlib import Path as PathLib

# Add parent directory to path for imports
sys.path.insert(0, str(PathLib(__file__).parent.parent))

from config import API_ENDPOINTS, FILE_PATHS
from src.utils import setup_logger

# Setup logger
logger = setup_logger(__name__, log_type="ingestion")


def flatten_json(nested_json: Dict, parent_key: str = '', sep: str = '_') -> Dict:
    """
    Flatten a nested JSON dictionary into a flat dictionary.
    
    Args:
        nested_json (dict): Nested JSON dictionary to flatten
        parent_key (str): Parent key prefix for nested keys
        sep (str): Separator for nested keys (default: '_')
    
    Returns:
        dict: Flattened dictionary
    """
    items = []
    
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        
        if isinstance(value, dict):
            items.extend(flatten_json(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            # Handle lists - convert to string or process each item
            if len(value) > 0 and isinstance(value[0], dict):
                # If list contains dicts, create indexed keys
                for idx, item in enumerate(value):
                    if isinstance(item, dict):
                        items.extend(flatten_json(item, f"{new_key}_{idx}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{idx}", item))
            else:
                # Simple list - join as string or keep as is
                items.append((new_key, json.dumps(value) if value else None))
        else:
            items.append((new_key, value))
    
    return dict(items)


def fetch_district_fhtc_data(
    district_code: str,
    district_name: str = None,
    financial_year: str = "2019-2020",
    year_id: str = None,
    timeout: int = None,
    retry_attempts: int = None
) -> Optional[Dict]:
    """
    Fetch FHTC data for a specific district from JJM IMIS API.
    
    Args:
        district_code (str): District code/ID
        district_name (str, optional): District name for logging
        financial_year (str): Financial year in format 'YYYY-YYYY' (default: '2019-2020')
        year_id (str, optional): Year ID for historical data if API requires it
        timeout (int, optional): Request timeout in seconds
        retry_attempts (int, optional): Number of retry attempts
    
    Returns:
        dict: JSON response data or None if failed
    """
    jjm_config = API_ENDPOINTS["jjm_imis"]
    base_url = jjm_config["base_url"]
    timeout = timeout or jjm_config["timeout"]
    retry_attempts = retry_attempts or jjm_config["retry_attempts"]
    
    # Construct API endpoint - adjust based on actual API structure
    # Using household_coverage endpoint as it likely contains FHTC data
    endpoint = f"{base_url}{jjm_config['endpoints']['household_coverage']}"
    
    # API parameters - adjust based on actual API requirements
    params = {
        "district_code": district_code,
        "data_type": "fhtc",  # Functional Household Tap Connection
        "financial_year": financial_year
    }
    
    # Add year_id if provided (some APIs require this for historical data)
    if year_id:
        params["year_id"] = year_id
    # Alternative: try to derive year_id from financial_year if not provided
    elif financial_year == "2019-2020":
        # Common year_id formats: "2019", "FY2019", "2019-20", etc.
        # Try the most common format first
        params["year_id"] = "2019"
    
    district_label = district_name or district_code
    
    for attempt in range(1, retry_attempts + 1):
        try:
            logger.debug(f"Fetching FHTC data for district {district_label} (attempt {attempt}/{retry_attempts})")
            
            response = requests.get(
                endpoint,
                params=params,
                timeout=timeout,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            )
            
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched data for district: {district_label}")
            return data
            
        except requests.exceptions.Timeout:
            logger.warning(
                f"Timeout error for district {district_label} (attempt {attempt}/{retry_attempts}). "
                f"Retrying..."
            )
            if attempt < retry_attempts:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to fetch data for district {district_label} after {retry_attempts} attempts: Timeout")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Request error for district {district_label} (attempt {attempt}/{retry_attempts}): {str(e)}"
            )
            if attempt < retry_attempts:
                time.sleep(2 ** attempt)
            else:
                logger.error(f"Failed to fetch data for district {district_label} after {retry_attempts} attempts: {str(e)}")
                return None
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for district {district_label}: {str(e)}")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error for district {district_label}: {str(e)}", exc_info=True)
            return None
    
    return None


def process_district_data(district_data: Dict, district_code: str, district_name: str = None) -> Optional[pd.DataFrame]:
    """
    Process and flatten district JSON data into a Pandas DataFrame.
    
    Args:
        district_data (dict): Raw JSON data for a district
        district_code (str): District code
        district_name (str, optional): District name
    
    Returns:
        pd.DataFrame: Flattened DataFrame or None if processing fails
    """
    try:
        # Flatten the nested JSON
        flattened_data = flatten_json(district_data)
        
        # Add district identifiers if not present
        if 'district_code' not in flattened_data:
            flattened_data['district_code'] = district_code
        if district_name and 'district_name' not in flattened_data:
            flattened_data['district_name'] = district_name
        
        # Convert to DataFrame
        df = pd.DataFrame([flattened_data])
        
        logger.debug(f"Successfully processed data for district: {district_code}")
        return df
        
    except Exception as e:
        logger.error(f"Error processing data for district {district_code}: {str(e)}", exc_info=True)
        return None


def ingest_jjm_fhtc_data(
    districts: List[Dict[str, str]] = None,
    financial_year: str = "2019-2020",
    year_id: str = None,
    output_file: str = None
) -> pd.DataFrame:
    """
    Main function to ingest district-level FHTC data from JJM IMIS API.
    
    Args:
        districts (list, optional): List of dictionaries with 'code' and 'name' keys.
                                   If None, attempts to fetch all districts.
        financial_year (str): Financial year in format 'YYYY-YYYY' (default: '2019-2020')
        year_id (str, optional): Year ID for historical data if API requires it
        output_file (str, optional): Output file path. Defaults to data/raw/jjm_raw_2019.csv
    
    Returns:
        pd.DataFrame: Combined DataFrame with all successfully fetched district data
    """
    logger.info(f"Starting JJM FHTC data ingestion for financial year {financial_year}")
    
    # Set default output file
    if output_file is None:
        output_file = Path(FILE_PATHS["data"]["raw"]) / "jjm_raw_2019.csv"
    else:
        output_file = Path(output_file)
    
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Default districts list if not provided
    # In production, this could be fetched from an API endpoint
    if districts is None:
        logger.warning("No districts provided. Using sample district codes.")
        logger.info("Please provide a list of districts with 'code' and 'name' keys")
        districts = []
    
    all_dataframes = []
    successful_districts = 0
    failed_districts = 0
    
    logger.info(f"Processing {len(districts)} districts")
    
    for district in districts:
        district_code = district.get('code', district.get('district_code', ''))
        district_name = district.get('name', district.get('district_name', None))
        
        if not district_code:
            logger.warning(f"Skipping district entry with missing code: {district}")
            failed_districts += 1
            continue
        
        # Fetch data for district
        district_data = fetch_district_fhtc_data(
            district_code=district_code,
            district_name=district_name,
            financial_year=financial_year,
            year_id=year_id
        )
        
        if district_data is None:
            failed_districts += 1
            logger.warning(f"Skipping district {district_code} due to fetch failure")
            continue
        
        # Process and flatten the data
        df = process_district_data(district_data, district_code, district_name)
        
        if df is not None and not df.empty:
            all_dataframes.append(df)
            successful_districts += 1
        else:
            failed_districts += 1
            logger.warning(f"No data processed for district {district_code}")
        
        # Small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Combine all DataFrames
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Combined data from {successful_districts} districts into DataFrame with shape {combined_df.shape}")
    else:
        logger.warning("No data was successfully fetched. Creating empty DataFrame.")
        combined_df = pd.DataFrame()
    
    # Save to CSV
    try:
        combined_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Saved raw data to {output_file}")
        logger.info(f"Summary: {successful_districts} successful, {failed_districts} failed")
    except Exception as e:
        logger.error(f"Error saving data to {output_file}: {str(e)}", exc_info=True)
    
    return combined_df


if __name__ == "__main__":
    # Example usage
    # You can provide a list of districts or fetch from an API
    sample_districts = [
        {"code": "D001", "name": "Sample District 1"},
        {"code": "D002", "name": "Sample District 2"},
    ]
    
    logger.info("Running JJM FHTC data ingestion script for 2019-2020")
    
    # Ingest data for financial year 2019-2020
    # Uncomment and modify the districts list with actual district codes
    # df = ingest_jjm_fhtc_data(
    #     districts=sample_districts,
    #     financial_year="2019-2020",
    #     year_id="2019"  # Adjust based on actual API requirements
    # )
    
    logger.info("Script execution completed. Please provide actual district codes to fetch real data.")

