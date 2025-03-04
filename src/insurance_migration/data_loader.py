"""
Data loading functionality for insurance policy migration.
Handles CSV file loading and initial data transformation.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re

# Configure logger
logger = logging.getLogger('processing')

def parse_date(date_str: str) -> Optional[str]:
    """Parse date string with multiple formats."""
    if pd.isna(date_str) or not date_str:
        return None
    date_str = str(date_str).strip()
    formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%m/%d/%y', '%d-%b-%Y', '%d-%b-%y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    logger.debug(f"Failed to parse date: {date_str}")
    return None

def parse_currency(value: any) -> float:
    """Convert currency string to float with improved handling."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    value_str = str(value).strip()
    if not value_str:
        return 0.0
    value_str = re.sub(r'[^\d.-]', '', value_str)  # Remove all non-numeric except . and -
    try:
        return float(value_str)
    except ValueError:
        logger.debug(f"Failed to parse currency: {value_str}")
        return 0.0

def normalize_column_name(col: str) -> str:
    """Normalize column name to lowercase and remove special characters."""
    if not col:
        return ""
    col = str(col).lower().strip()
    col = re.sub(r'[^a-z0-9]', ' ', col)  # Replace non-alphanumeric with space
    col = re.sub(r'\s+', ' ', col).strip()  # Normalize spaces
    col = col.replace(' ', '_')  # Replace spaces with underscores
    return col

def load_csv_files(input_dir: Path, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Load and parse CSV files from input directory.
    
    Args:
        input_dir: Directory containing CSV files
        logger: Logger instance
    
    Returns:
        List of policy dictionaries
    """
    policies = []
    csv_files = list(input_dir.glob('*.csv'))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {input_dir}")
        return policies
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    for csv_file in csv_files:
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Normalize column names
            df.columns = [normalize_column_name(col) for col in df.columns]
            
            # Define required and optional fields with their variations
            required = {
                'policy_number': ['policy_number', 'policy_no', 'policy_id', 'policy'],
                'insured_name': ['insured_name', 'insured', 'customer_name', 'client_name', 'policyholder', 'client']
            }
            optional = {
                'effective_date': ['effective_date', 'start_date', 'policy_date', 'date'],
                'broker_fee': ['broker_fee', 'broker_fee_amount', 'brokerfee'],
                'commission': ['commission', 'commission_amount', 'comm'],
                'broker': ['agent', 'broker', 'agent_name', 'broker_name'],
                'policy_type': ['policy_type', 'type', 'policy_category', 'policy type'],
                'carrier': ['carrier', 'carrier_name', 'insurance_company'],
                'premium': ['charge_amount', 'premium', 'amount', 'policy_amount', 'total_premium', 'premium_amount']
            }
            
            # Create a mapping of normalized column names
            column_map = {}
            for target, variations in {**required, **optional}.items():
                for var in variations:
                    normalized = normalize_column_name(var)
                    if normalized in df.columns:
                        column_map[target] = normalized
                        break
            
            # Check for missing required fields
            missing_required = [field for field in required.keys() if field not in column_map]
            if missing_required:
                logger.error(f"Missing required columns in {csv_file.name}: {missing_required}")
                continue
            
            # Rename columns to standardized names
            df = df.rename(columns={v: k for k, v in column_map.items()})
            
            # Convert date columns
            date_columns = ['effective_date', 'expiration_date', 'transaction_date']
            for col in date_columns:
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        invalid_dates = df[col].isna().sum()
                        if invalid_dates > 0:
                            logger.warning(f"Skipped {invalid_dates} rows with invalid dates in {csv_file.name}")
                    except Exception as e:
                        logger.error(f"Error converting dates in column {col}: {e}")
            
            # Convert DataFrame to list of dictionaries
            file_policies = df.to_dict('records')
            
            # Clean up NaN values and ensure required fields are not empty
            for policy in file_policies:
                for key, value in policy.items():
                    if pd.isna(value):
                        policy[key] = None if key in ['premium', 'broker_fee', 'commission_amount'] else ''
                
                # Ensure required fields are not empty
                if not policy.get('insured_name'):
                    logger.warning(f"Empty insured_name for policy {policy.get('policy_number')} in {csv_file.name}")
                    continue
                
                policies.append(policy)
            
            logger.debug(f"Loaded {len(file_policies)} policies from {csv_file.name}")
            
        except Exception as e:
            logger.error(f"Error loading {csv_file.name}: {e}")
            continue
    
    return policies

def policies_to_dataframe(policies: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert list of policy dictionaries to DataFrame.
    
    Args:
        policies: List of policy dictionaries
    
    Returns:
        DataFrame containing policy data
    """
    if not policies:
        return pd.DataFrame()
    
    df = pd.DataFrame(policies)
    
    # Convert date columns to datetime
    date_columns = ['effective_date', 'expiration_date', 'transaction_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    return df 