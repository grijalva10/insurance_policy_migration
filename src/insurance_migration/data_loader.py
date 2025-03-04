"""
Data loading functionality for insurance policy migration.
Handles CSV file loading and initial data transformation.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional
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

def load_csv_files(input_dir: Path) -> List[Dict]:
    """Load CSV files into policy dictionaries with enhanced field mapping."""
    if not input_dir.exists():
        logger.error(f"Input directory {input_dir} does not exist")
        return []
    
    csv_files = list(input_dir.glob('*.csv'))
    if not csv_files:
        logger.error(f"No CSV files found in {input_dir}")
        return []
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Define field mappings
    field_mappings = {
        'client': 'insured_name',
        'email': 'insured_email',
        'transaction_id': 'transaction_id',
        'charge_amount': 'premium',
        'date': 'effective_date',
        'batch_id': 'batch_id',
        'carrier': 'carrier',
        'policy_type': 'policy_type',
        'paid_in_full': 'paid_in_full',
        'broker_fee': 'broker_fee',
        'commission': 'commission_amount',
        'pay_ins_company': 'pay_ins_company',
        'card_fee': 'card_fee',
        'refund': 'refund',
        'difference': 'difference',
        'agent': 'broker',
        'policy_number': 'policy_number',
        'month': 'month',
        'cancellation_date': 'cancellation_date',
        'returned': 'returned_amount',
        'company': 'company',
        'uec_bop': 'uec_bop'
    }
    
    all_policies = []
    
    for csv_file in csv_files:
        logger.debug(f"Processing file: {csv_file}")
        
        try:
            # Read CSV file with appropriate data types
            # Force string type for fields that should be strings
            dtype = {
                'email': str,
                'insured_email': str,
                'policy_number': str,
                'carrier': str,
                'policy_type': str,
                'broker': str,
                'agent': str,
                'client': str,
                'insured_name': str
            }
            
            # Read CSV file
            df = pd.read_csv(csv_file, encoding='utf-8', dtype=dtype, na_values=['nan', 'NaN', 'NA', ''], keep_default_na=False)
            
            # Normalize column names
            df.columns = [normalize_column_name(col) for col in df.columns]
            
            # Map column names to standardized field names
            column_mapping = {}
            for col in df.columns:
                if col in field_mappings:
                    column_mapping[col] = field_mappings[col]
                else:
                    # Try to find a match in field_mappings keys
                    for key in field_mappings:
                        if key in col:
                            column_mapping[col] = field_mappings[key]
                            break
            
            logger.debug(f"Column mapping for {csv_file.name}: {column_mapping}")
            
            # Rename columns
            df = df.rename(columns=column_mapping)
            
            # Ensure email fields are strings
            for email_field in ['insured_email', 'email']:
                if email_field in df.columns:
                    df[email_field] = df[email_field].astype(str)
                    # Replace 'nan' strings with empty strings
                    df[email_field] = df[email_field].replace('nan', '')
            
            # Handle dates
            date_columns = ['effective_date', 'expiration_date', 'cancellation_date']
            for col in date_columns:
                if col in df.columns:
                    df[col] = df[col].apply(parse_date)
            
            # Calculate expiration date if not present
            if 'effective_date' in df.columns and 'expiration_date' not in df.columns:
                # Default to 1 year policy term
                df['expiration_date'] = df['effective_date'].apply(
                    lambda x: (datetime.strptime(x, '%Y-%m-%d') + relativedelta(years=1)).strftime('%Y-%m-%d') if x else None
                )
            
            # Handle currency fields
            currency_columns = ['premium', 'broker_fee', 'commission_amount', 'card_fee', 'returned_amount']
            for col in currency_columns:
                if col in df.columns:
                    df[col] = df[col].apply(parse_currency)
            
            # Filter out rows with invalid dates
            invalid_dates = df['effective_date'].isna()
            if invalid_dates.any():
                logger.warning(f"Skipped {invalid_dates.sum()} rows with invalid dates in {csv_file.name}")
                df = df[~invalid_dates]
            
            # Convert to list of dictionaries
            policies = df.to_dict('records')
            logger.debug(f"Extracted {len(policies)} policies from {csv_file.name}")
            
            all_policies.extend(policies)
            
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")
    
    logger.info(f"Total policies loaded: {len(all_policies)}")
    return all_policies

# Add a static method to convert policies to DataFrame
load_csv_files.policies_to_dataframe = lambda policies: pd.DataFrame(policies) 