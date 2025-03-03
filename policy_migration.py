#!/usr/bin/env python3
"""
Insurance Policy Migration Script

This script migrates insurance policy data from CSV files to an AMS via a Frappe API.
Part 1: Setup and CSV loading
Part 2: AMS lookups for insureds and carriers
Part 3: Static broker mapping
Part 4: Policy processing (premium calculation, status assignment, deduplication)
Part 5: Save processed policies to CSV files for reporting
"""

import os
import sys
import json
import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import re
import time
import base64  # For GitHub API authentication

# Constants
AMS_API_URL = "https://ams.jmggo.com/api/method"
AMS_API_TOKEN = None  # Will be set later
AMS_API_HEADERS = None  # Will be set later

# GitHub API settings
GITHUB_API_URL = "https://api.github.com"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  # Get token from environment variable
GITHUB_USERNAME = "grijalva10"  # Your GitHub username

# Directory structure
INPUT_DIR = "./data/input/"
OUTPUT_DIR = "./data/reports/"
CACHE_DIR = "./data/cache/"
LOG_FILE = "policy_upload_log.txt"

# Cache files
INSUREDS_CACHE_FILE = os.path.join(CACHE_DIR, "ams_insureds.csv")
CARRIERS_CACHE_FILE = os.path.join(CACHE_DIR, "ams_carriers.csv")

# Broker to email mapping
BROKER_TO_EMAIL = {
    "JMG/Randall": "randall@jmgia.com", "Grey": "grey@jmgia.com", "Grey Zwilling": "grey@jmgia.com",
    "Chelsea": "chelsea@jmgia.com", "Mike": "mike@jmgia.com", "Mke": "mike@jmgia.com", "MIke": "mike@jmgia.com",
    "Justin": "justin@jmgia.com", "Mark": "mark@jmgia.com", "Mark Gomez": "mark@jmgia.com", "Jon": "randall@jmgia.com",
    "Chris": "randall@jmgia.com", "JMG/Ramdall": "randall@jmgia.com", "JMG Randall": "randall@jmgia.com",
    "JMG /Randall": "randall@jmgia.com", "JMGRandall": "randall@jmgia.com", "JMG/ Randall": "randall@jmgia.com",
    "JGM/Randall": "randall@jmgia.com", "JMG/Randalll": "randall@jmgia.com", "JMG/Radall ": "randall@jmgia.com",
    "JMGI/Randall": "randall@jmgia.com", "JMG/Radall": "randall@jmgia.com", "Adrian": "adrian@jmgia.com",
    "Eduardo": "eduardo@jmgia.com", "Eduardo ": "eduardo@jmgia.com", "Eduardo/Mike": "eduardo@jmgia.com",
    "Eduardo/Chelsea": "eduardo@jmgia.com", "JMG/Eduado": "eduardo@jmgia.com", "Jeff": "jeff@jmgia.com",
    "Chalsea": "chelsea@jmgia.com", "Ted": "ted@jmgia.com", "Brennan": "brennan@jmgia.com",
    "JMG/Eduardo": "eduardo@jmgia.com", "JMG/Justin": "justin@jmgia.com", "JMG/Adrian": "adrian@jmgia.com",
    "JMG/ Adrian": "adrian@jmgia.com", "Chris H": "chrish@jmgia.com", "Sean": "sean@jmgia.com",
    "Collin": "collin@jmgia.com", "Collin ": "collin@jmgia.com", "Colin ": "collin@jmgia.com",
    "Colin": "collin@jmgia.com", "Collin Daly": "collin@jmgia.com", "Brian": "bryan@jmgia.com",
    "Brian ": "bryan@jmgia.com", "Bryan": "bryan@jmgia.com", "Bryan ": "bryan@jmgia.com",
    "Bryan Otten": "bryan@jmgia.com", "Anthony": "anthony@jmgia.com", "Dale": "dale@jmgia.com",
    "Brennan Clinebell": "bryan@jmgia.com", "Justin Angevine": "justin@jmgia.com", "Addison": "addison@jmgia.com",
    "Alexis": "alexis@jmgia.com", "Gerardo ": "gerardo@jmgia.com", "Gerardo": "gerardo@jmgia.com",
    "Gerardo Perales": "gerardo@jmgia.com", "Clint": "clint@jmgia.com", "Cara": "cara@jmgia.com",
    "Randall": "randall@jmgia.com", "Randall/Sean": "randall@jmgia.com", "Adrian/Eduardo": "adrian@jmgia.com"
}

# Add broker name standardization mapping
BROKER_NAME_STANDARDIZATION = {
    "JMG/Ranall": "JMG/Randall",
    "JMG/Randal": "JMG/Randall",
    "JMH/Randall": "JMG/Randall",
    "JMH/Randall ": "JMG/Randall",
    "Julie": "Julie Smith",
    "nan": None
}

# Update broker email mapping
BROKER_EMAIL_MAPPING = {
    "JMG/Randall": "randall@jmgia.com",
    "Julie Smith": "julie@jmgia.com",
    "Clint": "clint@jmgia.com",
    "Mark": "mark@jmgia.com",
    "Mike": "mike@jmgia.com",
    "Bryan": "bryan@jmgia.com",
    "Chelsea": "chelsea@jmgia.com",
    "Cara": "cara@jmgia.com",
    "Eduardo": "eduardo@jmgia.com",
    "Grey": "grey@jmgia.com",
    "Collin": "collin@jmgia.com",
    "Jon": "randall@jmgia.com",
    "Chris": "randall@jmgia.com",
    "Adrian": "adrian@jmgia.com"
}

# Policy types
POLICY_TYPES = [
    "Bond", "Builders Risk", "Commercial Auto", "Commercial Property", 
    "Excess", "General Liability", "Inland Marine", "Pollution Liability", 
    "Professional Liability", "Workers Compensation", "Endorsement", 
    "General Liability + Excess", "General Liability + Inland Marine", 
    "General Liability + Builders Risk", "Other"
]

# Setup logging
def setup_logging():
    """Configure logging to file and console with debug level"""
    # Create directories if they don't exist
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    # Configure logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # File handler
    file_handler = logging.FileHandler(LOG_FILE, mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_format = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_format)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Normalize column names
def normalize_column_name(column_name):
    """Normalize column names to be case-insensitive and strip spaces"""
    return column_name.lower().strip()

# Parse arguments
def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Insurance Policy Migration Script")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode (no API calls)")
    parser.add_argument("--no-cache", action="store_true", help="Don't use cached data")
    parser.add_argument("--github-token", help="GitHub API token")
    parser.add_argument("--ams-token", help="AMS API token for AMS API calls")
    parser.add_argument("--skip-ams-fetch", action="store_true", help="Skip fetching policies from AMS")
    return parser.parse_args()

# Parse date with multiple formats
def parse_date(date_str):
    """Parse date string with multiple possible formats"""
    if pd.isna(date_str) or not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Try different date formats
    formats = [
        '%Y-%m-%d',           # 2024-01-02
        '%Y-%m-%d %H:%M:%S',  # 2024-01-02 22:00:46
        '%m/%d/%Y',           # 01/02/2024
        '%m/%d/%y',           # 01/02/24
        '%d-%b-%Y',           # 02-Jan-2024
        '%d-%b-%y'            # 02-Jan-24
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If all formats fail, log and return None
    return None

# Parse currency value
def parse_currency(value):
    """Convert currency string to float"""
    if pd.isna(value) or not value:
        return 0.0
    
    if isinstance(value, (int, float)):
        return float(value)
    
    # Remove currency symbols and commas
    value_str = str(value).strip()
    value_str = re.sub(r'[$,]', '', value_str)
    
    try:
        return float(value_str)
    except ValueError:
        return 0.0

# Load CSV files
def load_csv_files(logger):
    """
    Load CSV files from input directory and return a list of policy dictionaries
    
    Maps:
    - 'Policy Number' to 'policy_number' (required)
    - 'Date' to 'effective_date' (add 'expiration_date' as +1 year)
    - 'Broker Fee' to 'broker_fee'
    - 'Commission' to 'commission'
    - 'Agent' to 'broker'
    - 'Policy Type' to 'policy_type'
    - 'Carrier' to 'carrier'
    """
    policies = []
    csv_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
    
    if not csv_files:
        logger.warning(f"No CSV files found in {INPUT_DIR}")
        return policies
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    for file_name in csv_files:
        file_path = os.path.join(INPUT_DIR, file_name)
        logger.debug(f"Processing file: {file_path}")
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Create a mapping of normalized column names to original column names
            column_mapping = {}
            for col in df.columns:
                normalized_col = normalize_column_name(col)
                column_mapping[normalized_col] = col
            
            logger.debug(f"Column mapping for {file_name}: {column_mapping}")
            
            # Check for required columns
            required_columns = {
                'policy number': 'policy_number',
            }
            
            optional_columns = {
                'date': 'effective_date',
                'broker fee': 'broker_fee',
                'commission': 'commission',
                'agent': 'broker',
                'policy type': 'policy_type',
                'carrier': 'carrier',
            }
            
            # Verify required columns exist
            missing_columns = []
            for req_col, _ in required_columns.items():
                if req_col not in column_mapping:
                    missing_columns.append(req_col)
            
            if missing_columns:
                logger.error(f"Missing required columns in {file_name}: {missing_columns}")
                continue
            
            # Create a new DataFrame with the mapped columns
            mapped_df = pd.DataFrame()
            
            # Map required columns
            for source_col, target_col in required_columns.items():
                if source_col in column_mapping:
                    mapped_df[target_col] = df[column_mapping[source_col]]
            
            # Map optional columns
            for source_col, target_col in optional_columns.items():
                if source_col in column_mapping:
                    mapped_df[target_col] = df[column_mapping[source_col]]
            
            # Process dates
            if 'effective_date' in mapped_df.columns:
                # Parse dates with multiple formats
                mapped_df['effective_date'] = mapped_df['effective_date'].apply(parse_date)
                
                # Filter out rows with invalid dates
                valid_date_mask = mapped_df['effective_date'].notna()
                if not valid_date_mask.all():
                    invalid_count = (~valid_date_mask).sum()
                    logger.warning(f"Skipped {invalid_count} rows with invalid dates in {file_name}")
                    mapped_df = mapped_df[valid_date_mask]
                
                # Calculate expiration date (1 year after effective date)
                mapped_df['expiration_date'] = mapped_df['effective_date'].apply(
                    lambda x: (datetime.strptime(x, '%Y-%m-%d') + relativedelta(years=1)).strftime('%Y-%m-%d') if x else None
                )
            
            # Process currency fields
            if 'broker_fee' in mapped_df.columns:
                mapped_df['broker_fee_amount'] = mapped_df['broker_fee'].apply(parse_currency)
            
            if 'commission' in mapped_df.columns:
                mapped_df['commission_amount'] = mapped_df['commission'].apply(parse_currency)
            
            # Convert DataFrame to list of dictionaries
            file_policies = mapped_df.to_dict('records')
            logger.debug(f"Extracted {len(file_policies)} policies from {file_name}")
            
            # Add source file information to each policy
            for policy in file_policies:
                policy['source_file'] = file_name
            
            policies.extend(file_policies)
            
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {str(e)}")
    
    logger.info(f"Total policies loaded: {len(policies)}")
    return policies

# Fetch insureds from AMS API
def fetch_ams_insureds(logger, use_cache=True):
    """
    Fetch insureds from AMS API with pagination
    
    Args:
        logger: Logger instance
        use_cache: Whether to use cached data if available
    
    Returns:
        Dictionary mapping insured_name/email to name (ID)
    """
    insureds_map = {}
    
    # Check if cache file exists and use it if requested
    if use_cache and os.path.exists(INSUREDS_CACHE_FILE):
        try:
            logger.debug(f"Loading insureds from cache: {INSUREDS_CACHE_FILE}")
            df = pd.read_csv(INSUREDS_CACHE_FILE)
            
            # Create lookup dictionary from name and email to ID
            for _, row in df.iterrows():
                if not pd.isna(row['insured_name']):
                    insureds_map[row['insured_name'].lower()] = row['name']
                if not pd.isna(row['email']) and row['email']:
                    insureds_map[row['email'].lower()] = row['name']
            
            logger.info(f"Loaded {len(df)} insureds from cache")
            return insureds_map
        except Exception as e:
            logger.error(f"Error loading insureds from cache: {str(e)}")
            # Continue to fetch from API if cache loading fails
    
    # Fetch from API
    try:
        all_insureds = []
        page = 0
        page_size = 1000
        more_data = True
        
        while more_data:
            page += 1
            logger.debug(f"Fetching AMS insureds, page {page}...")
            
            # Prepare API request
            payload = {
                "doctype": "Insured",
                "fields": ["name", "insured_name", "email"],
                "limit_start": (page - 1) * page_size,
                "limit_page_length": page_size
            }
            
            # Make API request with retry logic
            max_retries = 3
            retry_delay = 2  # seconds
            
            for attempt in range(max_retries):
                try:
                    response = requests.post(
                        f"{AMS_API_URL}/frappe.client.get_list",
                        headers=AMS_API_HEADERS,
                        json=payload,
                        timeout=30  # 30 seconds timeout
                    )
                    
                    response.raise_for_status()  # Raise exception for HTTP errors
                    data = response.json()
                    
                    if "message" in data and isinstance(data["message"], list):
                        insureds = data["message"]
                        all_insureds.extend(insureds)
                        logger.debug(f"Retrieved {len(insureds)} insureds on page {page}")
                        
                        # Check if we've reached the end
                        if len(insureds) < page_size:
                            more_data = False
                            break
                    else:
                        logger.warning(f"Unexpected API response format: {data}")
                        more_data = False
                    
                    # Successful request, break retry loop
                    break
                    
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"API request failed (attempt {attempt+1}/{max_retries}): {str(e)}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        logger.error(f"Failed to fetch insureds: {str(e)}")
                        # Try to use cache as fallback if it exists
                        if os.path.exists(INSUREDS_CACHE_FILE):
                            logger.info("Falling back to cached insureds data")
                            return fetch_ams_insureds(logger, use_cache=True)
                        more_data = False
        
        # Save to cache
        if all_insureds:
            try:
                df = pd.DataFrame(all_insureds)
                os.makedirs(os.path.dirname(INSUREDS_CACHE_FILE), exist_ok=True)
                df.to_csv(INSUREDS_CACHE_FILE, index=False)
                logger.debug(f"Saved {len(df)} insureds to cache")
            except Exception as e:
                logger.error(f"Error saving insureds to cache: {str(e)}")
        
        # Create lookup dictionary
        for insured in all_insureds:
            if "insured_name" in insured and insured["insured_name"]:
                insureds_map[insured["insured_name"].lower()] = insured["name"]
            if "email" in insured and insured["email"]:
                insureds_map[insured["email"].lower()] = insured["name"]
        
        logger.info(f"Fetched {len(all_insureds)} insureds from AMS")
        return insureds_map
        
    except Exception as e:
        logger.error(f"Error fetching insureds: {str(e)}")
        # Try to use cache as fallback if it exists
        if os.path.exists(INSUREDS_CACHE_FILE):
            logger.info("Falling back to cached insureds data due to error")
            return fetch_ams_insureds(logger, use_cache=True)
        return {}

# Fetch carriers from AMS API
def fetch_ams_carriers(logger, use_cache=True):
    """
    Fetch carriers from AMS API with pagination
    
    Args:
        logger: Logger instance
        use_cache: Whether to use cached data if available
    
    Returns:
        Dictionary mapping carrier_name to {name, commission}
    """
    carriers_map = {}
    
    # Check if cache file exists and use it if requested
    if use_cache and os.path.exists(CARRIERS_CACHE_FILE):
        try:
            logger.debug(f"Loading carriers from cache: {CARRIERS_CACHE_FILE}")
            df = pd.read_csv(CARRIERS_CACHE_FILE)
            
            # Create lookup dictionary from carrier_name to {name, commission}
            for _, row in df.iterrows():
                if not pd.isna(row['carrier_name']):
                    carriers_map[row['carrier_name'].lower()] = {
                        'name': row['name'],
                        'commission': row['commission'] if 'commission' in df.columns and not pd.isna(row['commission']) else 0.0
                    }
            
            logger.info(f"Loaded {len(df)} carriers from cache")
            return carriers_map
        except Exception as e:
            logger.error(f"Error loading carriers from cache: {str(e)}")
            # Continue to fetch from API if cache loading fails
    
    # Fetch from API
    try:
        all_carriers = []
        page = 0
        page_size = 500
        more_data = True
        
        while more_data:
            page += 1
            logger.debug(f"Fetching AMS carriers, page {page}...")
            
            # Prepare API request
            payload = {
                "doctype": "Carrier",
                "fields": ["name", "carrier_name", "commission"],
                "limit_start": (page - 1) * page_size,
                "limit_page_length": page_size
            }
            
            # Make API request with retry logic
            max_retries = 3
            retry_delay = 2  # seconds
            
            for attempt in range(max_retries):
                try:
                    response = requests.post(
                        f"{AMS_API_URL}/frappe.client.get_list",
                        headers=AMS_API_HEADERS,
                        json=payload,
                        timeout=30  # 30 seconds timeout
                    )
                    
                    response.raise_for_status()  # Raise exception for HTTP errors
                    data = response.json()
                    
                    if "message" in data and isinstance(data["message"], list):
                        carriers = data["message"]
                        all_carriers.extend(carriers)
                        logger.debug(f"Retrieved {len(carriers)} carriers on page {page}")
                        
                        # Check if we've reached the end
                        if len(carriers) < page_size:
                            more_data = False
                            break
                    else:
                        logger.warning(f"Unexpected API response format: {data}")
                        more_data = False
                    
                    # Successful request, break retry loop
                    break
                    
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"API request failed (attempt {attempt+1}/{max_retries}): {str(e)}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        logger.error(f"Failed to fetch carriers: {str(e)}")
                        # Try to use cache as fallback if it exists
                        if os.path.exists(CARRIERS_CACHE_FILE):
                            logger.info("Falling back to cached carriers data")
                            return fetch_ams_carriers(logger, use_cache=True)
                        more_data = False
        
        # Save to cache
        if all_carriers:
            try:
                df = pd.DataFrame(all_carriers)
                os.makedirs(os.path.dirname(CARRIERS_CACHE_FILE), exist_ok=True)
                df.to_csv(CARRIERS_CACHE_FILE, index=False)
                logger.debug(f"Saved {len(df)} carriers to cache")
            except Exception as e:
                logger.error(f"Error saving carriers to cache: {str(e)}")
        
        # Create lookup dictionary
        for carrier in all_carriers:
            if "carrier_name" in carrier and carrier["carrier_name"]:
                carriers_map[carrier["carrier_name"].lower()] = {
                    'name': carrier["name"],
                    'commission': carrier.get("commission", 0.0)
                }
        
        logger.info(f"Fetched {len(all_carriers)} carriers from AMS")
        return carriers_map
        
    except Exception as e:
        logger.error(f"Error fetching carriers: {str(e)}")
        # Try to use cache as fallback if it exists
        if os.path.exists(CARRIERS_CACHE_FILE):
            logger.info("Falling back to cached carriers data due to error")
            return fetch_ams_carriers(logger, use_cache=True)
        return {}

def fetch_close_brokers(logger):
    """
    Returns a case-insensitive copy of the static broker to email mapping
    
    Args:
        logger: Logger instance
    
    Returns:
        Dictionary mapping broker_name (lowercase) to email
    """
    # Create a case-insensitive copy of the broker mapping
    brokers_map = {k.lower(): v for k, v in BROKER_TO_EMAIL.items()}
    
    logger.info(f"Loaded {len(brokers_map)} brokers from static mapping")
    return brokers_map

def standardize_broker_name(broker):
    """Standardize broker names to handle variations and typos"""
    if pd.isna(broker) or broker is None:
        return None
    broker = str(broker).strip()
    return BROKER_NAME_STANDARDIZATION.get(broker, broker)

def get_broker_email(broker):
    """Get broker email with standardized name mapping"""
    if pd.isna(broker) or broker is None:
        return None
    
    # Standardize broker name first
    std_broker = standardize_broker_name(broker)
    if std_broker is None:
        return None
        
    # Get email from mapping
    email = BROKER_EMAIL_MAPPING.get(std_broker)
    if email is None:
        logger.warning(f"No email mapping found for broker '{broker}' (standardized: '{std_broker}')")
    return email

def process_policies(policies, carriers_map, brokers_map, logger):
    logger.info("Processing policies (premium calculation, status assignment, deduplication)")
    
    # Track policy numbers for deduplication
    policy_numbers = {}
    valid_policies = []
    invalid_policies = []
    unmapped_brokers = set()  # Track unique unmapped brokers
    
    # List of invalid policy number patterns
    invalid_patterns = [
        "audit", "refund", "2nd payment", "voided", "nan", "broker fee", 
        "payment to carrier", "full refund", "additional broker fee", 
        "limits endorsement", "payment declined", "monthly payment", 
        "second payment", "payment", "gl monthly payment", "audit payment"
    ]
    
    # Current date for status determination
    current_date = datetime.strptime("2025-03-02", "%Y-%m-%d").date()
    
    for policy in policies:
        # Get policy number and ensure it's a string
        policy_number = policy.get('policy_number', '')
        
        # Convert policy_number to string if it's not already
        if not isinstance(policy_number, str):
            policy_number = str(policy_number)
            policy['policy_number'] = policy_number
        
        # Map broker to email if brokers_map is provided - do this for ALL policies
        if brokers_map and policy.get('broker'):
            # Convert broker to string if it's not already
            if not isinstance(policy['broker'], str):
                policy['broker'] = str(policy['broker'])
            
            broker_name = policy['broker'].lower().strip()
            if broker_name in brokers_map:
                policy['broker_email'] = brokers_map[broker_name]
                logger.debug(f"Mapped broker '{policy['broker']}' to email '{policy['broker_email']}'")
            else:
                unmapped_brokers.add(policy['broker'])
                logger.warning(f"No email mapping found for broker '{policy['broker']}' in policy {policy_number}")
        
        # Check if this is an invalid policy number
        is_invalid = False
        if not policy_number or policy_number.lower().strip() in invalid_patterns:
            is_invalid = True
        else:
            # Check if it matches any of the invalid patterns
            for pattern in invalid_patterns:
                if pattern in policy_number.lower():
                    is_invalid = True
                    break
        
        # Process invalid policies differently
        if is_invalid:
            logger.info(f"Moving policy with invalid number '{policy_number}' to invalid list")
            invalid_policies.append(policy)
            continue
        
        # For valid policies, calculate premium and assign status
        
        # 1. Calculate premium based on commission amount and carrier percentage
        if 'commission_amount' in policy and policy['commission_amount']:
            try:
                commission_amount = float(policy['commission_amount'])
                
                # Get carrier commission percentage (default to 0.15 if missing or zero)
                carrier_percentage = 0.15
                if policy.get('carrier'):
                    # Convert carrier to string if it's not already
                    if not isinstance(policy['carrier'], str):
                        policy['carrier'] = str(policy['carrier'])
                    
                    if policy['carrier'].lower() in carriers_map:
                        carrier_info = carriers_map[policy['carrier'].lower()]
                        if carrier_info.get('commission') and float(carrier_info['commission']) > 0:
                            carrier_percentage = float(carrier_info['commission']) / 100.0  # Convert percentage to decimal
                
                # Calculate premium
                if carrier_percentage > 0:
                    policy['premium'] = round(commission_amount / carrier_percentage, 2)
                else:
                    policy['premium'] = round(commission_amount / 0.15, 2)  # Use default if percentage is zero
                    logger.warning(f"Using default commission percentage (15%) for policy {policy_number}")
            except (ValueError, TypeError) as e:
                logger.error(f"Error calculating premium for policy {policy_number}: {str(e)}")
                policy['premium'] = 0.0
        else:
            policy['premium'] = 0.0
        
        # 2. Assign status
        policy['status'] = 'Active'  # Default
        if policy.get('cancellation_date') and policy['cancellation_date']:
            policy['status'] = 'Canceled'
        elif policy.get('expiration_date'):
            try:
                expiration_date = datetime.strptime(str(policy['expiration_date']), "%Y-%m-%d").date()
                logger.debug(f"Policy {policy_number}: expiration_date={expiration_date}, current_date={current_date}, active={expiration_date >= current_date}")
                if expiration_date >= current_date:
                    policy['status'] = 'Active'
                else:
                    policy['status'] = 'Expired'
            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing expiration_date for policy {policy_number}: {str(e)}")
        
        # 3. Handle non-unique policy numbers
        is_endorsement = policy_number.lower() == "endorsement"
        is_duplicate = policy_number in policy_numbers
        
        if is_endorsement or is_duplicate:
            # Only add suffix for endorsements
            if is_endorsement:
                suffix_base = "-E"
                count = policy_numbers.get("endorsement", {}).get("count", 0) + 1
                policy_numbers.setdefault("endorsement", {})["count"] = count
                new_policy_number = f"{policy_number}{suffix_base}{count}"
                logger.info(f"Renaming endorsement to {new_policy_number}")
                policy['policy_number'] = new_policy_number
                valid_policies.append(policy)
            else:
                # For other duplicates, determine which to keep based on date and completeness
                existing_policy = policy_numbers[policy_number]["policy"]
                
                # Compare effective dates if available
                keep_new = False
                
                if policy.get('effective_date') and existing_policy.get('effective_date'):
                    try:
                        new_date = None
                        existing_date = None
                        
                        if isinstance(policy['effective_date'], str):
                            new_date = datetime.strptime(policy['effective_date'], "%Y-%m-%d").date()
                        elif isinstance(policy['effective_date'], datetime):
                            new_date = policy['effective_date'].date()
                            
                        if isinstance(existing_policy['effective_date'], str):
                            existing_date = datetime.strptime(existing_policy['effective_date'], "%Y-%m-%d").date()
                        elif isinstance(existing_policy['effective_date'], datetime):
                            existing_date = existing_policy['effective_date'].date()
                        
                        if new_date and existing_date and new_date > existing_date:
                            keep_new = True
                    except (ValueError, TypeError) as e:
                        logger.error(f"Error comparing dates for duplicate policies: {str(e)}")
                
                # If dates are equal or can't be compared, check completeness
                if not keep_new:
                    # Count non-empty fields as a measure of completeness
                    new_fields = sum(1 for k, v in policy.items() if v)
                    existing_fields = sum(1 for k, v in existing_policy.items() if v)
                    
                    if new_fields > existing_fields:
                        keep_new = True
                
                if keep_new:
                    # Replace the existing policy with the new one
                    logger.info(f"Replacing policy {policy_number} with more recent/complete version")
                    # Move the existing policy to invalid list
                    invalid_policies.append(existing_policy)
                    # Update the tracking dictionary with the new policy
                    policy_numbers[policy_number]["policy"] = policy
                    valid_policies.append(policy)
                else:
                    # Keep the existing policy, move the new one to invalid list
                    logger.info(f"Keeping existing policy {policy_number}, moving duplicate to invalid list")
                    invalid_policies.append(policy)
        else:
            # First occurrence of this policy number
            policy_numbers[policy_number] = {
                "count": 1,
                "policy": policy
            }
            valid_policies.append(policy)
    
    logger.info(f"Processed {len(valid_policies)} valid policies and {len(invalid_policies)} invalid policies")
    
    # Log summary of unmapped brokers
    if unmapped_brokers:
        logger.warning(f"Found {len(unmapped_brokers)} unmapped brokers: {', '.join(sorted(unmapped_brokers))}")
    
    return valid_policies, invalid_policies

def fetch_ams_policies(logger, use_cache=True):
    """Fetch existing policy numbers from AMS API"""
    cache_file = os.path.join(CACHE_DIR, "ams_policies.csv")
    policy_numbers = set()
    
    # Check if AMS API is properly configured
    if not AMS_API_HEADERS:
        logger.error("AMS API headers not configured. Please check token setup.")
        return policy_numbers
    
    if use_cache and os.path.exists(cache_file):
        try:
            logger.debug(f"Loading policies from cache: {cache_file}")
            df = pd.read_csv(cache_file)
            policy_numbers = set(df['policy_number'].dropna().astype(str))
            logger.info(f"Loaded {len(policy_numbers)} policy numbers from cache")
            return policy_numbers
        except Exception as e:
            logger.error(f"Error loading policies from cache: {str(e)}")
    
    logger.info("Fetching policy numbers from AMS API")
    try:
        all_policies = []
        page = 0
        page_size = 1000
        more_data = True
        
        # Log API configuration
        logger.debug(f"API URL: {AMS_API_URL}")
        logger.debug(f"API Headers: {json.dumps({k: ('***' if k == 'Authorization' else v) for k, v in AMS_API_HEADERS.items()})}")
        logger.debug(f"Authorization header format check: {AMS_API_HEADERS.get('Authorization', '').startswith('Token ')}")
        
        while more_data:
            page += 1
            logger.debug(f"Fetching AMS policies, page {page}...")
            
            payload = {
                "doctype": "Policy",
                "fields": ["policy_number"],
                "limit_start": (page - 1) * page_size,
                "limit_page_length": page_size
            }
            logger.debug(f"Request payload: {json.dumps(payload)}")
            
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    response = requests.post(
                        f"{AMS_API_URL}/frappe.client.get_list",
                        headers=AMS_API_HEADERS,
                        json=payload,
                        timeout=30
                    )
                    
                    # Log detailed response information
                    logger.debug(f"Response status code: {response.status_code}")
                    logger.debug(f"Response headers: {dict(response.headers)}")
                    
                    # Try to parse response as JSON first
                    try:
                        response_data = response.json()
                        logger.debug(f"Response data: {json.dumps(response_data)}")
                    except json.JSONDecodeError:
                        logger.debug(f"Raw response text: {response.text}")
                        raise
                    
                    response.raise_for_status()
                    
                    if "message" in response_data and isinstance(response_data["message"], list):
                        policies = response_data["message"]
                        all_policies.extend(policies)
                        logger.debug(f"Retrieved {len(policies)} policies on page {page}")
                        
                        if len(policies) < page_size:
                            more_data = False
                            break
                    else:
                        error_msg = response_data.get("error", "Unknown error")
                        logger.warning(f"Unexpected API response format. Error: {error_msg}")
                        more_data = False
                    break
                    
                except requests.exceptions.RequestException as e:
                    error_msg = str(e)
                    if hasattr(e.response, 'text'):
                        try:
                            error_data = e.response.json()
                            error_msg = error_data.get('error', error_msg)
                        except:
                            error_msg = e.response.text
                    
                    if attempt < max_retries - 1:
                        logger.warning(f"API request failed (attempt {attempt+1}/{max_retries}): {error_msg}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.error(f"Failed to fetch policies after {max_retries} retries: {error_msg}")
                        return policy_numbers
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse API response as JSON: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        return policy_numbers
        
        if all_policies:
            try:
                df = pd.DataFrame(all_policies)
                os.makedirs(os.path.dirname(cache_file), exist_ok=True)
                df.to_csv(cache_file, index=False)
                logger.debug(f"Saved {len(all_policies)} policies to cache")
            except Exception as e:
                logger.error(f"Error saving policies to cache: {str(e)}")
        
        policy_numbers = set(policy.get("policy_number") for policy in all_policies if policy.get("policy_number"))
        logger.info(f"Fetched {len(policy_numbers)} policy numbers from AMS")
        return policy_numbers
        
    except Exception as e:
        logger.error(f"Error fetching policies: {str(e)}")
        return policy_numbers

def save_policies_to_csv(policies, filename, logger):
    """
    Save policies to a CSV file
    
    Args:
        policies: List of policy dictionaries
        filename: Path to save the CSV file
        logger: Logger instance
    
    Returns:
        None
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(policies)
        
        # Save to CSV
        df.to_csv(filename, index=False)
        
        logger.info(f"Saved {len(policies)} policies to {filename}")
    except Exception as e:
        logger.error(f"Error saving policies to {filename}: {str(e)}")

def normalize_policy_type(policy_type):
    """
    Normalize policy type to match AMS-allowed options.
    Uses pattern matching to handle variations, typos, and manual entry differences.
    """
    if not policy_type or not isinstance(policy_type, str):
        return "Other"
    
    # Clean up the input
    policy_type = policy_type.strip().lower()
    
    # Skip invalid/special cases
    invalid_types = {
        "test", "2nd payment", "refund", "broker fee", "payment", 
        "monthly payment", "audit", "voided", "nan", "state endorsement",
        "limits endorsement", "comp ops endorsement", "house of worship",
        "per project endors", "ai endorsement", "non-eroding endors",
        "multi unit endorsement", "class codes endors", "wos endorsement",
        "state endorsement", "limits + comp ops endorsement", "gross & subs endorsement",
        "med limit endorsement", "water endorsement", "non eroding limits",
        "per project", "blanket ai endors", "states endorsement", "2nd paymet",
        "borker fee", "gl monthly payment", "audit payment", "full refund",
        "payment to carrier", "additional broker fee", "payment declined",
        "second payment", "gl endorsement", "cg2010", "cg2010 endors",
        "non-eroding limints", "specific endorsement", "single ai"
    }
    if policy_type in invalid_types or any(term in policy_type for term in ["payment", "endorsement", "endors", "limits"]):
        return "Other"
    
    # Direct matches (case-insensitive)
    policy_type_map = {
        "bond": "Bond",
        "simple bonds": "Bond",
        "bond express": "Bond",
        "bound": "Bond",
        "sipmle bonds": "Bond",  # Handle typo found in CSV
        "bonds express": "Bond",
        "commercial auto": "Commercial Auto",
        "auto": "Commercial Auto",
        "automobile": "Commercial Auto",
        "bolt access": "Commercial Auto",
        "commercial property": "Commercial Property",
        "excess": "Excess",
        "excess policy": "Excess",
        "general liability": "General Liability",
        "gl": "General Liability",
        "gl renewal": "General Liability",
        "gl rewrite": "General Liability",
        "general libaility": "General Liability",
        "inland marine": "Inland Marine",
        "equipment": "Inland Marine",
        "equipment renewal": "Inland Marine",
        "pollution liability": "Pollution Liability",
        "professional liability": "Professional Liability",
        "workers comp": "Workers Compensation",
        "workers compensation": "Workers Compensation",
        "wc renewal": "Workers Compensation",
        "worker comp": "Workers Compensation",
        "wc limits endorsement": "Workers Compensation"
    }
    
    # Check for direct matches first
    if policy_type in policy_type_map:
        return policy_type_map[policy_type]
    
    # Handle common variations and combinations
    if "workers" in policy_type and ("comp" in policy_type or "compensation" in policy_type):
        return "Workers Compensation"
    elif "general" in policy_type and "liability" in policy_type:
        if "excess" in policy_type:
            return "General Liability + Excess"
        elif "inland" in policy_type and "marine" in policy_type:
            return "General Liability + Inland Marine"
        elif "builders" in policy_type and "risk" in policy_type:
            return "General Liability + Builders Risk"
        else:
            return "General Liability"
    elif policy_type.startswith("gl ") or policy_type == "gl":
        return "General Liability"
    elif "builders" in policy_type and "risk" in policy_type:
        return "Builders Risk"
    elif "inland" in policy_type and "marine" in policy_type:
        return "Inland Marine"
    elif "commercial" in policy_type and "auto" in policy_type:
        return "Commercial Auto"
    elif "commercial" in policy_type and "property" in policy_type:
        return "Commercial Property"
    elif "professional" in policy_type and "liability" in policy_type:
        return "Professional Liability"
    elif "pollution" in policy_type and "liability" in policy_type:
        return "Pollution Liability"
    elif "bond" in policy_type:
        return "Bond"
    elif "excess" in policy_type:
        return "Excess"
    elif "auto" in policy_type or "automobile" in policy_type:
        return "Commercial Auto"
    elif "equipment" in policy_type:
        return "Inland Marine"
    
    # Default to "Other" if no match found
    return "Other"

def upload_to_ams(policy, logger):
    """
    Upload a policy to the AMS system via API.
    
    Args:
        policy: Dictionary containing policy data
        logger: Logger instance
    
    Returns:
        bool: True if upload successful, False otherwise
    """
    if not AMS_API_HEADERS:
        logger.error("AMS API headers not configured")
        return False
    
    try:
        # Prepare the payload
        payload = {
            "doctype": "Policy",
            "policy_number": policy["policy_number"],
            "effective_date": policy.get("effective_date"),
            "expiration_date": policy.get("expiration_date"),
            "status": policy.get("status", "Active"),
            "premium": policy.get("premium", 0.0),
            "broker": policy.get("broker_email", ""),  # Use broker email directly
            "policy_type": normalize_policy_type(policy.get("policy_type")),
            "carrier": policy.get("carrier"),
            "commission_amount": policy.get("commission_amount", 0.0),
            "broker_fee": policy.get("broker_fee_amount", 0.0)
        }
        
        # Make API request with retry logic
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    f"{AMS_API_URL}/policies",
                    headers=AMS_API_HEADERS,
                    json=payload
                )
                
                if response.status_code == 201:
                    logger.info(f"Successfully uploaded policy {policy['policy_number']}")
                    return True
                else:
                    error_msg = f"Failed to upload policy {policy['policy_number']}"
                    try:
                        error_data = response.json()
                        error_msg = f"{error_msg}: {error_data.get('error', 'Unknown error')}"
                    except:
                        error_msg = f"{error_msg}: HTTP {response.status_code}"
                    
                    if attempt < max_retries - 1:
                        logger.warning(f"{error_msg}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.error(error_msg)
                        return False
            
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request failed: {str(e)}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed to upload policy after {max_retries} attempts: {str(e)}")
                    return False
        
        return False
    
    except Exception as e:
        logger.error(f"Error uploading policy {policy.get('policy_number', 'unknown')}: {str(e)}")
        return False

def push_to_github(logger):
    """Push all changes to GitHub repository"""
    try:
        # Create a list of files to push
        files_to_push = {
            # Core files
            "policy_migration.py": "policy_migration.py",
            "README.md": "README.md",
            "requirements.txt": "requirements.txt",
            ".gitignore": ".gitignore",
            "setup_env.py": "setup_env.py",
            "init_git_repo.py": "init_git_repo.py",
            
            # Output files in data/reports
            "data/reports/valid_policies.csv": os.path.join(OUTPUT_DIR, "valid_policies.csv"),
            "data/reports/invalid_policies.csv": os.path.join(OUTPUT_DIR, "invalid_policies.csv"),
            "data/reports/new_policies.csv": os.path.join(OUTPUT_DIR, "new_policies.csv"),
            "data/reports/existing_policies.csv": os.path.join(OUTPUT_DIR, "existing_policies.csv"),
            
            # Log file
            "logs/policy_upload_log.txt": LOG_FILE
        }
        
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        
        # Copy log file to logs directory
        import shutil
        if os.path.exists(LOG_FILE):
            shutil.copy2(LOG_FILE, "logs/policy_upload_log.txt")
        
        # Fixed repository name and description
        repo_name = "insurance_policy_migration"
        description = f"Insurance Policy Migration - Updated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Push to GitHub
        repo_url = create_github_repo(
            repo_name=repo_name,
            description=description,
            files_dict=files_to_push,
            private=False,
            logger=logger,
            include_all_files=True  # This will include all files in the workspace
        )
        
        if repo_url:
            logger.info(f"Successfully pushed changes to GitHub: {repo_url}")
            return True
        else:
            logger.error("Failed to push changes to GitHub")
            return False
            
    except Exception as e:
        logger.error(f"Error pushing to GitHub: {str(e)}")
        return False

def main():
    """Main function"""
    # Parse arguments
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logging()
    
    # Log script start
    logger.info("Insurance Policy Migration Script - Started")
    logger.info(f"Dry run mode: {args.dry_run}")
    
    # Setup AMS API token
    if not setup_ams_api_token(args.ams_token, logger):
        logger.error("Failed to set up AMS API token. Exiting.")
        return None, None, None, None, None, None, None
    
    # Load CSV files
    policies = load_csv_files(logger)
    
    # Fetch AMS data
    insureds_map = fetch_ams_insureds(logger, use_cache=not args.no_cache)
    carriers_map = fetch_ams_carriers(logger, use_cache=not args.no_cache)
    
    # Fetch brokers from static mapping
    brokers_map = fetch_close_brokers(logger)
    
    # Fetch existing policy numbers from AMS
    existing_policy_numbers = set()
    if not args.skip_ams_fetch:
        existing_policy_numbers = fetch_ams_policies(logger, use_cache=not args.no_cache)
    
    logger.info(f"Fetched {len(insureds_map)} insureds, {len(carriers_map)} carriers, {len(brokers_map)} brokers, and {len(existing_policy_numbers)} existing policies")
    
    # Process policies
    valid_policies, invalid_policies = process_policies(policies, carriers_map, brokers_map, logger)
    
    # Split valid policies into new and existing
    new_policies = []
    existing_policies = []
    
    for policy in valid_policies:
        policy_number = policy.get("policy_number", "").strip()
        if policy_number in existing_policy_numbers:
            existing_policies.append(policy)
        else:
            new_policies.append(policy)
    
    # Log results
    logger.info(f"Successfully processed {len(policies)} total policies")
    logger.info(f"Valid policies for upload: {len(valid_policies)}")
    logger.info(f"New policies for upload: {len(new_policies)}")
    logger.info(f"Existing policies in AMS: {len(existing_policies)}")
    logger.info(f"Invalid policies for review: {len(invalid_policies)}")
    
    # Print sample policies
    if valid_policies:
        logger.info(f"Sample valid policy: {json.dumps(valid_policies[0], indent=2)}")
    if invalid_policies:
        logger.info(f"Sample invalid policy: {json.dumps(invalid_policies[0], indent=2)}")
    
    # Save policies to CSV files
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    valid_policies_file = os.path.join(OUTPUT_DIR, "valid_policies.csv")
    invalid_policies_file = os.path.join(OUTPUT_DIR, "invalid_policies.csv")
    new_policies_file = os.path.join(OUTPUT_DIR, "new_policies.csv")
    existing_policies_file = os.path.join(OUTPUT_DIR, "existing_policies.csv")
    
    save_policies_to_csv(valid_policies, valid_policies_file, logger)
    save_policies_to_csv(invalid_policies, invalid_policies_file, logger)
    save_policies_to_csv(new_policies, new_policies_file, logger)
    save_policies_to_csv(existing_policies, existing_policies_file, logger)
    
    # Upload new policies to AMS if not in dry run mode
    if not args.dry_run:
        logger.info("Uploading new policies to AMS...")
        upload_count = 0
        for policy in new_policies:
            if upload_to_ams(policy, logger):
                upload_count += 1
        logger.info(f"Successfully uploaded {upload_count} out of {len(new_policies)} new policies to AMS")
    
    # Push changes to GitHub
    push_to_github(logger)
    
    logger.info("Insurance Policy Migration Script - Completed")
    return valid_policies, invalid_policies, new_policies, existing_policies, insureds_map, carriers_map, brokers_map

if __name__ == "__main__":
    main()
