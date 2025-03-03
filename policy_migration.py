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
AMS_API_TOKEN = os.environ.get("AMS_API_TOKEN")  # Get token from environment variable
AMS_API_HEADERS = {
    "Authorization": f"Token {AMS_API_TOKEN}",
    "Content-Type": "application/json"
}

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
    parser.add_argument("--upload-log", action="store_true", help="Upload log file to GitHub")
    parser.add_argument("--upload-script", action="store_true", help="Upload script file to GitHub")
    parser.add_argument("--github-token", help="GitHub API token for GitHub upload")
    parser.add_argument("--ams-token", help="AMS API token for AMS API calls")
    parser.add_argument("--skip-ams-fetch", action="store_true", help="Skip fetching policies from AMS")
    parser.add_argument("--include-all-files", action="store_true", help="Include all files in the repository")
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
        while more_data:
            page += 1
            logger.debug(f"Fetching AMS policies, page {page}...")
            payload = {
                "doctype": "Policy",
                "fields": ["policy_number"],
                "limit_start": (page - 1) * page_size,
                "limit_page_length": page_size
            }
            logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")
            logger.debug(f"Request headers: {json.dumps(AMS_API_HEADERS, indent=2)}")
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
                    logger.debug(f"API Response Status: {response.status_code}")
                    logger.debug(f"API Response Headers: {dict(response.headers)}")
                    logger.debug(f"API Response Content: {response.text}")
                    response.raise_for_status()
                    data = response.json()
                    if "message" in data and isinstance(data["message"], list):
                        policies = data["message"]
                        all_policies.extend(policies)
                        logger.debug(f"Retrieved {len(policies)} policies on page {page}")
                        if len(policies) < page_size:
                            more_data = False
                            break
                    else:
                        logger.warning(f"Unexpected API response format: {data}")
                        more_data = False
                    break
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"API request failed (attempt {attempt+1}/{max_retries}): {str(e)}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.error(f"Failed to fetch policies after retries: {str(e)}")
                        return set()
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
        return set()

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

def upload_files_to_gist(files_dict, description, public=True, logger=None):
    """
    Upload multiple files to GitHub Gist and return the URL
    
    Args:
        files_dict: Dictionary mapping filenames to file paths
        description: Description of the Gist
        public: Whether the Gist should be public
        logger: Logger instance
        
    Returns:
        URL of the created Gist or None if upload failed
    """
    if logger is None:
        logger = logging.getLogger()
    
    # Get GitHub token from environment variable if not set
    github_token = GITHUB_TOKEN or os.environ.get("GITHUB_TOKEN")
    if not github_token:
        logger.error("GitHub token not found. Set GITHUB_TOKEN environment variable.")
        return None
    
    try:
        # Prepare the Gist data with multiple files
        gist_files = {}
        
        for file_name, file_path in files_dict.items():
            try:
                # Read the file with error handling for encoding issues
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                except UnicodeDecodeError:
                    # If UTF-8 fails, try with errors='replace' to handle invalid characters
                    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                        content = f.read()
                        logger.warning(f"Some characters in the file {file_name} could not be decoded with UTF-8 and were replaced.")
                
                gist_files[file_name] = {"content": content}
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {str(e)}")
                # Continue with other files even if one fails
        
        if not gist_files:
            logger.error("No files could be read for upload")
            return None
            
        gist_data = {
            "description": description,
            "public": public,
            "files": gist_files
        }
        
        # Set up authentication headers
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Create the Gist
        response = requests.post(
            f"{GITHUB_API_URL}/gists",
            headers=headers,
            json=gist_data
        )
        response.raise_for_status()
        
        # Get the Gist URL
        gist_data = response.json()
        gist_url = gist_data.get("html_url")
        
        logger.info(f"Files uploaded to GitHub Gist: {gist_url}")
        return gist_url
    
    except Exception as e:
        logger.error(f"Error uploading files to GitHub Gist: {str(e)}")
        return None

def create_github_repo(repo_name, description, files_dict, private=True, logger=None, include_all_files=False):
    """
    Create a GitHub repository and upload files to it.
    
    Args:
        repo_name (str): Name of the repository
        description (str): Repository description
        files_dict (dict): Dictionary of files to upload {filename: content}
        private (bool): Whether the repository should be private (default: True)
        logger (logging.Logger): Logger instance
        include_all_files (bool): Whether to include all files in the directory
        
    Returns:
        str: URL of the created repository
    """
    if logger is None:
        logger = logging.getLogger()
    
    # Get GitHub token from environment variable if not set
    github_token = GITHUB_TOKEN or os.environ.get("GITHUB_TOKEN")
    if not github_token:
        logger.error("GitHub token not found. Set GITHUB_TOKEN environment variable.")
        return None
    
    try:
        # Set up authentication headers
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Create the repository
        repo_data = {
            "name": repo_name,
            "description": description,
            "private": private,
            "auto_init": True  # Initialize with a README
        }
        
        # Check if repository already exists
        try:
            response = requests.get(
                f"{GITHUB_API_URL}/repos/{GITHUB_USERNAME}/{repo_name}",
                headers=headers
            )
            
            if response.status_code == 200:
                logger.info(f"Repository {repo_name} already exists, using existing repository")
                repo_info = response.json()
                repo_url = repo_info.get("html_url")
                repo_api_url = repo_info.get("url")
            else:
                # Create the repository
                response = requests.post(
                    f"{GITHUB_API_URL}/user/repos",
                    headers=headers,
                    json=repo_data
                )
                response.raise_for_status()
                
                # Get the repository details
                repo_info = response.json()
                repo_url = repo_info.get("html_url")
                repo_api_url = repo_info.get("url")
                
                logger.info(f"Created GitHub repository: {repo_url}")
                
                # Wait a moment for the repository to be fully created
                time.sleep(2)
        except Exception as e:
            logger.error(f"Error checking/creating repository: {str(e)}")
            return None
        
        # If include_all_files is True, add all files in the directory
        if include_all_files:
            # Add all files in the current directory and subdirectories
            for root, dirs, files in os.walk('.'):
                # Skip virtual environment and __pycache__ directories
                if 'venv' in dirs:
                    dirs.remove('venv')
                if '__pycache__' in dirs:
                    dirs.remove('__pycache__')
                if '.git' in dirs:
                    dirs.remove('.git')
                
                for file in files:
                    # Skip the log file if it's already in files_dict
                    if file == os.path.basename(LOG_FILE) and os.path.basename(LOG_FILE) in files_dict:
                        continue
                    
                    # Skip hidden files
                    if file.startswith('.'):
                        continue
                    
                    file_path = os.path.join(root, file)
                    # Use relative path for the file name in the repository
                    repo_file_path = file_path.replace('\\', '/').lstrip('./')
                    files_dict[repo_file_path] = file_path
        
        # Upload each file to the repository
        for file_name, file_path in files_dict.items():
            try:
                # Read the file with error handling for encoding issues
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                except UnicodeDecodeError:
                    # If UTF-8 fails, try with errors='replace' to handle invalid characters
                    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                        content = f.read()
                        logger.warning(f"Some characters in the file {file_name} could not be decoded with UTF-8 and were replaced.")
                except Exception as e:
                    # Try reading as binary for non-text files
                    try:
                        with open(file_path, 'rb') as f:
                            content_bytes = f.read()
                            base64_content = base64.b64encode(content_bytes).decode('utf-8')
                            
                            # Prepare the file data
                            file_data = {
                                "message": f"Add {file_name}",
                                "content": base64_content
                            }
                            
                            # Create the file in the repository
                            file_response = requests.put(
                                f"{repo_api_url}/contents/{file_name}",
                                headers=headers,
                                json=file_data
                            )
                            file_response.raise_for_status()
                            
                            logger.info(f"Uploaded binary file {file_name} to repository")
                            continue
                    except Exception as inner_e:
                        logger.error(f"Error reading file {file_path}: {str(inner_e)}")
                        continue
                
                # Encode content to base64
                content_bytes = content.encode('utf-8')
                base64_content = base64.b64encode(content_bytes).decode('utf-8')
                
                # Prepare the file data
                file_data = {
                    "message": f"Add {file_name}",
                    "content": base64_content
                }
                
                # Create the file in the repository
                file_response = requests.put(
                    f"{repo_api_url}/contents/{file_name}",
                    headers=headers,
                    json=file_data
                )
                file_response.raise_for_status()
                
                logger.info(f"Uploaded {file_name} to repository")
                
            except Exception as e:
                logger.error(f"Error uploading file {file_path} to repository: {str(e)}")
                # Continue with other files even if one fails
        
        # Create a .gitignore file if it doesn't exist
        gitignore_content = """
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
ENV/
env/
env.bak/
venv.bak/

# Environment variables
.env

# Logs
*.log

# Cache
.cache/
"""
        gitignore_data = {
            "message": "Add .gitignore",
            "content": base64.b64encode(gitignore_content.encode('utf-8')).decode('utf-8')
        }
        
        try:
            gitignore_response = requests.put(
                f"{repo_api_url}/contents/.gitignore",
                headers=headers,
                json=gitignore_data
            )
            if gitignore_response.status_code == 201:
                logger.info("Added .gitignore file to repository")
        except Exception as e:
            logger.error(f"Error adding .gitignore file: {str(e)}")
        
        return repo_url
    
    except Exception as e:
        logger.error(f"Error creating GitHub repository: {str(e)}")
        return None

def upload_log_to_gist(log_file_path, description, public=True, logger=None):
    """Upload a log file to GitHub Gist and return the URL"""
    if logger is None:
        logger = logging.getLogger()
    
    # Get GitHub token from environment variable if not set
    github_token = GITHUB_TOKEN or os.environ.get("GITHUB_TOKEN")
    if not github_token:
        logger.error("GitHub token not found. Set GITHUB_TOKEN environment variable.")
        return None
    
    try:
        # Read the log file with error handling for encoding issues
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            # If UTF-8 fails, try with errors='replace' to handle invalid characters
            with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
                logger.warning("Some characters in the log file could not be decoded with UTF-8 and were replaced.")
        
        # Prepare the Gist data
        filename = os.path.basename(log_file_path)
        gist_data = {
            "description": description,
            "public": public,
            "files": {
                filename: {
                    "content": content
                }
            }
        }
        
        # Set up authentication headers
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Create the Gist
        response = requests.post(
            f"{GITHUB_API_URL}/gists",
            headers=headers,
            json=gist_data
        )
        response.raise_for_status()
        
        # Get the Gist URL
        gist_data = response.json()
        gist_url = gist_data.get("html_url")
        
        logger.info(f"Log file uploaded to GitHub Gist: {gist_url}")
        return gist_url
    
    except Exception as e:
        logger.error(f"Error uploading log to GitHub Gist: {str(e)}")
        return None

def main():
    """Main function"""
    # Parse arguments
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logging()
    
    # Log script start
    logger.info("Insurance Policy Migration Script - Started")
    logger.info(f"Dry run mode: {args.dry_run}")
    
    # Set AMS API token from command line argument if provided
    if args.ams_token:
        global AMS_API_TOKEN
        AMS_API_TOKEN = args.ams_token
        global AMS_API_HEADERS
        AMS_API_HEADERS = {
            "Authorization": f"Token {AMS_API_TOKEN}",
            "Content-Type": "application/json"
        }
    
    # Check if AMS API token is set
    if not AMS_API_TOKEN:
        logger.error("AMS API token not found. Set AMS_API_TOKEN environment variable or use --ams-token.")
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
    
    # Process policies (premium calculation, status assignment, deduplication)
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
    
    # Always print a sample policy, not just in dry-run mode
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
    
    # Upload log file and/or script to GitHub
    if args.upload_log or args.upload_script:
        # Set GitHub token from command line argument if provided
        if args.github_token:
            global GITHUB_TOKEN
            GITHUB_TOKEN = args.github_token
            
        # Use fixed repository name
        repo_name = "insurance_policy_migration"
        description = f"Insurance Policy Migration - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        files_to_upload = {}
        if args.upload_log:
            files_to_upload[os.path.basename(LOG_FILE)] = LOG_FILE
        
        if args.upload_script:
            script_path = os.path.abspath(__file__)
            files_to_upload[os.path.basename(script_path)] = script_path
            
        # Add CSV files to upload
        files_to_upload["valid_policies.csv"] = valid_policies_file
        files_to_upload["invalid_policies.csv"] = invalid_policies_file
        files_to_upload["new_policies.csv"] = new_policies_file
        files_to_upload["existing_policies.csv"] = existing_policies_file
        
        if files_to_upload:
            repo_url = create_github_repo(repo_name, description, files_to_upload, private=False, logger=logger, include_all_files=args.include_all_files)
            if repo_url:
                logger.info(f"Files uploaded to GitHub repository: {repo_url}")
    
    logger.info("Insurance Policy Migration Script - Completed")
    return valid_policies, invalid_policies, new_policies, existing_policies, insureds_map, carriers_map, brokers_map

if __name__ == "__main__":
    main()
