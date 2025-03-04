#!/usr/bin/env python3
"""
Insurance Policy Migration Script

Migrates insurance policy data from CSV files to an AMS via Frappe API.
- Uses precomputed mappings from ./data/mappings/
- Excludes non-policy/non-carrier scenarios
- Optimized for performance and maintainability
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
import base64
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path
import asyncio
import aiohttp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Constants (move to config later)
INPUT_DIR = Path("./data/input/")
OUTPUT_DIR = Path("./data/reports/")
CACHE_DIR = Path("./data/cache/")
MAPPINGS_DIR = Path("./data/mappings/")
LOG_FILE = Path("policy_upload_log.txt")
AMS_API_URL = "https://ams.jmggo.com/api/method"
GITHUB_API_URL = "https://api.github.com"
GITHUB_USERNAME = "grijalva10"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  # Get GitHub token from environment

# Initialize mappings as None
BROKER_MAPPING = None
CARRIER_MAPPING = None
POLICY_TYPE_MAPPING = None
NON_POLICY_TYPES = None
NON_CARRIER_ENTRIES = None

def load_mappings() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Set[str], Set[str]]:
    """Load all mapping files and exclusion sets."""
    mapping_files = {
        'broker': 'broker_mapping.json',
        'carrier': 'carrier_mapping.json',
        'policy_type': 'policy_type_mapping.json',
        'exclusion': 'exclusion_mapping.json'
    }
    
    try:
        mappings = {}
        for key, filename in mapping_files.items():
            path = MAPPINGS_DIR / filename
            if not path.exists():
                logger.error(f"Mapping file {path} not found")
                return {}, {}, {}, set(), set()
            with path.open('r') as f:
                mappings[key] = json.load(f)
        
        # Extract exclusion sets
        non_policy_types = set(mappings['exclusion'].get('non_policy_types', []))
        non_carrier_entries = set(mappings['exclusion'].get('non_carrier_entries', []))
        
        logger.info(f"Loaded mappings: {len(mappings['broker'])} brokers, {len(mappings['carrier'])} carriers, {len(mappings['policy_type'])} policy types")
        return mappings['broker'], mappings['carrier'], mappings['policy_type'], non_policy_types, non_carrier_entries
        
    except Exception as e:
        logger.error(f"Error loading mappings: {e}")
        return {}, {}, {}, set(), set()

def initialize_mappings():
    """Initialize global mapping variables."""
    global BROKER_MAPPING, CARRIER_MAPPING, POLICY_TYPE_MAPPING, NON_POLICY_TYPES, NON_CARRIER_ENTRIES
    BROKER_MAPPING, CARRIER_MAPPING, POLICY_TYPE_MAPPING, NON_POLICY_TYPES, NON_CARRIER_ENTRIES = load_mappings()

# AMS API setup
AMS_API_TOKEN = None
AMS_API_HEADERS = None

def setup_logging() -> logging.Logger:
    """Configure logging with file and console handlers."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(LOG_FILE, mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.handlers = [file_handler, console_handler]
    return logger

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Insurance Policy Migration Script")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Run without API calls")
    parser.add_argument("--no-cache", action="store_true", help="Ignore cached data")
    parser.add_argument("--github-token", help="GitHub API token")
    parser.add_argument("--ams-token", help="AMS API token")
    parser.add_argument("--skip-ams-fetch", action="store_true", help="Skip AMS policy fetch")
    return parser.parse_args()

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

def parse_currency(value: str) -> float:
    """Convert currency string to float."""
    if pd.isna(value) or not value:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    value_str = re.sub(r'[$,]', '', str(value).strip())
    try:
        return float(value_str)
    except ValueError:
        return 0.0

def normalize_column_name(col: str) -> str:
    """Normalize column name to lowercase and remove special characters."""
    if not col:
        return ""
    # Convert to string, lowercase, and replace spaces/special chars with underscores
    return re.sub(r'[^a-z0-9_]', '_', str(col).lower().strip())

def load_csv_files(logger: logging.Logger) -> List[Dict]:
    """Load CSV files into policy dictionaries."""
    policies = []
    csv_files = list(INPUT_DIR.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {INPUT_DIR}")
        return policies
    
    logger.info(f"Processing {len(csv_files)} CSV files")
    for file_path in csv_files:
        try:
            # Read CSV and clean headers immediately
            df = pd.read_csv(file_path)
            df.columns = [col.strip() for col in df.columns]
            
            # Clean all string columns immediately
            for col in df.columns:
                if df[col].dtype == 'object':  # Only clean string columns
                    df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else x)
            
            # Log available columns for debugging
            logger.debug(f"Available columns in {file_path.name}: {', '.join(df.columns)}")
            
            # Create normalized column map
            column_map = {normalize_column_name(col): col for col in df.columns}
            logger.debug(f"Normalized column map: {column_map}")
            
            # Define required and optional columns with variations
            required = {
                'policy_number': ['policy number', 'policy_number', 'policy', 'policy_no', 'policy_no_', 'policy_no_']
            }
            optional = {
                'effective_date': ['date', 'effective_date', 'effective date', 'start date', 'policy date'],
                'broker_fee': ['broker fee', 'broker_fee', 'brokerfee', 'broker_fee_amount'],
                'commission': ['commission', 'commission_amount', 'comm'],
                'broker': ['agent', 'broker', 'agent_name', 'broker_name'],
                'policy_type': ['policy type', 'policy_type', 'type', 'policy_category'],
                'carrier': ['carrier', 'carrier_name', 'insurance_company'],
                'premium': ['charge amount', 'premium', 'amount', 'policy_amount']
            }
            
            # Check for required columns with variations
            missing_required = []
            for field, variations in required.items():
                if not any(var in column_map for var in variations):
                    missing_required.append(field)
            
            if missing_required:
                logger.error(f"Missing required columns in {file_path.name}: {missing_required}")
                continue
            
            # Map columns with variations and clean data immediately
            mapped_df = pd.DataFrame()
            for field, variations in {**required, **optional}.items():
                for var in variations:
                    if var in column_map:
                        # Clean string values immediately after loading
                        if field == 'broker':
                            mapped_df[field] = df[column_map[var]].apply(lambda x: clean_value(x, field_type='broker'))
                        elif field in ['policy_type', 'carrier']:
                            mapped_df[field] = df[column_map[var]].apply(clean_value)
                        else:
                            mapped_df[field] = df[column_map[var]]
                        break
            
            if 'effective_date' in mapped_df:
                mapped_df['effective_date'] = mapped_df['effective_date'].apply(parse_date)
                mapped_df['expiration_date'] = mapped_df['effective_date'].apply(
                    lambda x: (datetime.strptime(x, '%Y-%m-%d') + relativedelta(years=1)).strftime('%Y-%m-%d') if x else None
                )
                mapped_df = mapped_df.dropna(subset=['effective_date'])
            
            for col in ['broker_fee', 'commission', 'premium']:
                if col in mapped_df:
                    mapped_df[f"{col}_amount" if col != 'premium' else col] = mapped_df[col].apply(parse_currency)
            
            file_policies = mapped_df.to_dict('records')
            for policy in file_policies:
                policy['source_file'] = file_path.name
            policies.extend(file_policies)
            logger.info(f"Loaded {len(file_policies)} policies from {file_path.name}")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    logger.info(f"Loaded {len(policies)} total policies")
    return policies

async def fetch_ams_data(endpoint: str, doctype: str, fields: List[str], cache_file: Path, logger: logging.Logger, use_cache: bool) -> Dict:
    """Fetch AMS data asynchronously with pagination and caching."""
    def safe_key(value) -> str:
        """Convert any value to a string key safely."""
        if value is None:
            return ""
        return str(value).lower()

    if use_cache and cache_file.exists():
        try:
            df = pd.read_csv(cache_file)
            # For single field fetches, use the field itself as the key
            key_field = fields[0] if len(fields) == 1 else fields[1]
            result = {}
            for _, row in df.iterrows():
                key = safe_key(row.get(key_field))
                if key:  # Only add non-empty keys
                    result[key] = {k: row.get(k, 0.0) for k in fields}
            logger.info(f"Loaded {len(result)} {doctype}s from cache")
            return result
        except Exception as e:
            logger.error(f"Cache load failed for {cache_file}: {e}")
    
    async def fetch_page(session: aiohttp.ClientSession, page: int, page_size: int) -> List[Dict]:
        payload = {"doctype": doctype, "fields": fields, "limit_start": (page - 1) * page_size, "limit_page_length": page_size}
        max_retries, retry_delay = 3, 2
        for attempt in range(max_retries):
            try:
                async with session.post(f"{AMS_API_URL}/frappe.client.get_list", json=payload, headers=AMS_API_HEADERS) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    return data.get("message", [])
            except (aiohttp.ClientError, ValueError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Fetch failed for {doctype}, page {page}: {e}. Retrying...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed to fetch {doctype}: {e}")
                    return []
    
    all_items = []
    page, page_size = 0, 1000
    async with aiohttp.ClientSession() as session:
        while True:
            page += 1
            items = await fetch_page(session, page, page_size)
            all_items.extend(items)
            if len(items) < page_size:
                break
    
    if all_items:
        df = pd.DataFrame(all_items)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(cache_file, index=False)
        logger.info(f"Fetched and cached {len(all_items)} {doctype}s")
    
    # For single field fetches, use the field itself as the key
    key_field = fields[0] if len(fields) == 1 else fields[1]
    result = {}
    for item in all_items:
        key = safe_key(item.get(key_field))
        if key:  # Only add non-empty keys
            result[key] = {k: item.get(k, 0.0) for k in fields}
    return result

def clean_value(value: str, field_type: str = 'default') -> str:
    """Clean and normalize a value for mapping lookup."""
    if not value or pd.isna(value):
        return ""
    
    # Convert to string and clean
    value = str(value).strip()
    
    # Special handling for broker names
    if field_type == 'broker':
        # For brokers, we want to preserve the original format
        # Just remove extra whitespace and normalize case
        value = ' '.join(word.capitalize() for word in value.split())
        return value.strip()
    
    # Default cleaning for other fields
    # Remove all types of whitespace and normalize to single space
    value = ' '.join(value.split())
    
    # Remove common suffixes and prefixes
    value = re.sub(r'\s*/\s*.*$', '', value)  # Remove everything after /
    value = re.sub(r'^\s*.*?\s*/\s*', '', value)  # Remove everything before /
    value = re.sub(r'\s*\(.*?\)', '', value)  # Remove parenthetical text
    value = re.sub(r'\s*,\s*.*$', '', value)  # Remove everything after comma
    
    # Remove ALL whitespace and special characters from start and end
    value = re.sub(r'^[\s\-_\.]+|[\s\-_\.]+$', '', value)
    
    # Normalize common variations
    value = value.replace('&', 'and')
    value = value.replace('+', 'and')
    
    # Normalize case
    value = ' '.join(word.capitalize() for word in value.split())
    
    # Remove any remaining multiple spaces
    value = re.sub(r'\s+', ' ', value)
    
    # Final strip to ensure no whitespace remains
    value = value.strip()
    
    # Log the cleaning process for debugging
    logger.debug(f"Cleaned value: '{value}'")
    
    return value

def validate_policy(policy: Dict, carriers_map: Dict, logger: logging.Logger) -> bool:
    """Validate a single policy."""
    policy_number = clean_policy_number(policy.get('policy_number', ''))
    # Allow endorsement variations as valid policy numbers
    if policy_number.lower() in {'endorsement', 'endorsements', 'limits endorsement'}:
        policy_number = 'Endorsement'  # Standardize to "Endorsement"
    elif not policy_number or policy_number.lower() in {'nan', 'none', 'null', 'refunded', 'voided', 'audit'} or 'refund' in policy_number.lower():
        logger.debug(f"Invalid policy number: {policy_number}")
        return False
    
    carrier = clean_value(policy.get('carrier', ''))
    if not carrier or carrier in NON_CARRIER_ENTRIES or CARRIER_MAPPING.get(carrier) is None:
        logger.debug(f"Invalid or excluded carrier: {carrier}")
        return False
    
    policy_type = clean_value(policy.get('policy_type', ''))
    if policy_type in NON_POLICY_TYPES or POLICY_TYPE_MAPPING.get(policy_type) is None:
        logger.debug(f"Invalid or excluded policy type: {policy_type}")
        return False
    
    if not policy.get('effective_date') or not policy.get('expiration_date'):
        logger.debug(f"Missing dates for policy {policy_number}")
        return False
    
    return True

def normalize_policy_fields(policy: Dict, carriers_map: Dict, logger: logging.Logger) -> Dict:
    """Normalize policy fields using mappings."""
    # Clean values before mapping
    carrier = clean_value(policy.get('carrier', ''))
    policy_type = clean_value(policy.get('policy_type', ''))
    broker = clean_value(policy.get('broker', ''), field_type='broker')  # Use special broker cleaning
    
    policy['carrier'] = CARRIER_MAPPING.get(carrier, carrier)
    
    # Handle endorsement variations
    policy_number = clean_policy_number(policy['policy_number'])
    if policy_number.lower() in {'endorsement', 'endorsements', 'limits endorsement'}:
        policy['policy_number'] = 'Endorsement'
        policy['policy_type'] = 'Endorsement'
    elif any(endors in policy_number.lower() for endors in ['endorsement', 'endors']):
        policy['policy_type'] = 'Endorsement'
    else:
        policy['policy_type'] = POLICY_TYPE_MAPPING.get(policy_type, 'Other')
    
    policy['broker_email'] = BROKER_MAPPING.get(broker, None)
    policy['broker'] = policy['broker_email']
    
    effective_date = datetime.strptime(policy['effective_date'], '%Y-%m-%d').date()
    expiration_date = datetime.strptime(policy['expiration_date'], '%Y-%m-%d').date()
    policy['effective_date'] = effective_date.strftime('%Y-%m-%d')
    policy['expiration_date'] = expiration_date.strftime('%Y-%m-%d')
    policy['status'] = 'Active' if expiration_date > datetime.now().date() else 'Expired'
    
    policy['premium'] = parse_currency(policy.get('premium', 0))
    policy['broker_fee_amount'] = parse_currency(policy.get('broker_fee', 0))
    policy['commission_amount'] = parse_currency(policy.get('commission', 0))
    
    return policy

def process_policies(policies: List[Dict], carriers_map: Dict, logger: logging.Logger) -> Tuple[List[Dict], List[Dict]]:
    """Process policies in batches with mappings."""
    valid_policies, invalid_policies = [], []
    unmapped = {'policy_types': set(), 'carriers': set(), 'brokers': set()}
    
    def process_policy(policy: Dict) -> Optional[Dict]:
        if not validate_policy(policy, carriers_map, logger):
            return None
        try:
            normalized = normalize_policy_fields(policy.copy(), carriers_map, logger)
            
            # Clean policy number for logging
            policy_number = str(policy.get('policy_number', '')).strip().replace('\u200b', '')
            
            # Log all mapping checks for debugging
            original_broker = policy.get('broker', '')
            cleaned_broker = clean_value(original_broker, field_type='broker')
            mapped_broker = BROKER_MAPPING.get(cleaned_broker)
            
            original_carrier = policy.get('carrier', '')
            cleaned_carrier = clean_value(original_carrier)
            mapped_carrier = CARRIER_MAPPING.get(cleaned_carrier)
            
            original_policy_type = policy.get('policy_type', '')
            cleaned_policy_type = clean_value(original_policy_type)
            mapped_policy_type = POLICY_TYPE_MAPPING.get(cleaned_policy_type)
            
            logger.debug(f"Mapping checks for policy {policy_number}:")
            logger.debug(f"  Broker: Original='{original_broker}', Cleaned='{cleaned_broker}', Mapped='{mapped_broker}'")
            logger.debug(f"  Carrier: Original='{original_carrier}', Cleaned='{cleaned_carrier}', Mapped='{mapped_carrier}'")
            logger.debug(f"  Policy Type: Original='{original_policy_type}', Cleaned='{cleaned_policy_type}', Mapped='{mapped_policy_type}'")
            
            # Check for unmapped values
            if not mapped_policy_type:
                unmapped['policy_types'].add(cleaned_policy_type)
                logger.debug(f"Added unmapped policy type: '{cleaned_policy_type}'")
            
            if not mapped_carrier:
                unmapped['carriers'].add(cleaned_carrier)
                logger.debug(f"Added unmapped carrier: '{cleaned_carrier}'")
            
            if not mapped_broker:
                unmapped['brokers'].add(cleaned_broker)
                logger.debug(f"Added unmapped broker: '{cleaned_broker}'")
            
            return normalized
        except Exception as e:
            logger.error(f"Error normalizing policy {policy.get('policy_number', 'unknown')}: {e}")
            return None
    
    # Process policies in batches
    for i in range(0, len(policies), 1000):
        batch = policies[i:i + 1000]
        logger.debug(f"Processing batch {i // 1000 + 1} ({len(batch)} policies)")
        for policy in batch:
            if normalized := process_policy(policy):
                valid_policies.append(normalized)
            else:
                invalid_policies.append(policy)
    
    # Update unmatched values
    try:
        unmatched_path = MAPPINGS_DIR / "unmatched_values.json"
        existing = {}
        if unmatched_path.exists():
            with unmatched_path.open('r') as f:
                existing = json.load(f)
        
        # Clean existing values and merge with new unmapped values
        for category, values in unmapped.items():
            # For brokers, use the special broker cleaning
            if category == 'brokers':
                cleaned_values = {clean_value(v, field_type='broker') for v in existing.get(category, []) + list(values)}
            else:
                cleaned_values = {clean_value(v) for v in existing.get(category, []) + list(values)}
            
            # Only include non-empty values that aren't in the mapping
            if category == 'brokers':
                cleaned_values = {v for v in cleaned_values if v and v not in BROKER_MAPPING}
            elif category == 'carriers':
                cleaned_values = {v for v in cleaned_values if v and v not in CARRIER_MAPPING}
            elif category == 'policy_types':
                cleaned_values = {v for v in cleaned_values if v and v not in POLICY_TYPE_MAPPING}
            
            existing[category] = sorted(list(cleaned_values))
        
        with unmatched_path.open('w') as f:
            json.dump(existing, f, indent=4)
        logger.info(f"Updated unmatched values in {unmatched_path}")
    except Exception as e:
        logger.error(f"Failed to save unmatched values: {e}")
    
    # Log unmapped values
    for key, items in unmapped.items():
        if items:
            logger.warning(f"Unmapped {key}: {', '.join(sorted(str(i) for i in items))}")
    
    logger.info(f"Processed {len(valid_policies)} valid, {len(invalid_policies)} invalid policies")
    return valid_policies, invalid_policies

async def upload_to_ams(policies: List[Dict], logger: logging.Logger) -> int:
    """Upload policies asynchronously."""
    async def upload_one(session: aiohttp.ClientSession, policy: Dict) -> bool:
        payload = {
            "doctype": "Policy", "policy_number": policy["policy_number"],
            "effective_date": policy["effective_date"], "expiration_date": policy["expiration_date"],
            "status": policy["status"], "premium": policy["premium"], "broker": policy["broker_email"],
            "policy_type": policy["policy_type"], "carrier": policy["carrier"],
            "commission_amount": policy["commission_amount"], "broker_fee": policy["broker_fee_amount"]
        }
        max_retries, retry_delay = 3, 2
        for attempt in range(max_retries):
            try:
                async with session.post(f"{AMS_API_URL}/policies", json=payload, headers=AMS_API_HEADERS) as resp:
                    if resp.status == 201:
                        logger.debug(f"Uploaded {policy['policy_number']}")
                        return True
                    logger.warning(f"Failed {policy['policy_number']}: {resp.status}")
            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed {policy['policy_number']} after retries: {e}")
                    return False
        return False
    
    upload_count = 0
    async with aiohttp.ClientSession() as session:
        tasks = [upload_one(session, policy) for policy in policies]
        results = await asyncio.gather(*tasks)
        upload_count = sum(results)
    logger.info(f"Uploaded {upload_count} of {len(policies)} policies")
    return upload_count

def push_to_github(logger: logging.Logger, token: str) -> bool:
    """Push files to GitHub dynamically."""
    from glob import glob
    files = {
        "policy_migration.py": Path("policy_migration.py"),
        **{f"data/reports/{f.name}": f for f in OUTPUT_DIR.glob("*.csv")},
        "logs/policy_upload_log.txt": LOG_FILE
    }
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    repo_name = "insurance_policy_migration"
    
    # Check if repo exists
    resp = requests.get(f"{GITHUB_API_URL}/repos/{GITHUB_USERNAME}/{repo_name}", headers=headers)
    if resp.status_code == 404:
        create_resp = requests.post(f"{GITHUB_API_URL}/user/repos", headers=headers, json={"name": repo_name, "private": False})
        if create_resp.status_code not in {200, 201}:
            logger.error(f"Failed to create repository: {create_resp.status_code}")
            return False
    
    for remote_path, local_path in files.items():
        if not local_path.exists():
            logger.debug(f"Skipping {local_path} (not found)")
            continue
            
        try:
            # Get current file content if it exists
            get_resp = requests.get(f"{GITHUB_API_URL}/repos/{GITHUB_USERNAME}/{repo_name}/contents/{remote_path}", headers=headers)
            sha = None
            if get_resp.status_code == 200:
                sha = get_resp.json().get('sha')
            
            # Read and encode new content
            with local_path.open('rb') as f:
                content = base64.b64encode(f.read()).decode('utf-8')
            
            # Prepare payload
            payload = {
                "message": f"Update {remote_path}",
                "content": content,
                "branch": "main"
            }
            
            # Add SHA if updating existing file
            if sha:
                payload["sha"] = sha
            
            # Push file
            resp = requests.put(
                f"{GITHUB_API_URL}/repos/{GITHUB_USERNAME}/{repo_name}/contents/{remote_path}",
                headers=headers,
                json=payload
            )
            
            if resp.status_code not in {200, 201}:
                error_msg = resp.json().get('message', 'Unknown error')
                logger.error(f"Failed to push {remote_path}: {resp.status_code} - {error_msg}")
                return False
                
            logger.info(f"Successfully pushed {remote_path}")
            
        except Exception as e:
            logger.error(f"Error pushing {remote_path}: {e}")
            return False
    
    logger.info(f"Pushed files to GitHub: https://github.com/{GITHUB_USERNAME}/{repo_name}")
    return True

def setup_ams_api(args: argparse.Namespace) -> bool:
    """Setup AMS API credentials."""
    global AMS_API_TOKEN, AMS_API_HEADERS
    token = args.ams_token or os.environ.get("AMS_API_TOKEN")
    if not token:
        logger.error("AMS API token missing")
        return False
    AMS_API_TOKEN = f"Token {token.strip()}" if not token.startswith("Token ") else token
    AMS_API_HEADERS = {"Authorization": AMS_API_TOKEN, "Content-Type": "application/json"}
    return True

def clean_policy_number(policy_number: str) -> str:
    """Clean policy number by removing zero-width spaces and other special characters."""
    if not policy_number:
        return ""
    # Convert to string and remove zero-width spaces and other special characters
    return str(policy_number).replace('\u200b', '').strip()

async def main():
    args = parse_arguments()
    logger = setup_logging()
    logger.info("Starting Insurance Policy Migration")
    
    # Initialize and setup
    initialize_mappings()
    if not setup_ams_api(args):
        return
    
    # Load and process data
    policies = load_csv_files(logger)
    carriers_map = await fetch_ams_data("carriers", "Carrier", ["name", "carrier_name", "commission"], 
                                      CACHE_DIR / "ams_carriers.csv", logger, not args.no_cache)
    
    valid_policies, invalid_policies = process_policies(policies, carriers_map, logger)
    
    # Get existing policies if needed
    existing_policy_numbers = {}
    if not args.skip_ams_fetch and not args.no_cache:
        existing_policy_numbers = await fetch_ams_data("policies", "Policy", ["policy_number"], 
                                                     CACHE_DIR / "ams_policies.csv", logger, True)
    
    # Debug logging for policy categorization
    logger.info(f"Total valid policies: {len(valid_policies)}")
    logger.info(f"Existing policy numbers in AMS: {len(existing_policy_numbers)}")
    
    # Log sample of valid policies
    if valid_policies:
        logger.info("Sample of valid policies:")
        for p in valid_policies[:5]:
            clean_number = clean_policy_number(p['policy_number'])
            logger.info(f"Policy: {clean_number}, Premium: {p['premium']}, Type: {p['policy_type']}")
    
    # Categorize policies with detailed logging
    now = datetime.now().date()
    new_policies = []
    existing_policies = []
    
    for policy in valid_policies:
        policy_number = clean_policy_number(policy["policy_number"])
        premium = policy["premium"]
        is_existing = policy_number in existing_policy_numbers
        
        logger.debug(f"Processing policy {policy_number}:")
        logger.debug(f"  Premium: {premium}")
        logger.debug(f"  Exists in AMS: {is_existing}")
        
        if not is_existing and premium > 0:
            new_policies.append(policy)
            logger.debug(f"  Added to new_policies")
        elif is_existing:
            existing_policies.append(policy)
            logger.debug(f"  Added to existing_policies")
        else:
            logger.debug(f"  Skipped - premium <= 0")
    
    logger.info(f"New policies: {len(new_policies)}")
    logger.info(f"Existing policies: {len(existing_policies)}")
    
    # Save reports
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, data in [
        ("valid_policies", valid_policies),
        ("invalid_policies", invalid_policies),
        ("new_policies", new_policies),
        ("existing_policies", existing_policies)
    ]:
        pd.DataFrame(data).to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
        logger.info(f"Saved {len(data)} policies to {name}.csv")
    
    # Upload and push to GitHub if needed
    if not args.dry_run:
        await upload_to_ams(new_policies, logger)
    
    if github_token := (args.github_token or GITHUB_TOKEN):
        push_to_github(logger, github_token)
    else:
        logger.info("Skipping GitHub push - no token provided")
    
    logger.info("Migration completed")

if __name__ == "__main__":
    asyncio.run(main())