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

# Constants
INPUT_DIR = Path("./data/input/")
OUTPUT_DIR = Path("./data/reports/")
CACHE_DIR = Path("./data/cache/")
MAPPINGS_DIR = Path("./data/mappings/")
LOG_FILE = Path("policy_upload_log.txt")
AMS_API_URL = "https://ams.jmggo.com/api/method"
GITHUB_API_URL = "https://api.github.com"
GITHUB_USERNAME = "grijalva10"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# Global mappings
BROKER_MAPPING = None
CARRIER_MAPPING = None
POLICY_TYPE_MAPPING = None
NON_POLICY_TYPES = None
NON_CARRIER_ENTRIES = None

def load_mappings() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Set[str], Set[str]]:
    """Load mapping files and exclusion sets."""
    mapping_files = {
        'broker': 'broker_mapping.json',
        'carrier': 'carrier_mapping.json',
        'policy_type': 'policy_type_mapping.json'
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
        
        # Static exclusion sets (since exclusion_mapping.json isn't provided)
        non_policy_types = {
            "2nd Payment", "2nd payment", "3rd payment", "Additional Broker Fee", "Additional Premium",
            "Audit Payment", "Broker Fee", "Declined", "Full Refund", "Full refund", "GL 2nd Payment",
            "GL 2nd Paymnet", "GL Monthly Payment", "GL+Excess 2nd payment", "Monthly Payment",
            "Partial refund", "Payment Declined", "Payment disputed", "Payment to carrier", "Redunded",
            "Refund", "Refunded", "VOIDED", "Voided", "new GL 2nd payment", "October Installment",
            "Payment to Carrier", "Second Payment", "Second payment"
        }
        non_carrier_entries = {
            "2nd Payment", "2nd payment", "3rd payment", "Additional Broker Fee", "Additional Premium",
            "Audit Payment", "Broker Fee", "Declined", "Full Refund", "Full refund", "Monthly Payment",
            "Monthly payment", "October Installment", "Partial refund", "Payment Declined",
            "Payment disputed", "Payment to Carrier", "Payment to carrier", "Refund", "Refunded",
            "Second Payment", "Second payment", "VOIDED", "Voided"
        }
        
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
    file_handler = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
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
    return re.sub(r'[^a-z0-9_]', '_', str(col).lower().strip())

def load_csv_files(logger: logging.Logger) -> List[Dict]:
    """Load CSV files into policy dictionaries with premium parsing."""
    policies = []
    csv_files = list(INPUT_DIR.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {INPUT_DIR}")
        return policies
    
    logger.info(f"Processing {len(csv_files)} CSV files")
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            df.columns = [col.strip() for col in df.columns]
            
            # Log all columns for debugging
            logger.debug(f"Columns in {file_path.name}: {', '.join(df.columns)}")
            
            # Create normalized column map
            column_map = {normalize_column_name(col): col for col in df.columns}
            logger.debug(f"Normalized column map: {column_map}")
            
            required = {'policy_number': ['policy number', 'policy_number', 'policy', 'policy_no', 'policy_no_']}
            optional = {
                'effective_date': ['date', 'effective_date', 'effective date', 'start date', 'policy date'],
                'broker_fee': ['broker fee', 'broker_fee', 'brokerfee', 'broker_fee_amount'],
                'commission': ['commission', 'commission_amount', 'comm'],
                'broker': ['agent', 'broker', 'agent_name', 'broker_name'],
                'policy_type': ['policy type', 'policy_type', 'type', 'policy_category'],
                'carrier': ['carrier', 'carrier_name', 'insurance_company'],
                'premium': ['charge amount', 'premium', 'amount', 'policy_amount', 'total_premium', 'premium_amount']
            }
            
            missing_required = [field for field, variations in required.items() if not any(var in column_map for var in variations)]
            if missing_required:
                logger.error(f"Missing required columns in {file_path.name}: {missing_required}")
                continue
            
            mapped_df = pd.DataFrame()
            for field, variations in {**required, **optional}.items():
                for var in variations:
                    normalized_var = normalize_column_name(var)
                    if normalized_var in column_map:
                        # Special handling for premium (Charge Amount)
                        if field == 'premium':
                            raw_values = df[column_map[normalized_var]]
                            mapped_df[field] = raw_values.apply(parse_currency)
                            # Log raw and parsed values for debugging
                            for idx, (raw, parsed) in enumerate(zip(raw_values, mapped_df[field])):
                                logger.debug(f"Policy {idx+1}: Raw {column_map[normalized_var]}='{raw}', Parsed premium={parsed}")
                        else:
                            mapped_df[field] = df[column_map[normalized_var]]
                        break
            
            # Parse dates
            if 'effective_date' in mapped_df:
                mapped_df['effective_date'] = mapped_df['effective_date'].apply(parse_date)
                mapped_df['expiration_date'] = mapped_df['effective_date'].apply(
                    lambda x: (datetime.strptime(x, '%Y-%m-%d') + relativedelta(years=1)).strftime('%Y-%m-%d') if x else None
                )
                mapped_df = mapped_df.dropna(subset=['effective_date'])
            
            # Parse other currency fields
            for col in ['broker_fee', 'commission']:
                if col in mapped_df:
                    raw_values = mapped_df[col]
                    mapped_df[f"{col}_amount"] = raw_values.apply(parse_currency)
                    # Log currency parsing for debugging
                    for idx, (raw, parsed) in enumerate(zip(raw_values, mapped_df[f"{col}_amount"])):
                        logger.debug(f"Policy {idx+1}: Raw {col}='{raw}', Parsed {col}_amount={parsed}")
            
            file_policies = mapped_df.to_dict('records')
            for policy in file_policies:
                policy['source_file'] = file_path.name
                # Log complete policy data for debugging
                logger.debug(f"Loaded policy from {file_path.name}:")
                logger.debug(f"  Policy Number: {policy.get('policy_number', 'unknown')}")
                logger.debug(f"  Premium: {policy.get('premium', 'N/A')}")
                logger.debug(f"  Broker Fee: {policy.get('broker_fee_amount', 'N/A')}")
                logger.debug(f"  Commission: {policy.get('commission_amount', 'N/A')}")
            
            policies.extend(file_policies)
            logger.info(f"Loaded {len(file_policies)} policies from {file_path.name}")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            logger.exception("Detailed error:")
    
    logger.info(f"Loaded {len(policies)} total policies")
    return policies

async def fetch_ams_data(endpoint: str, doctype: str, fields: List[str], cache_file: Path, logger: logging.Logger, use_cache: bool) -> Dict:
    """Fetch AMS data asynchronously with pagination and caching."""
    if use_cache and cache_file.exists():
        try:
            df = pd.read_csv(cache_file)
            key_field = fields[0] if len(fields) == 1 else fields[1]
            result = {str(row[key_field]).lower(): {k: row.get(k, 0.0) for k in fields} for _, row in df.iterrows() if pd.notna(row[key_field])}
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
    
    key_field = fields[0] if len(fields) == 1 else fields[1]
    return {str(item[key_field]).lower(): {k: item.get(k, 0.0) for k in fields} for item in all_items if item.get(key_field)}

def clean_value(value: str, field_type: str = 'default') -> str:
    """Clean and normalize a value for mapping lookup."""
    if pd.isna(value) or not value:
        return ""
    
    value = str(value).strip()
    
    # Special handling for broker names
    if field_type == 'broker':
        return ' '.join(word.capitalize() for word in value.split())
    
    # Remove common suffixes for insurance companies
    suffixes = [
        'Insurance Company', 'Insurance Co', 'Insurance', 'Ins Co', 'Ins.',
        'Inc.', 'Corporation', 'Corp.', 'Limited', 'Ltd.'
    ]
    for suffix in suffixes:
        if value.lower().endswith(suffix.lower()):
            value = value[:-len(suffix)].strip()
    
    # Remove text after certain characters
    value = re.sub(r'\s*/\s*.*$', '', value)  # Remove everything after /
    value = re.sub(r'^\s*.*?\s*/\s*', '', value)  # Remove everything before /
    value = re.sub(r'\s*\(.*?\)', '', value)  # Remove parentheses and contents
    value = re.sub(r'\s*,\s*.*$', '', value)  # Remove everything after comma
    
    # Clean up remaining text
    value = re.sub(r'^[\s\-_\.]+|[\s\-_\.]+$', '', value)  # Remove leading/trailing special chars
    value = value.replace('&', 'and').replace('+', 'and')
    value = ' '.join(word.capitalize() for word in value.split())  # Consistent capitalization
    value = re.sub(r'\s+', ' ', value).strip()
    
    # Special cases for carriers
    if field_type == 'carrier':
        value = value.replace('The ', '').strip()  # Remove leading "The"
        
        # Map common abbreviations
        abbreviations = {
            'Natl': 'National',
            'Intl': 'International',
            'Amer': 'American',
            'Gen': 'General',
            'Corp': 'Corporation',
            'Ins': 'Insurance'
        }
        for abbr, full in abbreviations.items():
            value = re.sub(rf'\b{abbr}\b', full, value, flags=re.IGNORECASE)
    
    return value

def clean_policy_number(policy_number: str) -> str:
    """Clean policy number by removing zero-width spaces and other special characters."""
    if not policy_number:
        return ""
    # Remove zero-width spaces and other invisible characters
    cleaned = ''.join(char for char in str(policy_number) if char.isprintable() and ord(char) >= 32)
    # Remove any remaining whitespace
    cleaned = cleaned.strip()
    return cleaned

def validate_policy(policy: Dict, carriers_map: Dict, logger: logging.Logger) -> bool:
    """Validate a single policy."""
    policy_number = clean_policy_number(policy.get('policy_number', ''))
    if not policy_number or policy_number.lower() in {'nan', 'none', 'null', 'refunded', 'voided', 'audit'} or 'refund' in policy_number.lower():
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
    # Log initial values for debugging
    policy_number = clean_policy_number(policy.get('policy_number', ''))
    logger.debug(f"Normalizing policy {policy_number}:")
    logger.debug(f"  Initial premium: {policy.get('premium', 0.0)}")
    logger.debug(f"  Initial broker fee: {policy.get('broker_fee_amount', 0.0)}")
    logger.debug(f"  Initial commission: {policy.get('commission_amount', 0.0)}")
    
    # Clean and map carrier
    carrier = clean_value(policy.get('carrier', ''))
    policy['carrier'] = CARRIER_MAPPING.get(carrier, carrier)
    logger.debug(f"  Mapped carrier: {carrier} -> {policy['carrier']}")
    
    # Handle policy type mapping
    policy_type = clean_value(policy.get('policy_type', ''))
    if any(endors in policy_number.lower() for endors in ['endorsement', 'endors']):
        policy['policy_type'] = 'Endorsement'
    else:
        policy['policy_type'] = POLICY_TYPE_MAPPING.get(policy_type, 'Other')
    logger.debug(f"  Mapped policy type: {policy_type} -> {policy['policy_type']}")
    
    # Map broker
    broker = clean_value(policy.get('broker', ''), field_type='broker')
    policy['broker_email'] = BROKER_MAPPING.get(broker, None)
    policy['broker'] = policy['broker_email']
    logger.debug(f"  Mapped broker: {broker} -> {policy['broker_email']}")
    
    # Handle dates
    effective_date = datetime.strptime(policy['effective_date'], '%Y-%m-%d').date()
    expiration_date = datetime.strptime(policy['expiration_date'], '%Y-%m-%d').date()
    policy['effective_date'] = effective_date.strftime('%Y-%m-%d')
    policy['expiration_date'] = expiration_date.strftime('%Y-%m-%d')
    policy['status'] = 'Active' if expiration_date > datetime.now().date() else 'Expired'
    
    # Handle financial fields
    # Use the premium value that was already parsed in load_csv_files
    premium = policy.get('premium', 0.0)
    broker_fee = policy.get('broker_fee_amount', 0.0)
    
    # Get carrier commission rate and calculate commission
    carrier_commission = carriers_map.get(policy['carrier'], {}).get('commission', 0.0)
    commission = policy.get('commission_amount', 0.0)
    
    # If commission is not already set, calculate it from premium and carrier rate
    if not commission and premium > 0 and carrier_commission > 0:
        commission = premium * (carrier_commission / 100.0)
    
    policy['premium'] = premium
    policy['broker_fee_amount'] = broker_fee
    policy['commission_amount'] = commission
    
    # Log final values for debugging
    logger.debug(f"  Final premium: {policy['premium']}")
    logger.debug(f"  Final broker fee: {policy['broker_fee_amount']}")
    logger.debug(f"  Final commission: {policy['commission_amount']}")
    logger.debug(f"  Commission rate: {carrier_commission}%")
    
    return policy

def process_policies(policies: List[Dict], carriers_map: Dict, logger: logging.Logger) -> Tuple[List[Dict], List[Dict]]:
    """Process policies in batches with mappings."""
    valid_policies, invalid_policies = [], []
    unmapped = {'policy_types': set(), 'carriers': set(), 'brokers': set()}
    
    batch_size = 1000
    for i in range(0, len(policies), batch_size):
        batch = policies[i:i + batch_size]
        logger.debug(f"Processing batch {i // batch_size + 1} ({len(batch)} policies)")
        
        for policy in batch:
            if not validate_policy(policy, carriers_map, logger):
                invalid_policies.append(policy)
                continue
            
            try:
                normalized = normalize_policy_fields(policy, carriers_map, logger)
                if normalized['policy_type'] == 'Other':
                    unmapped['policy_types'].add(clean_value(policy.get('policy_type', '')))
                if normalized['carrier'] not in carriers_map:
                    unmapped['carriers'].add(clean_value(policy.get('carrier', '')))
                if not normalized['broker_email']:
                    unmapped['brokers'].add(clean_value(policy.get('broker', ''), 'broker'))
                valid_policies.append(normalized)
            except Exception as e:
                logger.error(f"Error normalizing policy {clean_policy_number(policy.get('policy_number', 'unknown'))}: {e}")
                invalid_policies.append(policy)
    
    # Save unmapped values to JSON file
    unmapped_file = MAPPINGS_DIR / 'unmatched_values.json'
    try:
        if unmapped_file.exists():
            with unmapped_file.open('r') as f:
                existing_unmapped = json.load(f)
        else:
            existing_unmapped = {'carrier': [], 'policy_type': [], 'broker': []}
        
        # Update with new unmapped values
        for key in unmapped:
            if key not in existing_unmapped:
                existing_unmapped[key] = []
            existing_unmapped[key] = sorted(list(set(existing_unmapped[key] + list(unmapped[key]))))
        
        # Save updated unmapped values
        MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)
        with unmapped_file.open('w') as f:
            json.dump(existing_unmapped, f, indent=4)
        logger.info(f"Updated unmapped values in {unmapped_file}")
    except Exception as e:
        logger.error(f"Failed to save unmapped values: {e}")
    
    for key, items in unmapped.items():
        if items:
            logger.warning(f"Unmapped {key}: {', '.join(sorted(str(i) for i in items))}")
    
    logger.info(f"Processed {len(valid_policies)} valid, {len(invalid_policies)} invalid policies")
    return valid_policies, invalid_policies

async def upload_to_ams(policies: List[Dict], logger: logging.Logger) -> int:
    """Upload policies asynchronously."""
    async def upload_one(session: aiohttp.ClientSession, policy: Dict) -> bool:
        # Ensure broker_email is not empty
        if not policy.get("broker_email"):
            policy["broker_email"] = "default@example.com"
            logger.warning(f"Using default broker email for policy {policy['policy_number']}")
        
        # Ensure insured is not empty
        if not policy.get("insured"):
            policy["insured"] = policy.get("insured_name", "Unknown Insured")
            logger.warning(f"Using fallback insured name for policy {policy['policy_number']}")
        
        payload = {
            "doctype": "Policy", 
            "policy_number": policy["policy_number"],
            "effective_date": policy["effective_date"], 
            "expiration_date": policy["expiration_date"],
            "status": policy["status"], 
            "premium": policy["premium"], 
            "broker": policy["broker_email"],
            "policy_type": policy["policy_type"], 
            "carrier": policy["carrier"],
            "commission_amount": policy["commission_amount"], 
            "broker_fee": policy["broker_fee_amount"],
            "insured": policy["insured"]  # Add insured field to payload
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
    
    resp = requests.get(f"{GITHUB_API_URL}/repos/{GITHUB_USERNAME}/{repo_name}", headers=headers)
    if resp.status_code == 404:
        requests.post(f"{GITHUB_API_URL}/user/repos", headers=headers, json={"name": repo_name, "private": False})
    
    for remote_path, local_path in files.items():
        if not local_path.exists():
            logger.debug(f"Skipping {local_path} (not found)")
            continue
        with local_path.open('rb') as f:
            content = base64.b64encode(f.read()).decode('utf-8')
        payload = {"message": f"Update {remote_path}", "content": content, "branch": "main"}
        resp = requests.put(f"{GITHUB_API_URL}/repos/{GITHUB_USERNAME}/{repo_name}/contents/{remote_path}", headers=headers, json=payload)
        if resp.status_code not in {200, 201}:
            logger.error(f"Failed to push {remote_path}: {resp.status_code}")
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

async def main():
    args = parse_arguments()
    logger = setup_logging()
    logger.info("Starting Insurance Policy Migration")
    
    initialize_mappings()
    if not setup_ams_api(args):
        return
    
    policies = load_csv_files(logger)
    carriers_map = await fetch_ams_data("carriers", "Carrier", ["name", "carrier_name", "commission"], CACHE_DIR / "ams_carriers.csv", logger, not args.no_cache)
    
    valid_policies, invalid_policies = process_policies(policies, carriers_map, logger)
    existing_policy_numbers = await fetch_ams_data("policies", "Policy", ["policy_number"], CACHE_DIR / "ams_policies.csv", logger, not args.skip_ams_fetch and not args.no_cache) if not args.skip_ams_fetch else {}
    
    now = datetime.now().date()
    new_policies = [p for p in valid_policies if p["policy_number"] not in existing_policy_numbers and p["premium"] > 0 and datetime.strptime(p["expiration_date"], '%Y-%m-%d').date() > now]
    existing_policies = [p for p in valid_policies if p["policy_number"] in existing_policy_numbers]
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, data in [("valid_policies", valid_policies), ("invalid_policies", invalid_policies), ("new_policies", new_policies), ("existing_policies", existing_policies)]:
        pd.DataFrame(data).to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
        logger.info(f"Saved {len(data)} policies to {name}.csv")
    
    if not args.dry_run:
        await upload_to_ams(new_policies, logger)
    
    if github_token := (args.github_token or GITHUB_TOKEN):
        push_to_github(logger, github_token)
    
    logger.info("Migration completed")

if __name__ == "__main__":
    asyncio.run(main())