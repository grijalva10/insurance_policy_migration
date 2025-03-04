"""
Policy processing functionality for insurance policy migration.
Handles policy validation, normalization, and mapping.
"""

import json
import logging
import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

# Configure logger
logger = logging.getLogger('processing')

# Carrier name mappings - these are fallbacks if not in the mapping files
CARRIER_MAPPINGS = {
    'ISC/TCIC': 'TCI Insurance Company',
    'ISC/OSIC': 'Obsidian Specialty Insurance Company',
    'ISC/SSIC': 'Sierra Specialty Ins Co',
    'ISC': 'ISC Insurance',
    'TCIC': 'TCI Insurance Company',
    'OSIC': 'Obsidian Specialty Insurance Company',
    'SSIC': 'Sierra Specialty Ins Co',
    'BTIS': 'BTIS Insurance',
    'BIBERK': 'BiBerk',
    'APPALACHIAN': 'Appalachian Insurance Company',
    'SHIELD': 'Shield',
    'PATHPOINT': 'Pathpoint Insurance',
    'PROPELLER': 'Propeller Insurance',
    'BOLT': 'BOLT Insurance',
    'NEXT': 'Next Insurance',
    'TRAVELERS': 'Travelers Casualty Company'
}

def clean_value(value: str, field_type: str = 'default') -> str:
    """Clean and normalize a value for mapping lookup."""
    if pd.isna(value) or not value:
        return ""
    
    value = str(value).strip()
    
    # Special handling for broker names
    if field_type == 'broker':
        return ' '.join(word.capitalize() for word in value.split())
    
    # Special handling for carriers
    if field_type == 'carrier':
        # Convert to uppercase for consistent matching
        value = value.upper()
        
        # Try to match against carrier mappings
        for pattern, mapped_name in CARRIER_MAPPINGS.items():
            if pattern.upper() in value:
                return mapped_name
        
        # Remove common suffixes for insurance companies
        suffixes = [
            'Insurance Company', 'Insurance Co', 'Insurance', 'Ins Co', 'Ins.',
            'Ins', 'Inc.', 'Inc', 'LLC', 'Corp.', 'Corp', 'Company', 'Co.'
        ]
        for suffix in suffixes:
            if value.endswith(f" {suffix.upper()}"):
                value = value[:-len(suffix)-1]
                break
        
        return value.title()
    
    return value

def clean_policy_number(policy_number: str) -> str:
    """Clean and normalize a policy number."""
    if pd.isna(policy_number) or not policy_number:
        return ""
    
    policy_number = str(policy_number).strip()
    
    # Remove common prefixes
    prefixes = ['Policy #', 'Policy#', 'Policy ', 'Policy: ', '#']
    for prefix in prefixes:
        if policy_number.startswith(prefix):
            policy_number = policy_number[len(prefix):]
            break
    
    return policy_number.strip()

def validate_policy(
    policy: Dict, 
    carriers_map: Dict, 
    non_policy_types: Set[str], 
    non_carrier_entries: Set[str],
    logger: logging.Logger
) -> bool:
    """
    Validate a policy by checking required fields and carrier mapping.
    
    Args:
        policy: Policy dictionary
        carriers_map: Dictionary mapping carrier names to IDs
        non_policy_types: Set of strings that are not valid policy types
        non_carrier_entries: Set of strings that are not valid carrier names
        logger: Logger instance
        
    Returns:
        True if policy is valid, False otherwise
    """
    # Check for required fields
    required_fields = ['insured_name', 'policy_number', 'carrier', 'policy_type']
    for field in required_fields:
        if field not in policy or not policy[field]:
            logger.info(f"Missing required field: {field}")
            return False
    
    # Clean policy number
    policy_number = clean_policy_number(policy['policy_number'])
    if not policy_number:
        logger.info(f"Moving policy with invalid number '{policy['policy_number']}' to invalid list")
        return False
    
    # Check for refund in policy number
    if 'refund' in policy_number.lower():
        logger.info(f"Moving policy {policy_number} to invalid list due to 'refund' in policy number")
        return False
    
    # Check for valid carrier
    carrier = clean_value(policy['carrier'], field_type='carrier')
    carrier_upper = carrier.upper()
    
    # First check exclusions
    if carrier in non_carrier_entries:
        logger.info(f"Moving policy {policy_number} to invalid list due to invalid carrier: '{carrier}'")
        return False
    
    # Then try matching through carrier mappings
    mapped_carrier = None
    if carrier_upper in CARRIER_MAPPINGS:
        mapped_carrier = CARRIER_MAPPINGS[carrier_upper]
        logger.debug(f"Found carrier mapping: {carrier} -> {mapped_carrier}")
    
    # Check if carrier exists in AMS
    carrier_exists = False
    for ams_carrier in carriers_map.values():
        if carrier.lower() == ams_carrier['carrier_name'].lower():
            carrier_exists = True
            break
        if mapped_carrier and mapped_carrier.lower() == ams_carrier['carrier_name'].lower():
            carrier_exists = True
            break
    
    if not carrier_exists:
        logger.debug(f"Carrier not found in AMS: {carrier}")
        # We'll still process it, but it will be flagged for creation
    
    # Check for valid policy type
    policy_type = clean_value(policy['policy_type'])
    if policy_type in non_policy_types:
        logger.info(f"Moving policy {policy_number} to invalid list due to invalid policy type: '{policy_type}'")
        return False
    
    return True

def normalize_policy_fields(
    policy: Dict,
    carriers_map: Dict,
    insureds_map: Dict,
    mappings: Dict[str, Dict[str, str]],
    mapping_manager,
    logger: logging.Logger
) -> Dict:
    """
    Normalize policy fields for AMS upload.
    
    Args:
        policy: Policy dictionary
        carriers_map: Dictionary mapping carrier names to IDs
        insureds_map: Dictionary mapping insured names to IDs
        mappings: Dictionary containing all mappings
        mapping_manager: MappingManager instance for tracking unmapped values
        logger: Logger instance
    
    Returns:
        Normalized policy dictionary
    """
    # Create a copy to avoid modifying the original
    normalized = policy.copy()
    
    # Clean and normalize fields
    if 'policy_number' in normalized:
        normalized['policy_number'] = clean_policy_number(normalized['policy_number'])
    
    if 'carrier' in normalized:
        carrier = clean_value(normalized['carrier'], 'carrier')
        normalized['carrier'] = carrier
        
        # Try to map carrier name
        mapped_carrier = mappings['carrier'].get(carrier)
        if mapped_carrier:
            normalized['carrier'] = mapped_carrier
        else:
            # Track unmapped carrier
            if carrier and not mapping_manager.is_excluded('carrier', carrier):
                mapping_manager.track_unmapped_value('carrier', carrier)
                logger.warning(f"Unmapped carrier: {carrier}")
    
    if 'policy_type' in normalized:
        policy_type = clean_value(normalized['policy_type'])
        normalized['policy_type'] = policy_type
        
        # Try to map policy type
        mapped_policy_type = mappings['policy_type'].get(policy_type)
        if mapped_policy_type:
            normalized['policy_type'] = mapped_policy_type
        else:
            # Track unmapped policy type
            if policy_type and not mapping_manager.is_excluded('policy_type', policy_type):
                mapping_manager.track_unmapped_value('policy_type', policy_type)
                logger.warning(f"Unmapped policy type: {policy_type}")
    
    if 'broker' in normalized:
        broker = clean_value(normalized['broker'], 'broker')
        normalized['broker'] = broker
        
        # Try to map broker name
        mapped_broker = mappings['broker'].get(broker)
        if mapped_broker:
            normalized['broker'] = mapped_broker
        else:
            # Track unmapped broker
            if broker:
                mapping_manager.track_unmapped_value('broker', broker)
                logger.warning(f"Unmapped broker: {broker}")
    
    # Format dates
    date_fields = ['effective_date', 'expiration_date', 'transaction_date']
    for field in date_fields:
        if field in normalized and normalized[field]:
            try:
                date_value = normalized[field]
                if isinstance(date_value, str):
                    # Try different date formats
                    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%d/%m/%Y']:
                        try:
                            date_obj = datetime.strptime(date_value, fmt)
                            normalized[field] = date_obj.strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            continue
            except Exception as e:
                logger.warning(f"Error formatting date {field}: {e}")
    
    return normalized

def process_policies(
    policies: List[Dict],
    carriers_map: Dict,
    insureds_map: Dict,
    mappings: Dict[str, Dict[str, str]],
    mapping_manager,
    output_dir: Path,
    logger: logging.Logger,
    non_policy_types: Set[str] = None,
    non_carrier_entries: Set[str] = None,
    dry_run: bool = False,
    existing_policies: Dict = None
) -> Tuple[List[Dict], List[Dict], Dict]:
    """
    Process policies for AMS upload.
    
    Args:
        policies: List of policy dictionaries
        carriers_map: Dictionary mapping carrier names to IDs
        insureds_map: Dictionary mapping insured names to IDs
        mappings: Dictionary containing all mappings
        mapping_manager: MappingManager instance for tracking unmapped values
        output_dir: Directory for output files
        logger: Logger instance
        non_policy_types: Set of policy types to exclude
        non_carrier_entries: Set of carrier entries to exclude
        dry_run: Whether to run in dry-run mode
        existing_policies: Dictionary of existing policies
    
    Returns:
        Tuple containing valid policies, invalid policies, and statistics
    """
    if non_policy_types is None:
        non_policy_types = set()
    if non_carrier_entries is None:
        non_carrier_entries = set()
    if existing_policies is None:
        existing_policies = {}
    
    valid_policies = []
    invalid_policies = []
    stats = {
        'total': len(policies),
        'valid': 0,
        'invalid': 0,
        'duplicate': 0,
        'unmapped_carriers': set(),
        'unmapped_policy_types': set(),
        'unmapped_brokers': set()
    }
    
    for policy in policies:
        # Validate policy
        if not validate_policy(policy, carriers_map, non_policy_types, non_carrier_entries, logger):
            invalid_policies.append(policy)
            stats['invalid'] += 1
            continue
        
        # Normalize policy fields
        normalized = normalize_policy_fields(policy, carriers_map, insureds_map, mappings, mapping_manager, logger)
        
        # Check for duplicates
        policy_key = f"{normalized.get('policy_number', '')}-{normalized.get('carrier', '')}"
        if policy_key in existing_policies:
            logger.info(f"Skipping duplicate policy: {policy_key}")
            stats['duplicate'] += 1
            continue
        
        # Track unmapped values for statistics
        carrier = normalized.get('carrier', '')
        policy_type = normalized.get('policy_type', '')
        broker = normalized.get('broker', '')
        
        if carrier and carrier not in carriers_map and not mapping_manager.is_excluded('carrier', carrier):
            stats['unmapped_carriers'].add(carrier)
        
        if policy_type and policy_type not in mappings['policy_type'] and not mapping_manager.is_excluded('policy_type', policy_type):
            stats['unmapped_policy_types'].add(policy_type)
        
        if broker and broker not in mappings['broker']:
            stats['unmapped_brokers'].add(broker)
        
        valid_policies.append(normalized)
        stats['valid'] += 1
        existing_policies[policy_key] = True
    
    # Convert sets to lists for JSON serialization
    stats['unmapped_carriers'] = list(stats['unmapped_carriers'])
    stats['unmapped_policy_types'] = list(stats['unmapped_policy_types'])
    stats['unmapped_brokers'] = list(stats['unmapped_brokers'])
    
    # Save statistics
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
        stats_file = output_dir / 'processing_stats.json'
        try:
            with stats_file.open('w') as f:
                json.dump(stats, f, indent=4)
            logger.info(f"Saved processing statistics to {stats_file}")
        except Exception as e:
            logger.error(f"Error saving statistics: {e}")
    
    return valid_policies, invalid_policies, stats 