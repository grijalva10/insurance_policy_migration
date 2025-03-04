#!/usr/bin/env python3
"""
Recover mapping data from logs and code.
This script extracts mapping data from the policy_upload_log.txt file and the hardcoded mappings in the code.
"""

import json
import re
import logging
from pathlib import Path
from typing import Dict, List, Set

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('recover_mappings')

# Constants
LOG_FILE = Path('./policy_upload_log.txt')
MAPPINGS_DIR = Path('./data/mappings')
CARRIER_MAPPINGS_FILE = MAPPINGS_DIR / 'carrier_mapping.json'
BROKER_MAPPINGS_FILE = MAPPINGS_DIR / 'broker_mapping.json'
POLICY_TYPE_MAPPINGS_FILE = MAPPINGS_DIR / 'policy_type_mapping.json'
UNMATCHED_VALUES_FILE = MAPPINGS_DIR / 'unmatched_values.json'
EXCLUSION_MAPPING_FILE = MAPPINGS_DIR / 'exclusion_mapping.json'

# Hardcoded mappings from the code
HARDCODED_CARRIER_MAPPINGS = {
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

HARDCODED_POLICY_TYPE_MAPPINGS = {
    'GL': 'General Liability',
    'WC': 'Workers Compensation',
    'Auto': 'Commercial Auto',
    'Property': 'Commercial Property',
    'Umbrella': 'Commercial Umbrella',
    'Bond': 'Surety Bond',
    'Equipment': 'Inland Marine',
    'Excess': 'Excess Liability'
}

def extract_mappings_from_log() -> Dict[str, Dict[str, str]]:
    """Extract mapping data from the log file."""
    if not LOG_FILE.exists():
        logger.error(f"Log file {LOG_FILE} not found")
        return {
            'broker': {},
            'carrier': {},
            'policy_type': {}
        }
    
    logger.info(f"Extracting mappings from {LOG_FILE}")
    
    # Initialize mappings
    mappings = {
        'broker': {},
        'carrier': {},
        'policy_type': {}
    }
    
    # Extract carrier mappings from log
    carrier_pattern = re.compile(r"Found carrier mapping: (.*?) -> (.*?)$")
    
    with LOG_FILE.open('r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Extract carrier mappings
            carrier_match = carrier_pattern.search(line)
            if carrier_match:
                source = carrier_match.group(1).strip()
                target = carrier_match.group(2).strip()
                mappings['carrier'][source] = target
    
    logger.info(f"Extracted {len(mappings['carrier'])} carrier mappings from log")
    
    return mappings

def extract_mappings_from_unmatched_values() -> Dict[str, List[str]]:
    """Extract unmapped values from the unmatched_values.json file."""
    if not UNMATCHED_VALUES_FILE.exists():
        logger.error(f"Unmatched values file {UNMATCHED_VALUES_FILE} not found")
        return {
            'brokers': [],
            'carriers': [],
            'policy_types': []
        }
    
    logger.info(f"Extracting unmapped values from {UNMATCHED_VALUES_FILE}")
    
    try:
        with UNMATCHED_VALUES_FILE.open('r') as f:
            unmatched_values = json.load(f)
        
        logger.info(f"Extracted {len(unmatched_values.get('carriers', []))} unmapped carriers, "
                   f"{len(unmatched_values.get('policy_types', []))} unmapped policy types, "
                   f"{len(unmatched_values.get('brokers', []))} unmapped brokers")
        
        return unmatched_values
    except Exception as e:
        logger.error(f"Error extracting unmapped values: {e}")
        return {
            'brokers': [],
            'carriers': [],
            'policy_types': []
        }

def extract_exclusions() -> Dict[str, List[str]]:
    """Extract exclusions from the exclusion_mapping.json file."""
    if not EXCLUSION_MAPPING_FILE.exists():
        logger.error(f"Exclusion mapping file {EXCLUSION_MAPPING_FILE} not found")
        return {
            'non_policy_types': [],
            'non_carrier_entries': []
        }
    
    logger.info(f"Extracting exclusions from {EXCLUSION_MAPPING_FILE}")
    
    try:
        with EXCLUSION_MAPPING_FILE.open('r') as f:
            exclusions = json.load(f)
        
        logger.info(f"Extracted {len(exclusions.get('non_policy_types', []))} non-policy types, "
                   f"{len(exclusions.get('non_carrier_entries', []))} non-carrier entries")
        
        return exclusions
    except Exception as e:
        logger.error(f"Error extracting exclusions: {e}")
        return {
            'non_policy_types': [],
            'non_carrier_entries': []
        }

def merge_mappings(log_mappings: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """Merge mappings from different sources."""
    merged_mappings = {
        'broker': {},
        'carrier': {},
        'policy_type': {}
    }
    
    # Add hardcoded carrier mappings
    for source, target in HARDCODED_CARRIER_MAPPINGS.items():
        merged_mappings['carrier'][source] = target
    
    # Add hardcoded policy type mappings
    for source, target in HARDCODED_POLICY_TYPE_MAPPINGS.items():
        merged_mappings['policy_type'][source] = target
    
    # Add mappings from log
    for mapping_type, mappings in log_mappings.items():
        for source, target in mappings.items():
            if source and target:
                merged_mappings[mapping_type][source] = target
    
    # Add common policy type mappings
    common_policy_types = {
        'General Liability': 'General Liability',
        'GL': 'General Liability',
        'Workers Compensation': 'Workers Compensation',
        'Workers Comp': 'Workers Compensation',
        'WC': 'Workers Compensation',
        'Commercial Auto': 'Commercial Auto',
        'Auto': 'Commercial Auto',
        'Commercial Property': 'Commercial Property',
        'Property': 'Commercial Property',
        'Excess': 'Excess',
        'Excess Liability': 'Excess',
        'Umbrella': 'Excess',
        'Professional Liability': 'Professional Liability',
        'E&O': 'Professional Liability',
        'Errors and Omissions': 'Professional Liability',
        'Inland Marine': 'Inland Marine',
        'Equipment': 'Inland Marine',
        'Bond': 'Bond',
        'Surety Bond': 'Bond',
        'Builders Risk': 'Builders Risk',
        'Pollution Liability': 'Pollution Liability',
        'Endorsement': 'Endorsement'
    }
    
    for source, target in common_policy_types.items():
        if source not in merged_mappings['policy_type']:
            merged_mappings['policy_type'][source] = target
    
    logger.info(f"Merged mappings: {len(merged_mappings['broker'])} brokers, "
               f"{len(merged_mappings['carrier'])} carriers, "
               f"{len(merged_mappings['policy_type'])} policy types")
    
    return merged_mappings

def save_mappings(mappings: Dict[str, Dict[str, str]]) -> None:
    """Save mappings to files."""
    MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save carrier mappings
    with CARRIER_MAPPINGS_FILE.open('w') as f:
        json.dump(mappings['carrier'], f, indent=4)
    logger.info(f"Saved {len(mappings['carrier'])} carrier mappings to {CARRIER_MAPPINGS_FILE}")
    
    # Save broker mappings
    with BROKER_MAPPINGS_FILE.open('w') as f:
        json.dump(mappings['broker'], f, indent=4)
    logger.info(f"Saved {len(mappings['broker'])} broker mappings to {BROKER_MAPPINGS_FILE}")
    
    # Save policy type mappings
    with POLICY_TYPE_MAPPINGS_FILE.open('w') as f:
        json.dump(mappings['policy_type'], f, indent=4)
    logger.info(f"Saved {len(mappings['policy_type'])} policy type mappings to {POLICY_TYPE_MAPPINGS_FILE}")

def main():
    """Main function."""
    logger.info("Starting mapping recovery")
    
    # Extract mappings from log
    log_mappings = extract_mappings_from_log()
    
    # Extract unmapped values
    unmatched_values = extract_mappings_from_unmatched_values()
    
    # Extract exclusions
    exclusions = extract_exclusions()
    
    # Merge mappings
    merged_mappings = merge_mappings(log_mappings)
    
    # Save mappings
    save_mappings(merged_mappings)
    
    logger.info("Mapping recovery completed")

if __name__ == "__main__":
    main() 