"""
Main entry point for insurance policy migration.
"""

import os
import json
import logging
import asyncio
from pathlib import Path
from argparse import ArgumentParser

# Fix imports to work both as module and script
if __name__ == "__main__":
    # When run as script
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from src.insurance_migration.ams_client import AMSClient
    from src.insurance_migration.data_loader import load_csv_files
    from src.insurance_migration.policy_processor import process_policies
    from src.insurance_migration.mapping_manager import MappingManager
else:
    # When imported as module
    from .ams_client import AMSClient
    from .data_loader import load_csv_files
    from .policy_processor import process_policies
    from .mapping_manager import MappingManager

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('processing')

# Constants
INPUT_DIR = Path("./data/input/")
CACHE_DIR = Path("./data/cache/")
OUTPUT_DIR = Path("./data/reports/")
MAPPINGS_DIR = Path("./data/mappings/")

def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode")
    parser.add_argument("--no-cache", action="store_true", help="Ignore cached data")
    parser.add_argument("--github-token", help="GitHub token for API access")
    parser.add_argument("--ams-token", help="AMS API token")
    parser.add_argument("--skip-ams-fetch", action="store_true", help="Skip fetching data from AMS")
    return parser.parse_args()

async def main():
    """Main entry point."""
    args = parse_args()
    
    logger.info("Starting Insurance Policy Migration")
    logger.info(f"Dry run mode: {args.dry_run}")
    
    # Ensure directories exist
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Initialize mapping manager
    mapping_manager = MappingManager(MAPPINGS_DIR)
    mappings = mapping_manager.get_mappings()
    non_policy_types, non_carrier_entries = mapping_manager.get_exclusions()
    
    # Initialize AMS client
    ams_client = AMSClient.from_env(CACHE_DIR)
    if not ams_client:
        logger.error("Failed to initialize AMS client")
        return
    
    # Load policies from CSV files
    policies = load_csv_files(INPUT_DIR)
    
    # Fetch carriers and insureds from AMS
    carriers_map = await ams_client.fetch_data(
        "carriers", ["name", "carrier_name", "commission"], "ams_carriers.csv",
        not args.no_cache
    )
    
    insureds_map = await ams_client.fetch_data(
        "insureds", ["name", "insured_name", "email"], "ams_insureds.csv",
        not args.no_cache
    )
    
    # Fetch existing policies from AMS to avoid duplicates
    existing_policies_map = {}
    if not args.skip_ams_fetch:
        existing_policies_map = await ams_client.fetch_data(
            "policies", ["name", "policy_number", "insured"], "ams_policies.csv",
            not args.no_cache
        )
        logger.info(f"Fetched {len(existing_policies_map)} existing policies from AMS")
    
    # Process policies
    valid_policies, invalid_policies, results = process_policies(
        policies,
        carriers_map,
        insureds_map,
        mappings,
        OUTPUT_DIR,
        logger,
        non_policy_types=non_policy_types,
        non_carrier_entries=non_carrier_entries,
        dry_run=args.dry_run,
        existing_policies=existing_policies_map
    )
    
    # Track unmapped values
    for policy in invalid_policies:
        if 'carrier' in policy and policy['carrier'] and not mapping_manager.get_mapped_value('carrier', policy['carrier']):
            mapping_manager.track_unmapped_value('carrier', policy['carrier'])
        
        if 'policy_type' in policy and policy['policy_type'] and not mapping_manager.get_mapped_value('policy_type', policy['policy_type']):
            mapping_manager.track_unmapped_value('policy_type', policy['policy_type'])
    
    # Save results
    logger.info(f"Processed {len(policies)} policies")
    logger.info(f"Valid policies: {len(valid_policies)}")
    logger.info(f"Invalid policies: {len(invalid_policies)}")
    logger.info(f"Successful uploads: {results['success']}")
    logger.info(f"Failed uploads: {results['failed']}")
    
    # Save reports
    valid_df = load_csv_files.policies_to_dataframe(valid_policies)
    invalid_df = load_csv_files.policies_to_dataframe(invalid_policies)
    
    valid_df.to_csv(OUTPUT_DIR / "valid_policies.csv", index=False)
    invalid_df.to_csv(OUTPUT_DIR / "invalid_policies.csv", index=False)
    
    logger.info("Insurance Policy Migration completed")

if __name__ == "__main__":
    asyncio.run(main()) 