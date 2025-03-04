"""
Main entry point for insurance policy migration.
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional

from .ams_client import AMSClient
from .data_loader import load_csv_files
from .policy_processor import process_policies
from .mapping_manager import MappingManager
from .github_sync import GitHubSync

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('processing')

async def main():
    """Main entry point."""
    try:
        # Initialize paths
        project_root = Path(__file__).parent.parent.parent
        data_dir = project_root / 'data'
        input_dir = data_dir / 'input'
        mappings_dir = data_dir / 'mappings'
        output_dir = data_dir / 'reports'
        cache_dir = data_dir / 'cache'
        
        # Create directories if they don't exist
        for dir_path in [input_dir, mappings_dir, output_dir, cache_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize mapping manager
        mapping_manager = MappingManager(mappings_dir)
        mappings = mapping_manager.get_mappings()
        non_policy_types, non_carrier_entries = mapping_manager.get_exclusions()
        
        # Load policies from CSV files
        policies = load_csv_files(input_dir, logger)
        logger.info(f"Total policies loaded: {len(policies)}")
        
        # Initialize AMS client
        ams_client = AMSClient(cache_dir)
        
        # Load data from AMS
        carriers_map = await ams_client.get_carriers()
        insureds_map = await ams_client.get_insureds()
        existing_policies = await ams_client.get_policies()
        logger.info(f"Fetched {len(existing_policies)} existing policies from AMS")
        
        # Process policies
        valid_policies, invalid_policies, results = process_policies(
            policies=policies,
            carriers_map=carriers_map,
            insureds_map=insureds_map,
            mappings=mappings,
            mapping_manager=mapping_manager,
            output_dir=output_dir,
            logger=logger,
            non_policy_types=non_policy_types,
            non_carrier_entries=non_carrier_entries,
            existing_policies=existing_policies
        )
        
        # Upload valid policies to AMS
        if valid_policies:
            logger.info(f"Uploading {len(valid_policies)} valid policies to AMS")
            await ams_client.upload_policies(valid_policies)
        
        # Push to GitHub to preserve mapping files
        github_sync = GitHubSync.from_env()
        if github_sync:
            logger.info("Pushing to GitHub to preserve mapping files...")
            if github_sync.push_to_github(project_root):
                logger.info("Successfully pushed to GitHub")
            else:
                logger.error("Failed to push to GitHub")
        
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Error during migration: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    asyncio.run(main()) 