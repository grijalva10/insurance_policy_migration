#!/usr/bin/env python3
"""
Run the insurance policy migration.
This is a simple entry point script that sets up the environment and runs the migration.
"""

import os
import asyncio
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path('./logs/policy_upload_log.txt')),
        logging.StreamHandler()
    ]
)

# Ensure logs directory exists
Path('./logs').mkdir(exist_ok=True)

# Set environment variables if not already set
if not os.environ.get('AMS_API_URL'):
    os.environ['AMS_API_URL'] = 'https://ams.jmggo.com/api/method'

if not os.environ.get('AMS_API_TOKEN'):
    os.environ['AMS_API_TOKEN'] = 'Token 0bee14763d4aa5f:1853fd79a3c25f9'

# Import and run the main function
try:
    from src.insurance_migration.__main__ import main
    
    if __name__ == '__main__':
        asyncio.run(main())
except ImportError:
    logging.error("Failed to import the insurance_migration package. Make sure it's installed or in the Python path.")
    raise 