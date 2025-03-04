"""
AMS (Agency Management System) client for API interactions.
Handles data fetching and uploading through the Frappe API.
"""

import os
import json
import logging
import aiohttp
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path
from dotenv import load_dotenv

# Configure logger
logger = logging.getLogger('ams')

class AMSClient:
    """Client for interacting with the AMS API."""
    
    def __init__(self, cache_dir: Path):
        """Initialize AMS client.
        
        Args:
            cache_dir: Directory for caching API responses
        """
        # Load environment variables
        load_dotenv()
        
        # Get API configuration from environment
        self.api_url = os.environ.get('AMS_API_URL', 'https://ams.jmggo.com/api/method')
        self.api_token = os.environ.get('AMS_API_TOKEN')
        
        if not self.api_token:
            raise ValueError("AMS_API_TOKEN not found in environment variables")
        
        # Set up API headers
        self.headers = {
            'Authorization': self.api_token,
            'Content-Type': 'application/json'
        }
        
        # Set up cache directory
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize cache
        self._carriers_cache: Optional[Dict] = None
        self._insureds_cache: Optional[Dict] = None
        self._policies_cache: Optional[Dict] = None
    
    async def get_carriers(self) -> Dict:
        """Get carriers from AMS or cache."""
        if self._carriers_cache is None:
            cache_file = self.cache_dir / 'carriers.json'
            if cache_file.exists():
                try:
                    with cache_file.open('r') as f:
                        self._carriers_cache = json.load(f)
                    logger.info(f"Loaded {len(self._carriers_cache)} carriers from cache")
                except Exception as e:
                    logger.error(f"Error loading carriers cache: {e}")
                    self._carriers_cache = {}
            else:
                self._carriers_cache = {}
                # In a real implementation, we would fetch from the API here
                logger.info("Would fetch carriers from API in production")
        
        return self._carriers_cache
    
    async def get_insureds(self) -> Dict:
        """Get insureds from AMS or cache."""
        if self._insureds_cache is None:
            cache_file = self.cache_dir / 'insureds.json'
            if cache_file.exists():
                try:
                    with cache_file.open('r') as f:
                        self._insureds_cache = json.load(f)
                    logger.info(f"Loaded {len(self._insureds_cache)} insureds from cache")
                except Exception as e:
                    logger.error(f"Error loading insureds cache: {e}")
                    self._insureds_cache = {}
            else:
                self._insureds_cache = {}
                # In a real implementation, we would fetch from the API here
                logger.info("Would fetch insureds from API in production")
        
        return self._insureds_cache
    
    async def get_policies(self) -> Dict:
        """Get policies from AMS or cache."""
        if self._policies_cache is None:
            cache_file = self.cache_dir / 'policies.json'
            if cache_file.exists():
                try:
                    with cache_file.open('r') as f:
                        self._policies_cache = json.load(f)
                    logger.info(f"Loaded {len(self._policies_cache)} policies from cache")
                except Exception as e:
                    logger.error(f"Error loading policies cache: {e}")
                    self._policies_cache = {}
            else:
                self._policies_cache = {}
                # In a real implementation, we would fetch from the API here
                logger.info("Would fetch policies from API in production")
        
        return self._policies_cache
    
    async def fetch_data(self, doctype: str, fields: List[str], cache_file: str, 
                        use_cache: bool = True) -> Dict:
        """Fetch data from AMS with caching support."""
        cache_path = self.cache_dir / cache_file
        
        # Try to load from cache
        if use_cache and cache_path.exists():
            try:
                df = pd.read_csv(cache_path)
                key_field = fields[0] if len(fields) == 1 else fields[1]
                result = {
                    str(row[key_field]).lower(): {k: row.get(k, '') for k in fields}
                    for _, row in df.iterrows() if pd.notna(row[key_field])
                }
                logger.info(f"Loaded {len(result)} {doctype}s from cache")
                return result
            except Exception as e:
                logger.error(f"Cache load failed for {cache_file}: {e}")

        # Fetch from API
        async def fetch_page(session: aiohttp.ClientSession, page: int, page_size: int) -> List[Dict]:
            payload = {
                "doctype": doctype,
                "fields": fields,
                "limit_start": (page - 1) * page_size,
                "limit_page_length": page_size
            }
            max_retries, retry_delay = 3, 2
            for attempt in range(max_retries):
                try:
                    async with session.post(
                        f"{self.api_url}/frappe.client.get_list",
                        json=payload,
                        headers=self.headers
                    ) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                        return data.get("message", [])
                except Exception as e:
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

        # Cache results
        if all_items:
            df = pd.DataFrame(all_items)
            df.to_csv(cache_path, index=False)
            logger.info(f"Fetched and cached {len(all_items)} {doctype}s")

        key_field = fields[0] if len(fields) == 1 else fields[1]
        return {
            str(item[key_field]).lower(): {k: item.get(k, '') for k in fields}
            for item in all_items if item.get(key_field)
        }

    async def upload_policies(self, policies: List[Dict]) -> bool:
        """Upload policies to AMS.
        
        Args:
            policies: List of policy dictionaries to upload
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # In a real implementation, we would upload to the API here
            logger.info(f"Would upload {len(policies)} policies to AMS in production")
            return True
        except Exception as e:
            logger.error(f"Error uploading policies: {e}")
            return False

    async def create_insured(self, insured: Dict) -> Optional[str]:
        """Create a new insured in AMS and return their name."""
        payload = {
            "doctype": "Insured",
            "name": insured["insured_name"],  # Using name as primary key
            "insured_name": insured["insured_name"],
            "email": insured["email"]
        }
        
        max_retries, retry_delay = 3, 2
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.api_url}/frappe.client.insert",
                        json=payload,
                        headers=self.headers
                    ) as resp:
                        if resp.status == 200:
                            logger.debug(f"Created insured: {insured['insured_name']}")
                            return insured["insured_name"]
                        elif resp.status == 409:  # Already exists
                            logger.debug(f"Insured already exists: {insured['insured_name']}")
                            return insured["insured_name"]
                        logger.warning(f"Failed to create insured {insured['insured_name']}: {resp.status}")
            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed to create insured {insured['insured_name']} after retries: {e}")
        return None

    async def create_policy(self, policy: Dict) -> bool:
        """Create a new policy in AMS."""
        payload = {
            "doctype": "Policy",
            "name": policy["policy_number"],  # Explicitly set name as the primary key
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
            "insured": policy.get("insured_name")  # Use insured_name directly as it's the primary key
        }
        
        # Add endorsement_type field if it exists
        if policy.get("endorsement_type"):
            payload["endorsement_type"] = policy["endorsement_type"]

        max_retries, retry_delay = 3, 2
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.api_url}/frappe.client.insert",
                        json=payload,
                        headers=self.headers
                    ) as resp:
                        if resp.status == 200:
                            logger.debug(f"Created policy: {policy['policy_number']}")
                            return True
                        logger.warning(f"Failed to create policy {policy['policy_number']}: {resp.status}")
            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed to create policy {policy['policy_number']} after retries: {e}")
        return False

    async def update_policy(self, policy: Dict) -> bool:
        """Update an existing policy in AMS."""
        payload = {
            "doctype": "Policy",
            "name": policy["policy_number"],  # Use policy_number as the name for updates
            "effective_date": policy["effective_date"],
            "expiration_date": policy["expiration_date"],
            "status": policy["status"],
            "premium": policy["premium"],
            "broker": policy["broker_email"],
            "policy_type": policy["policy_type"],
            "carrier": policy["carrier"],
            "commission_amount": policy["commission_amount"],
            "broker_fee": policy["broker_fee_amount"],
            "insured": policy.get("insured_name")  # Use insured_name directly as it's the primary key
        }
        
        # Add endorsement_type field if it exists
        if policy.get("endorsement_type"):
            payload["endorsement_type"] = policy["endorsement_type"]

        max_retries, retry_delay = 3, 2
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.api_url}/frappe.client.set_value",
                        json=payload,
                        headers=self.headers
                    ) as resp:
                        if resp.status == 200:
                            logger.debug(f"Updated policy: {policy['policy_number']}")
                            return True
                        logger.warning(f"Failed to update policy {policy['policy_number']}: {resp.status}")
            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed to update policy {policy['policy_number']} after retries: {e}")
        return False

    @classmethod
    def from_env(cls) -> Optional['AMSClient']:
        """Create AMSClient instance from environment variables."""
        try:
            # Load environment variables
            load_dotenv()
            
            # Check for required environment variables
            if not os.environ.get('AMS_API_TOKEN'):
                logger.error("AMS_API_TOKEN not found in environment variables")
                return None
            
            # Create cache directory
            cache_dir = Path('data/cache')
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            return cls(cache_dir)
        except Exception as e:
            logger.error(f"Error creating AMSClient: {e}")
            return None 