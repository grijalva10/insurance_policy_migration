"""
Mapping manager for insurance policy migration.
Handles loading, saving, and updating mapping files.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Set, Tuple, List, Optional

# Configure logger
logger = logging.getLogger('processing')

class MappingManager:
    """Manages mapping files for policy migration."""
    
    # Define critical mapping files that must be protected
    CRITICAL_MAPPING_FILES = [
        'broker_mapping.json',
        'carrier_mapping.json',
        'policy_type_mapping.json'
    ]
    
    def __init__(self, mappings_dir: Path):
        """Initialize the mapping manager.
        
        Args:
            mappings_dir: Directory containing mapping files
        """
        self.mappings_dir = mappings_dir
        self.mappings_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize mappings
        self.broker_mapping: Dict[str, str] = {}
        self.carrier_mapping: Dict[str, str] = {}
        self.policy_type_mapping: Dict[str, str] = {}
        
        # Initialize exclusion sets
        self.non_policy_types: Set[str] = set()
        self.non_carrier_entries: Set[str] = set()
        
        # Initialize unmapped values tracking
        self.unmapped_values = {
            'broker': [],
            'carrier': [],
            'policy_type': [],
            'brokers': [],
            'carriers': [],
            'policy_types': []
        }
        
        # Load mappings - this will exit if critical files are missing
        self._load_mappings()
        self._load_exclusions()
        self._load_unmapped_values()
    
    def _load_mappings(self) -> None:
        """Load mapping files. Exits if critical files are missing."""
        mapping_files = {
            'broker': 'broker_mapping.json',
            'carrier': 'carrier_mapping.json',
            'policy_type': 'policy_type_mapping.json'
        }
        
        # First check if all critical files exist
        missing_files = []
        for filename in self.CRITICAL_MAPPING_FILES:
            path = self.mappings_dir / filename
            if not path.exists():
                missing_files.append(filename)
        
        if missing_files:
            logger.error(f"CRITICAL ERROR: The following required mapping files are missing: {', '.join(missing_files)}")
            logger.error("These files must exist and cannot be regenerated. Exiting to prevent data loss.")
            sys.exit(1)
                
        # Load mappings from files
        for key, filename in mapping_files.items():
            path = self.mappings_dir / filename
            try:
                with path.open('r') as f:
                    mapping = json.load(f)
                    
                if key == 'broker':
                    self.broker_mapping = mapping
                elif key == 'carrier':
                    self.carrier_mapping = mapping
                elif key == 'policy_type':
                    self.policy_type_mapping = mapping
                    
                logger.info(f"Loaded {len(mapping)} entries from {filename}")
            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")
                sys.exit(1)
    
    def _load_exclusions(self) -> None:
        """Load exclusion sets."""
        path = self.mappings_dir / 'exclusion_mapping.json'
        if not path.exists():
            logger.warning(f"Exclusion file {path} not found, using default exclusions")
            # Default exclusions
            self.non_policy_types = {
                "2nd Payment", "2nd payment", "3rd payment", "Additional Broker Fee", 
                "Additional Premium", "Audit Payment", "Broker Fee", "Declined", 
                "Full Refund", "Full refund", "GL 2nd Payment", "GL 2nd Paymnet", 
                "GL Monthly Payment", "GL+Excess 2nd payment", "Monthly Payment",
                "Partial refund", "Payment Declined", "Payment disputed", 
                "Payment to carrier", "Redunded", "Refund", "Refunded", 
                "VOIDED", "Voided", "new GL 2nd payment", "October Installment",
                "Payment to Carrier", "Second Payment", "Second payment"
            }
            self.non_carrier_entries = {
                "2nd Payment", "2nd payment", "3rd payment", "Additional Broker Fee",
                "Additional Premium", "Audit Payment", "Broker Fee", "Declined",
                "Full Refund", "Full refund", "Monthly Payment", "Monthly payment",
                "October Installment", "Partial refund", "Payment Declined",
                "Payment disputed", "Payment to Carrier", "Payment to carrier",
                "Refund", "Refunded", "Second Payment", "Second payment",
                "VOIDED", "Voided"
            }
            return
            
        try:
            with path.open('r') as f:
                exclusions = json.load(f)
                self.non_policy_types = set(exclusions.get('non_policy_types', []))
                self.non_carrier_entries = set(exclusions.get('non_carrier_entries', []))
                logger.info(f"Loaded {len(self.non_policy_types)} non-policy types and {len(self.non_carrier_entries)} non-carrier entries")
        except Exception as e:
            logger.error(f"Error loading exclusions: {e}")
    
    def _load_unmapped_values(self) -> None:
        """Load unmapped values."""
        path = self.mappings_dir / 'unmatched_values.json'
        if not path.exists():
            logger.warning(f"Unmapped values file {path} not found, creating empty file")
            self._save_unmapped_values()
            return
            
        try:
            with path.open('r') as f:
                self.unmapped_values = json.load(f)
                logger.info(f"Loaded unmapped values: {len(self.unmapped_values.get('carriers', []))} carriers, {len(self.unmapped_values.get('policy_types', []))} policy types, {len(self.unmapped_values.get('brokers', []))} brokers")
        except Exception as e:
            logger.error(f"Error loading unmapped values: {e}")
    
    def _save_unmapped_values(self) -> None:
        """Save unmapped values to file."""
        path = self.mappings_dir / 'unmatched_values.json'
        try:
            with path.open('w') as f:
                json.dump(self.unmapped_values, f, indent=4)
            logger.info(f"Updated unmapped values in {path}")
        except Exception as e:
            logger.error(f"Error saving unmapped values: {e}")
    
    def track_unmapped_value(self, value_type: str, value: str) -> None:
        """Track an unmapped value.
        
        Args:
            value_type: Type of value ('carrier', 'policy_type', or 'broker')
            value: The unmapped value
        """
        if not value:
            return
            
        plural_type = f"{value_type}s"
        if value not in self.unmapped_values.get(plural_type, []):
            if plural_type not in self.unmapped_values:
                self.unmapped_values[plural_type] = []
            self.unmapped_values[plural_type].append(value)
            logger.warning(f"Unmapped {value_type} value: {value}")
            self._save_unmapped_values()
    
    def get_mappings(self) -> Dict[str, Dict[str, str]]:
        """Get all mappings.
        
        Returns:
            Dictionary containing all mappings
        """
        return {
            'broker': self.broker_mapping,
            'carrier': self.carrier_mapping,
            'policy_type': self.policy_type_mapping
        }
    
    def get_exclusions(self) -> Tuple[Set[str], Set[str]]:
        """Get exclusion sets.
        
        Returns:
            Tuple containing non-policy types and non-carrier entries
        """
        return self.non_policy_types, self.non_carrier_entries
    
    def save_mappings(self) -> None:
        """
        This method is intentionally disabled to protect critical mapping files.
        Only unmatched_values.json should be updated.
        """
        logger.warning("Attempt to save mappings was blocked - critical mapping files are read-only")
        return
    
    def add_mapping(self, mapping_type: str, source: str, target: str) -> None:
        """
        Add a new mapping to memory only - does not update critical mapping files.
        Only tracks the unmapped value for later review.
        
        Args:
            mapping_type: Type of mapping ('broker', 'carrier', or 'policy_type')
            source: Source value
            target: Target value
        """
        if not source or not target:
            return
            
        # Instead of updating the mapping files, just track as unmapped
        self.track_unmapped_value(mapping_type, source)
        
        logger.warning(f"Mapping request for {mapping_type}: '{source}' -> '{target}' was recorded in unmatched_values.json")
        logger.warning("Critical mapping files are protected and were not modified")
    
    def add_exclusion(self, exclusion_type: str, value: str) -> None:
        """Add a new exclusion.
        
        Args:
            exclusion_type: Type of exclusion ('policy_type' or 'carrier')
            value: Value to exclude
        """
        if not value:
            return
            
        if exclusion_type == 'policy_type':
            self.non_policy_types.add(value)
        elif exclusion_type == 'carrier':
            self.non_carrier_entries.add(value)
        else:
            logger.warning(f"Unknown exclusion type: {exclusion_type}")
            return
            
        # Save exclusions
        path = self.mappings_dir / 'exclusion_mapping.json'
        try:
            exclusions = {
                'non_policy_types': list(self.non_policy_types),
                'non_carrier_entries': list(self.non_carrier_entries)
            }
            with path.open('w') as f:
                json.dump(exclusions, f, indent=4)
            logger.info(f"Updated exclusions in {path}")
        except Exception as e:
            logger.error(f"Error saving exclusions: {e}")
    
    def is_excluded(self, value_type: str, value: str) -> bool:
        """Check if a value is excluded.
        
        Args:
            value_type: Type of value ('carrier' or 'policy_type')
            value: The value to check
        
        Returns:
            True if the value is excluded, False otherwise
        """
        if not value:
            return False
            
        if value_type == 'policy_type':
            return value in self.non_policy_types
        elif value_type == 'carrier':
            return value in self.non_carrier_entries
        
        return False
    
    def get_mapped_value(self, mapping_type: str, value: str, default: Optional[str] = None) -> Optional[str]:
        """Get a mapped value.
        
        Args:
            mapping_type: Type of mapping ('broker', 'carrier', or 'policy_type')
            value: Source value
            default: Default value if not found
        
        Returns:
            Mapped value or default if not found
        """
        if not value:
            return default
            
        if mapping_type == 'broker':
            mapped = self.broker_mapping.get(value)
        elif mapping_type == 'carrier':
            mapped = self.carrier_mapping.get(value)
        elif mapping_type == 'policy_type':
            mapped = self.policy_type_mapping.get(value)
        else:
            logger.warning(f"Unknown mapping type: {mapping_type}")
            return default
            
        if mapped is None:
            self.track_unmapped_value(mapping_type, value)
            return default
            
        return mapped 