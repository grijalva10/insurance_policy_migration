"""
Logging configuration for the insurance policy migration system.
Provides separate loggers for processing and AMS operations.
"""

import logging
from pathlib import Path

def setup_loggers() -> tuple[logging.Logger, logging.Logger]:
    """Configure and return processing and AMS loggers."""
    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)
    
    # Common format for detailed logging
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # Simple format for console output
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    # Setup processing logger
    processing_logger = logging.getLogger('processing')
    processing_logger.setLevel(logging.DEBUG)
    
    # File handler for processing logs
    proc_file_handler = logging.FileHandler('logs/processing.log', mode='w')
    proc_file_handler.setLevel(logging.DEBUG)
    proc_file_handler.setFormatter(detailed_formatter)
    processing_logger.addHandler(proc_file_handler)
    
    # Setup AMS logger
    ams_logger = logging.getLogger('ams')
    ams_logger.setLevel(logging.DEBUG)
    
    # File handler for AMS logs
    ams_file_handler = logging.FileHandler('logs/ams.log', mode='w')
    ams_file_handler.setLevel(logging.DEBUG)
    ams_file_handler.setFormatter(detailed_formatter)
    ams_logger.addHandler(ams_file_handler)
    
    # Console handler for both loggers (INFO level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(console_formatter)
    processing_logger.addHandler(console_handler)
    ams_logger.addHandler(console_handler)
    
    return processing_logger, ams_logger 