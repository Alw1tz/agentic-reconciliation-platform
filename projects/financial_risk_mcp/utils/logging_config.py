"""
Logging Configuration for Financial Risk MCP Server

Provides consistent logging setup with proper formatting and levels.
"""

import logging
import os
from datetime import datetime

def setup_logger(name: str) -> logging.Logger:
    """
    Setup logger with appropriate configuration.
    
    Args:
        name (str): Logger name
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers
    if not logger.handlers:
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
    
    return logger

# Setup default logger
logger = setup_logger(__name__)