"""
Utility functions for Jal Jeevan Mission Econometric Analysis Pipeline
"""

import logging
import os
from pathlib import Path
from datetime import datetime
from config import FILE_PATHS, LOGGING_CONFIG


def setup_logger(
    name: str,
    log_type: str = "ingestion",
    log_level: str = None,
    log_to_file: bool = True,
    log_to_console: bool = True
) -> logging.Logger:
    """
    Setup and configure a logger for tracking data ingestion errors and other operations.
    
    Args:
        name (str): Name of the logger (typically __name__ or module name)
        log_type (str): Type of logging - 'ingestion', 'processing', or 'analysis'
                        Determines which log directory to use
        log_level (str, optional): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                                   If None, uses default from LOGGING_CONFIG
        log_to_file (bool): Whether to log to a file (default: True)
        log_to_console (bool): Whether to log to console (default: True)
    
    Returns:
        logging.Logger: Configured logger instance
    
    Example:
        logger = setup_logger(__name__, log_type="ingestion")
        logger.info("Starting data ingestion")
        logger.error("Failed to fetch data from API")
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Set log level
    if log_level is None:
        log_level = LOGGING_CONFIG["log_levels"].get(log_type, "INFO")
    
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Prevent duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    # Create formatter
    formatter = logging.Formatter(
        LOGGING_CONFIG["formatters"]["detailed"]["format"],
        datefmt=LOGGING_CONFIG["formatters"]["detailed"]["datefmt"]
    )
    
    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_to_file:
        # Determine log directory based on log_type
        log_dir = FILE_PATHS["logs"].get(log_type, FILE_PATHS["logs"]["root"])
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Create log filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d")
        log_filename = f"{log_type}_{timestamp}.log"
        log_filepath = os.path.join(log_dir, log_filename)
        
        # File handler with rotation (append mode)
        file_handler = logging.FileHandler(log_filepath, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # File gets all log levels
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str, log_type: str = "ingestion") -> logging.Logger:
    """
    Convenience function to get a logger instance.
    Shorthand for setup_logger with default parameters.
    
    Args:
        name (str): Name of the logger
        log_type (str): Type of logging - 'ingestion', 'processing', or 'analysis'
    
    Returns:
        logging.Logger: Configured logger instance
    """
    return setup_logger(name, log_type=log_type)

