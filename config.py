"""
Configuration file for Jal Jeevan Mission Econometric Analysis Pipeline
Contains API endpoints, file paths, and other configuration settings
"""

import os
from pathlib import Path

# Base directory for the project
BASE_DIR = Path(__file__).parent

# API Endpoints Configuration
API_ENDPOINTS = {
    "jjm_imis": {
        "base_url": "https://imis.jaljeevanmission.gov.in/api",
        "endpoints": {
            "dashboard": "/dashboard",
            "village_data": "/village-data",
            "habitation_data": "/habitation-data",
            "household_coverage": "/household-coverage",
            "water_quality": "/water-quality",
            "infrastructure": "/infrastructure",
            "financial_data": "/financial-data",
            "progress_report": "/progress-report"
        },
        "auth_endpoint": "/auth/login",
        "timeout": 30,
        "retry_attempts": 3
    },
    "health_data": {
        "base_url": "https://api.healthdata.gov.in/api",
        "endpoints": {
            "disease_surveillance": "/disease-surveillance",
            "waterborne_diseases": "/waterborne-diseases",
            "health_indicators": "/health-indicators",
            "morbidity_data": "/morbidity-data",
            "mortality_data": "/mortality-data"
        },
        "auth_endpoint": "/auth/token",
        "timeout": 30,
        "retry_attempts": 3
    }
}

# File Paths Configuration
FILE_PATHS = {
    "data": {
        "raw": str(BASE_DIR / "data" / "raw"),
        "processed": str(BASE_DIR / "data" / "processed"),
        "external": str(BASE_DIR / "data" / "external"),
        "interim": str(BASE_DIR / "data" / "interim")
    },
    "logs": {
        "root": str(BASE_DIR / "logs"),
        "ingestion": str(BASE_DIR / "logs" / "ingestion"),
        "processing": str(BASE_DIR / "logs" / "processing"),
        "analysis": str(BASE_DIR / "logs" / "analysis")
    },
    "output": {
        "reports": str(BASE_DIR / "output" / "reports"),
        "figures": str(BASE_DIR / "output" / "figures"),
        "models": str(BASE_DIR / "output" / "models")
    }
}

# Create directories if they don't exist
def create_directories():
    """Create all necessary directories for the project"""
    for path_group in [FILE_PATHS["data"], FILE_PATHS["logs"], FILE_PATHS["output"]]:
        for path in path_group.values():
            os.makedirs(path, exist_ok=True)

# Logging Configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "detailed": {
            "format": "%(asctime)s [%(levelname)s] %(name)s [%(filename)s:%(lineno)d]: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "log_levels": {
        "ingestion": "INFO",
        "processing": "INFO",
        "analysis": "DEBUG"
    }
}

# Initialize directories on import
create_directories()

