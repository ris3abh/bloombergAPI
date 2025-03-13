"""
Configuration utilities for Bloomberg to HANA integration
"""

import os
import logging
import logging.handlers
from dotenv import load_dotenv
from pathlib import Path

def setup_logging(log_dir="logs"):
    """
    Set up logging configuration.
    
    Args:
        log_dir (str): Directory to store log files
    """
    # Create logs directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Configure the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s')
    console_formatter = logging.Formatter('[%(levelname)-8s] %(message)s')
    
    # Create and configure file handler (with rotation)
    log_file = os.path.join(log_dir, "bloomberg_to_hana.log")
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Create and configure console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def load_config():
    """
    Load configuration from .env file and environment variables.
    
    Returns:
        dict: Configuration parameters
    """
    # Load environment variables from .env file
    load_dotenv()
    
    config = {
        # Bloomberg API settings
        'bloomberg': {
            'client_id': os.getenv('BLOOMBERG_CLIENT_ID'),
            'client_secret': os.getenv('BLOOMBERG_CLIENT_SECRET'),
            'api_host': os.getenv('BLOOMBERG_API_HOST', 'https://api.bloomberg.com'),
            'oauth_endpoint': os.getenv('BLOOMBERG_OAUTH_ENDPOINT', 
                                      'https://bsso.blpprofessional.com/ext/api/as/token.oauth2')
        },
        
        # SAP HANA settings
        'hana': {
            'address': os.getenv('HANA_ADDRESS'),
            'port': os.getenv('HANA_PORT'),
            'user': os.getenv('HANA_USER'),
            'password': os.getenv('HANA_PASSWORD'),
            'schema': os.getenv('HANA_SCHEMA', 'BLOOMBERG_DATA'),
            'table': os.getenv('HANA_TABLE', 'FINANCIAL_RATIOS')
        },
        
        # File paths
        'paths': {
            'data_dir': os.getenv('DATA_DIR', 'data'),
            'downloads_dir': os.getenv('DOWNLOADS_DIR', 'downloads'),
            'identifiers_file': os.getenv('IDENTIFIERS_FILE', 'data/identifiers.json')
        }
    }
    
    # Validate required configuration
    missing_bloomberg = []
    if not config['bloomberg']['client_id']:
        missing_bloomberg.append('BLOOMBERG_CLIENT_ID')
    if not config['bloomberg']['client_secret']:
        missing_bloomberg.append('BLOOMBERG_CLIENT_SECRET')
        
    if missing_bloomberg:
        raise ValueError(f"Missing required Bloomberg API configuration: {', '.join(missing_bloomberg)}")
    
    # Warn about missing HANA config, but don't fail (might run in download-only mode)
    missing_hana = []
    if not config['hana']['address']:
        missing_hana.append('HANA_ADDRESS')
    if not config['hana']['port']:
        missing_hana.append('HANA_PORT')
    if not config['hana']['user']:
        missing_hana.append('HANA_USER')
    if not config['hana']['password']:
        missing_hana.append('HANA_PASSWORD')
        
    if missing_hana:
        logging.warning(f"Missing SAP HANA configuration: {', '.join(missing_hana)}")
        logging.warning("HANA integration will be disabled")
        
    # Create directories if they don't exist
    for dir_path in [config['paths']['data_dir'], config['paths']['downloads_dir']]:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    return config