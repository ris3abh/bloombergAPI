#!/usr/bin/env python
# coding: utf-8
"""
Bloomberg to SAP HANA Integration

Main script to fetch financial data from Bloomberg and store it in SAP HANA.
"""

import argparse
import sys
import os
import logging

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config import setup_logging, load_config
from api.bloomberg_api import BloombergApiClient
from db.hana_client import HanaClient


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Bloomberg to SAP HANA Data Integration')
    
    parser.add_argument(
        '--download-only', 
        action='store_true',
        help='Download data from Bloomberg but do not store in HANA'
    )
    
    parser.add_argument(
        '--schema',
        type=str,
        help='SAP HANA schema name (overrides .env setting)'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        help='SAP HANA table name (overrides .env setting)'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=45,
        help='Timeout in minutes for waiting for Bloomberg response'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the Bloomberg to SAP HANA integration."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logging()
    logger.info("Starting Bloomberg to SAP HANA integration")
    
    try:
        # Load configuration
        config = load_config()
        
        # Override config with command line arguments if provided
        if args.schema:
            config['hana']['schema'] = args.schema
        if args.table:
            config['hana']['table'] = args.table
        
        # Initialize Bloomberg API client
        bloomberg_client = BloombergApiClient(config)
        logger.info("Initialized Bloomberg API client")
        
        # Fetch data from Bloomberg
        logger.info("Fetching financial data from Bloomberg...")
        df, file_path = bloomberg_client.fetch_financial_data()
        
        if df is None:
            logger.error("Failed to fetch data from Bloomberg")
            return 1
        
        logger.info(f"Successfully fetched data from Bloomberg: {len(df)} rows")
        logger.info(f"Data stored in: {file_path}")
        
        # Stop here if download-only flag is set
        if args.download_only:
            logger.info("Download-only flag set. Skipping HANA integration.")
            return 0
        
        # Initialize HANA client and store data
        try:
            hana_client = HanaClient(config)
            logger.info("Initialized SAP HANA client")
            
            # Connect to HANA
            if not hana_client.connect():
                logger.error("Failed to connect to SAP HANA")
                return 1
            
            # Create schema if it doesn't exist
            schema_name = config['hana']['schema']
            if not hana_client.create_schema_if_not_exists(schema_name):
                logger.error(f"Failed to create schema: {schema_name}")
                return 1
            
            # Create table if it doesn't exist
            table_name = config['hana']['table']
            if not hana_client.create_table(schema_name, table_name):
                logger.error(f"Failed to create table: {schema_name}.{table_name}")
                return 1
            
            # Insert data
            rows_inserted = hana_client.insert_data(df, schema_name, table_name)
            if rows_inserted > 0:
                logger.info(f"Successfully inserted {rows_inserted} rows into {schema_name}.{table_name}")
            else:
                logger.error("Failed to insert data into SAP HANA")
                return 1
                
        except ImportError:
            logger.error("SAP HANA integration is not available (hdbcli not installed)")
            return 1
        finally:
            # Close HANA connection
            if 'hana_client' in locals() and hana_client:
                hana_client.close()
        
        logger.info("Bloomberg to SAP HANA integration completed successfully")
        return 0
        
    except Exception as e:
        logger.exception(f"Error in Bloomberg to SAP HANA integration: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())