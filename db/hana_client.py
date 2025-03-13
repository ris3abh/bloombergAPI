"""
SAP HANA database client for storing Bloomberg financial data
"""

import datetime
import logging

# Import SAP HANA Python client
try:
    from hdbcli import dbapi
    HDBCLI_AVAILABLE = True
except ImportError:
    HDBCLI_AVAILABLE = False
    logging.warning("hdbcli package not installed. SAP HANA integration will not work.")
    logging.warning("Install using: pip install hdbcli")

class HanaClient:
    """Client for interacting with SAP HANA database."""
    
    def __init__(self, config):
        """
        Initialize the SAP HANA client with configuration.
        
        Args:
            config (dict): Configuration parameters
        """
        self.logger = logging.getLogger(__name__)
        
        if not HDBCLI_AVAILABLE:
            self.logger.error("hdbcli package not installed. Cannot use SAP HANA integration.")
            raise ImportError("hdbcli package not installed")
        
        # Set HANA connection parameters
        self.address = config['hana']['address']
        self.port = config['hana']['port']
        self.user = config['hana']['user']
        self.password = config['hana']['password']
        self.schema = config['hana']['schema']
        
        # Connection will be set later
        self.connection = None
        
    def connect(self):
        """
        Establish a connection to SAP HANA database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = dbapi.connect(
                address=self.address,
                port=int(self.port),
                user=self.user,
                password=self.password
            )
            
            self.logger.info("Successfully connected to SAP HANA at %s:%s", 
                         self.address, self.port)
            return True
            
        except Exception as e:
            self.logger.error("Failed to connect to SAP HANA: %s", str(e))
            return False
    
    def close(self):
        """Close the connection to SAP HANA database."""
        if self.connection:
            self.connection.close()
            self.logger.info("Closed connection to SAP HANA")
            self.connection = None
    
    def create_schema_if_not_exists(self, schema_name):
        """
        Create a schema in SAP HANA if it doesn't exist.
        
        Args:
            schema_name (str): The schema name to create
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            self.logger.error("No connection to SAP HANA. Cannot create schema.")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Check if schema exists
            cursor.execute(f"""
            SELECT COUNT(*) FROM SYS.SCHEMAS WHERE SCHEMA_NAME = '{schema_name}'
            """)
            
            schema_exists = cursor.fetchone()[0] > 0
            
            if not schema_exists:
                cursor.execute(f"""
                CREATE SCHEMA "{schema_name}"
                """)
                self.logger.info(f'Successfully created schema "{schema_name}" in SAP HANA')
            else:
                self.logger.info(f'Schema "{schema_name}" already exists in SAP HANA')
                
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating schema: {str(e)}")
            return False
    
    def create_table(self, schema_name, table_name):
        """
        Create a table in SAP HANA for storing the Bloomberg data.
        
        Args:
            schema_name (str): The schema name in SAP HANA
            table_name (str): The table name to create
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            self.logger.error("No connection to SAP HANA. Cannot create table.")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Create table with columns for common financial data fields
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                "ID" INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                "TICKER" NVARCHAR(50),
                "IDENTIFIER_TYPE" NVARCHAR(20),
                "IDENTIFIER_VALUE" NVARCHAR(100),
                "TOT_DEBT_TO_TOT_ASSET" DECIMAL(18,6),
                "CASH_DVD_COVERAGE" DECIMAL(18,6),
                "TOT_DEBT_TO_EBITDA" DECIMAL(18,6),
                "CUR_RATIO" DECIMAL(18,6),
                "QUICK_RATIO" DECIMAL(18,6),
                "GROSS_MARGIN" DECIMAL(18,6),
                "INTEREST_COVERAGE_RATIO" DECIMAL(18,6),
                "EBITDA_MARGIN" DECIMAL(18,6),
                "TOT_LIAB_AND_EQY" DECIMAL(18,6),
                "NET_DEBT_TO_SHRHLDR_EQTY" DECIMAL(18,6),
                "TIMESTAMP" TIMESTAMP
            )
            """
            
            cursor.execute(create_table_sql)
            cursor.close()
            
            self.logger.info(f'Successfully created table "{schema_name}"."{table_name}" in SAP HANA')
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating HANA table: {str(e)}")
            return False
    
    def insert_data(self, df, schema_name, table_name):
        """
        Insert data from DataFrame to SAP HANA table.
        
        Args:
            df (DataFrame): The pandas DataFrame containing Bloomberg data
            schema_name (str): The schema name in SAP HANA
            table_name (str): The table name to insert into
            
        Returns:
            int: The number of rows inserted
        """
        if not self.connection:
            self.logger.error("No connection to SAP HANA. Cannot insert data.")
            return 0
        
        try:
            cursor = self.connection.cursor()
            rows_inserted = 0
            timestamp = datetime.datetime.now()
            
            # Process the Bloomberg API response DataFrame
            # NOTE: This data mapping might need to be customized based on the actual
            # structure of the Bloomberg API response
            for index, row in df.iterrows():
                try:
                    # Extract data from DataFrame
                    # This is a simplified example that needs to be adjusted based on 
                    # the actual structure of your Bloomberg data
                    ticker = row.get('ticker', '')
                    identifier_type = row.get('identifierType', '')
                    identifier_value = row.get('identifierValue', '')
                    
                    # Extract financial metrics
                    tot_debt_to_asset = self._extract_value(row, 'TOT_DEBT_TO_TOT_ASSET')
                    cash_dvd_coverage = self._extract_value(row, 'CASH_DVD_COVERAGE')
                    tot_debt_to_ebitda = self._extract_value(row, 'TOT_DEBT_TO_EBITDA')
                    cur_ratio = self._extract_value(row, 'CUR_RATIO')
                    quick_ratio = self._extract_value(row, 'QUICK_RATIO')
                    gross_margin = self._extract_value(row, 'GROSS_MARGIN')
                    interest_coverage = self._extract_value(row, 'INTEREST_COVERAGE_RATIO')
                    ebitda_margin = self._extract_value(row, 'EBITDA_MARGIN')
                    tot_liab_eqy = self._extract_value(row, 'TOT_LIAB_AND_EQY')
                    net_debt_shrhldr = self._extract_value(row, 'NET_DEBT_TO_SHRHLDR_EQTY')
                    
                    # Insert into HANA
                    insert_sql = f"""
                    INSERT INTO "{schema_name}"."{table_name}" (
                        "TICKER", "IDENTIFIER_TYPE", "IDENTIFIER_VALUE",
                        "TOT_DEBT_TO_TOT_ASSET", "CASH_DVD_COVERAGE", "TOT_DEBT_TO_EBITDA",
                        "CUR_RATIO", "QUICK_RATIO", "GROSS_MARGIN",
                        "INTEREST_COVERAGE_RATIO", "EBITDA_MARGIN", "TOT_LIAB_AND_EQY",
                        "NET_DEBT_TO_SHRHLDR_EQTY", "TIMESTAMP"
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    cursor.execute(insert_sql, (
                        ticker, identifier_type, identifier_value,
                        tot_debt_to_asset, cash_dvd_coverage, tot_debt_to_ebitda,
                        cur_ratio, quick_ratio, gross_margin,
                        interest_coverage, ebitda_margin, tot_liab_eqy,
                        net_debt_shrhldr, timestamp
                    ))
                    
                    rows_inserted += 1
                    
                except Exception as row_error:
                    self.logger.warning(f"Error inserting row {index}: {str(row_error)}")
                    continue
            
            self.connection.commit()
            cursor.close()
            
            self.logger.info(f'Successfully inserted {rows_inserted} rows into "{schema_name}"."{table_name}"')
            return rows_inserted
            
        except Exception as e:
            self.logger.error(f"Error inserting data to HANA: {str(e)}")
            return 0
    
    def _extract_value(self, row, field_name):
        """
        Helper method to extract field values from Bloomberg data.
        
        Args:
            row: DataFrame row
            field_name: Field name to extract
            
        Returns:
            Value or None if not found
        """
        # The structure of Bloomberg API response can vary, so we need to handle
        # different possible locations of the data
        
        # Direct access
        if field_name in row:
            return row[field_name]
        
        # Try nested dictionaries (common in Bloomberg responses)
        if 'data' in row and isinstance(row['data'], dict) and field_name in row['data']:
            return row['data'][field_name]
            
        # Try looking in a 'fields' or 'values' dictionary
        for container in ['fields', 'values', 'results']:
            if container in row and isinstance(row[container], dict) and field_name in row[container]:
                return row[container][field_name]
        
        # Not found
        return None