#!/usr/bin/env python
# coding: utf-8
"""
Bloomberg Data License API Client

A structured, object-oriented client for accessing the Bloomberg Data License API.
This client allows for retrieving financial data from Bloomberg using identifiers
from a JSON file and environment-based configuration.
"""

import datetime
import io
import json
import logging
import os
import shutil
import time
import uuid
from urllib.parse import urljoin
from dotenv import load_dotenv

from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session


class DLRestApiSession(OAuth2Session):
    """Custom session class for making requests to a DL REST API using OAuth2 authentication."""
    
    def __init__(self, *args, **kwargs):
        """Initialize a DLRestApiSession instance."""
        super().__init__(*args, **kwargs)

    def request_token(self, oauth2_endpoint, client_secret):
        """
        Fetch an OAuth2 access token by making a request to the token endpoint.
        
        Args:
            oauth2_endpoint (str): The OAuth2 token endpoint URL
            client_secret (str): The client secret for authentication
        """
        self.token = self.fetch_token(
            token_url=oauth2_endpoint,
            client_secret=client_secret
        )

    def request(self, *args, **kwargs):
        """
        Override the parent class method to handle TokenExpiredError by refreshing the token.
        
        Returns:
            Response: The response object from the API request
        """
        try:
            response = super().request(*args, **kwargs)
        except TokenExpiredError:
            self.request_token()
            response = super().request(*args, **kwargs)

        return response

    def send(self, request, **kwargs):
        """
        Override the parent class method to log request and response information.
        
        Args:
            request: Prepared request object
            
        Returns:
            Response: The response object from the API request
        """
        logging.info("Request being sent to HTTP server: %s, %s, %s", 
                     request.method, request.url, request.headers)

        response = super().send(request, **kwargs)

        logging.info("Response status: %s", response.status_code)
        logging.info("Response x-request-id: %s", response.headers.get("x-request-id"))

        if response.ok:
            # Filter out file download responses and empty responses.
            if not response.headers.get("Content-Disposition") and response.content:
                logging.info("Response content: %s", json.dumps(response.json(), indent=2))
        else:
            raise RuntimeError(
                '\n\tUnexpected response status code: {c}\nDetails: {r}'.format(
                    c=str(response.status_code), r=response.json())
            )

        return response


class BloombergDataClient:
    """Client for interacting with Bloomberg Data License API."""
    
    def __init__(self):
        """Initialize the Bloomberg Data Client with configurations from environment variables."""
        # Load environment variables
        load_dotenv()
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s',
        )
        self.logger = logging.getLogger(__name__)
        
        # Get credentials from environment variables
        self.client_id = os.getenv('BLOOMBERG_CLIENT_ID')
        self.client_secret = os.getenv('BLOOMBERG_CLIENT_SECRET')
        
        if not self.client_id or not self.client_secret:
            raise ValueError("Bloomberg API credentials not found in environment variables.")
        
        # Define API endpoints
        self.host = os.getenv('BLOOMBERG_API_HOST', 'https://api.bloomberg.com')
        self.oauth_endpoint = os.getenv('BLOOMBERG_OAUTH_ENDPOINT', 
                                        'https://bsso.blpprofessional.com/ext/api/as/token.oauth2')
        
        # Set up directories
        self.downloads_path = os.path.join(os.getcwd(), 'downloads')
        os.makedirs(self.downloads_path, exist_ok=True)
        
        self.data_path = os.path.join(os.getcwd(), 'data')
        self.identifiers_file = os.path.join(self.data_path, 'identifiers.json')
        
        # Initialize session
        self._initialize_session()
        
        # Catalog ID will be set later
        self.catalog_id = None

    def _initialize_session(self):
        """Initialize the OAuth2 session with Bloomberg API."""
        client = BackendApplicationClient(client_id=self.client_id)
        self.session = DLRestApiSession(client=client)
        self.session.headers['api-version'] = '2'
        self.session.request_token(self.oauth_endpoint, self.client_secret)
    
    def load_identifiers(self):
        """
        Load identifiers array from the JSON file in the data directory.
        
        Returns:
            list: The list of identifier objects
        """
        try:
            with open(self.identifiers_file, 'r') as file:
                identifiers = json.load(file)
                self.logger.info(f"Successfully loaded {len(identifiers)} identifiers from JSON file")
                return identifiers
        except FileNotFoundError:
            self.logger.error(f"Identifiers file not found: {self.identifiers_file}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON format in identifiers file: {self.identifiers_file}")
            raise
    
    def discover_catalog_id(self):
        """
        Discover the catalog identifier for scheduling requests.
        
        Returns:
            str: The catalog identifier
        """
        catalogs_url = urljoin(self.host, '/eap/catalogs/')
        response = self.session.get(catalogs_url)
        
        catalogs = response.json()['contains']
        for catalog in catalogs:
            if catalog['subscriptionType'] == 'scheduled':
                self.catalog_id = catalog['identifier']
                return self.catalog_id
                
        self.logger.error('Scheduled catalog not in %r', response.json()['contains'])
        raise RuntimeError('Scheduled catalog not found')
    
    def create_request(self, identifiers, fields=None):
        """
        Create a data request with Bloomberg API.
        
        Args:
            identifiers (list): List of identifier objects
            fields (list, optional): List of field mnemonic objects
            
        Returns:
            tuple: (request_name, request_id, request_url)
        """
        if fields is None:
            fields = [
                {'mnemonic': 'TOT_DEBT_TO_TOT_ASSET'},
                {'mnemonic': 'CASH_DVD_COVERAGE'},
                {'mnemonic': 'TOT_DEBT_TO_EBITDA'},
                {'mnemonic': 'CUR_RATIO'},
                {'mnemonic': 'QUICK_RATIO'},
                {'mnemonic': 'GROSS_MARGIN'},
                {'mnemonic': 'INTEREST_COVERAGE_RATIO'},
                {'mnemonic': 'EBITDA_MARGIN'},
                {'mnemonic': 'TOT_LIAB_AND_EQY'},
                {'mnemonic': 'NET_DEBT_TO_SHRHLDR_EQTY'}
            ]
        
        request_name = 'Python301DataRequest' + str(uuid.uuid1())[:6]
        
        request_payload = {
            '@type': 'DataRequest',
            'name': request_name,
            'description': 'Bloomberg financial data request using identifiers from JSON file',
            'universe': {
                '@type': 'Universe',
                'contains': identifiers
            },
            'fieldList': {
                '@type': 'DataFieldList',
                'contains': fields,
            },
            'trigger': {
                '@type': 'SubmitTrigger',
            },
            'formatting': {
                '@type': 'MediaType',
                'outputMediaType': 'application/json',
            },
        }
        
        self.logger.info('Request component payload:\n%s', json.dumps(request_payload, indent=2))
        
        catalog_url = urljoin(self.host, f'/eap/catalogs/{self.catalog_id}/')
        requests_url = urljoin(catalog_url, 'requests/')
        response = self.session.post(requests_url, json=request_payload)
        
        request_location = response.headers['Location']
        request_url = urljoin(self.host, request_location)
        request_id = json.loads(response.text)['request']['identifier']
        
        self.logger.info('%s resource has been successfully created at %s',
                 request_name, request_url)
        
        # Inspect the newly-created request component
        self.session.get(request_url)
        
        return request_name, request_id, request_url
    
    def wait_for_response(self, request_name, request_id, timeout_minutes=45):
        """
        Poll the content responses endpoint to wait for results to be available.
        
        Args:
            request_name (str): The name of the request
            request_id (str): The identifier of the request
            timeout_minutes (int): The maximum time to wait in minutes
            
        Returns:
            str: The output key for downloading results
        """
        responses_url = urljoin(self.host, f'/eap/catalogs/{self.catalog_id}/content/responses/')
        
        params = {
            'prefix': request_name,
            'requestIdentifier': request_id,
        }
        
        reply_timeout = datetime.timedelta(minutes=timeout_minutes)
        expiration_timestamp = datetime.datetime.utcnow() + reply_timeout
        
        while datetime.datetime.utcnow() < expiration_timestamp:
            content_responses = self.session.get(responses_url, params=params)
            response_contains = json.loads(content_responses.text)['contains']
            
            if len(response_contains) > 0:
                output = response_contains[0]
                self.logger.info('Response listing:\n%s', json.dumps(output, indent=2))
                
                output_key = output['key']
                return output_key
            else:
                self.logger.info('Content not ready for download yet. Waiting...')
                time.sleep(30)
        
        self.logger.info('Response not received within %s minutes. Exiting.', timeout_minutes)
        return None
    
    def download_result(self, output_key):
        """
        Download the result file from Bloomberg API.
        
        Args:
            output_key (str): The key for the output file
            
        Returns:
            str: The path to the downloaded file
        """
        output_url = urljoin(
            self.host,
            f'/eap/catalogs/{self.catalog_id}/content/responses/{output_key}'
        )
        
        with self.session.get(output_url, stream=True) as response:
            output_filename = output_key
            
            if 'content-encoding' in response.headers:
                if response.headers['content-encoding'] == 'gzip':
                    output_filename = output_filename + '.gz'
                elif response.headers['content-encoding'] == '':
                    pass
                else:
                    raise RuntimeError('Unsupported content encoding received in the response')
            
            output_file_path = os.path.join(self.downloads_path, output_filename)
            
            with open(output_file_path, 'wb') as output_file:
                self.logger.info('Loading file from: %s (can take a while) ...', output_url)
                shutil.copyfileobj(response.raw, output_file)
        
        self.logger.info('File downloaded: %s', output_filename)
        self.logger.debug('File location: %s', output_file_path)
        
        return output_file_path
    
    def display_result(self, file_path):
        """
        Display the content of the downloaded file using pandas.
        
        Args:
            file_path (str): The path to the downloaded file
        """
        try:
            import pandas
            with open(file_path, 'rb') as output_file:
                df = pandas.read_json(output_file, compression='gzip')
                print(df)
                return df
        except ImportError:
            self.logger.warning("pandas not installed. To view the data, install pandas or manually check the downloaded file.")
            return None
    
    def fetch_financial_data(self):
        """
        Main method to fetch financial data from Bloomberg API.
        
        Returns:
            DataFrame or None: The fetched data as a pandas DataFrame if available
        """
        try:
            # Discover catalog ID
            self.discover_catalog_id()
            
            # Load identifiers
            identifiers = self.load_identifiers()
            
            # Create data request
            request_name, request_id, _ = self.create_request(identifiers)
            
            # Wait for the response
            output_key = self.wait_for_response(request_name, request_id)
            if not output_key:
                return None
            
            # Download result
            output_file_path = self.download_result(output_key)
            
            # Display result
            return self.display_result(output_file_path)
            
        except Exception as e:
            self.logger.error(f"Error fetching financial data: {str(e)}")
            raise


def main():
    """Main function to run the Bloomberg data client."""
    client = BloombergDataClient()
    client.fetch_financial_data()


if __name__ == '__main__':
    main()