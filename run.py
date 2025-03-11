import requests
import json
import os
import logging
import datetime
from urllib.parse import urljoin
from dotenv import load_dotenv
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s',
)
LOG = logging.getLogger(__name__)

# Configuration
HOST = 'https://api.bloomberg.com'
CLIENT_ID = os.environ.get("BLOOMBERG_CLIENT_ID")
CLIENT_SECRET = os.environ.get("BLOOMBERG_CLIENT_SECRET")
OAUTH2_ENDPOINT = "https://bsso.blpprofessional.com/ext/api/as/token.oauth2"

class BloombergApiSession(OAuth2Session):
    """Custom session class for making requests to Bloomberg API using OAuth2 authentication."""

    def __init__(self, *args, **kwargs):
        """Initialize a BloombergApiSession instance."""
        super().__init__(*args, **kwargs)
        # This is a required header for each call to Bloomberg API
        self.headers['api-version'] = '2'

    def request_token(self):
        """Fetch an OAuth2 access token by making a request to the token endpoint."""
        self.token = self.fetch_token(
            token_url=OAUTH2_ENDPOINT,
            client_secret=CLIENT_SECRET
        )
        expires_in_hours = self.token.get('expires_in', 0) / 3600
        LOG.info(f'OAuth2 token obtained. Expires in {expires_in_hours:.1f} hours')

    def request(self, *args, **kwargs):
        """
        Override the parent class method to handle TokenExpiredError by refreshing the token.
        """
        try:
            response = super().request(*args, **kwargs)
        except TokenExpiredError:
            LOG.info("Token expired. Requesting a new one...")
            self.request_token()
            response = super().request(*args, **kwargs)
        return response

    def send(self, request, **kwargs):
        """
        Override the parent class method to log request and response information.
        """
        LOG.info("Request being sent to HTTP server: %s, %s, %s", request.method, request.url, request.headers)

        response = super().send(request, **kwargs)

        LOG.info("Response status: %s", response.status_code)
        LOG.info("Response x-request-id: %s", response.headers.get("x-request-id"))

        if response.ok:
            # Filter out file download responses and empty responses.
            if not response.headers.get("Content-Disposition") and response.content:
                # Limit the response content logging to avoid overwhelming logs
                content = response.json()
                LOG.info("Response content: %s", json.dumps(content, indent=2)[:300] + "...")
        else:
            try:
                error_detail = response.json()
                LOG.error("Error response: %s", json.dumps(error_detail, indent=2))
            except:
                LOG.error("Error response (not JSON): %s", response.text[:300])
            
            raise RuntimeError(f'\n\tUnexpected response status code: {response.status_code}\nDetails: {response.text[:300]}')

        return response

def get_catalog_data(catalog_id=None):
    """
    Retrieve catalog data from Bloomberg API.
    
    Args:
        catalog_id (str, optional): The ID of a specific catalog to retrieve.
                                   If None, retrieves all catalogs.
    
    Returns:
        dict: The catalog data
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Bloomberg client credentials not found. Please set BLOOMBERG_CLIENT_ID and BLOOMBERG_CLIENT_SECRET in your .env file.")

    # Initialize OAuth session
    client = BackendApplicationClient(client_id=CLIENT_ID)
    session = BloombergApiSession(client=client)
    session.request_token()
    
    # Construct the URL based on whether a catalog_id was provided
    if catalog_id:
        url = urljoin(HOST, f'/eap/catalogs/{catalog_id}')
    else:
        url = urljoin(HOST, '/eap/catalogs/')
    
    # Make the request
    response = session.get(url)
    return response.json()

def discover_scheduled_catalog():
    """
    Discover the catalog identifier for scheduling requests.
    
    Returns:
        str: The catalog ID for scheduled requests
    """
    catalogs_data = get_catalog_data()
    catalogs = catalogs_data['contains']
    
    for catalog in catalogs:
        if catalog.get('subscriptionType') == 'scheduled':
            # Take the catalog having "scheduled" subscription type,
            # which corresponds to the Data License account number.
            return catalog['identifier']
    
    # We exhausted the catalogs, but didn't find a scheduled catalog.
    LOG.error('Scheduled catalog not in %r', catalogs)
    raise RuntimeError('Scheduled catalog not found')

def explore_catalog_resources(catalog_id, resource_type):
    """
    Explore a specific resource type within the catalog (datasets, requests, etc.)
    
    Args:
        catalog_id (str): The catalog ID
        resource_type (str): The type of resource to explore (datasets, requests, etc.)
    
    Returns:
        dict: The resource data
    """
    # Initialize a new session for this request
    client = BackendApplicationClient(client_id=CLIENT_ID)
    session = BloombergApiSession(client=client)
    session.request_token()
    
    url = urljoin(HOST, f'/eap/catalogs/{catalog_id}/{resource_type}')
    
    response = session.get(url)
    return response.json()

def explore_catalog_structure(catalog_data):
    """
    Explore and print the structure of the catalog data.
    
    Args:
        catalog_data (dict): The catalog data to explore
    """
    if not catalog_data:
        LOG.info("No catalog data to explore.")
        return
    
    # Print basic catalog information
    LOG.info(f"Catalog ID: {catalog_data.get('identifier', 'N/A')}")
    LOG.info(f"Catalog Title: {catalog_data.get('title', 'N/A')}")
    LOG.info(f"Description: {catalog_data.get('description', 'N/A')}")
    
    # Print contained resources
    if 'contains' in catalog_data:
        LOG.info("\nContained resources:")
        for resource in catalog_data['contains']:
            LOG.info(f"- {resource.get('title', 'Unnamed')}: {resource.get('description', 'No description')}")
            LOG.info(f"  ID: {resource.get('@id', 'N/A')}")

def save_catalog_data(catalog_data, filename=None):
    """
    Save the catalog data to a JSON file.
    
    Args:
        catalog_data (dict): The catalog data to save
        filename (str, optional): Custom filename. If None, a default name with timestamp will be used.
    """
    if not catalog_data:
        LOG.info("No catalog data to save.")
        return
    
    if not filename:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        catalog_id = catalog_data.get('identifier', 'unknown')
        filename = f"data/bloomberg_catalog_{catalog_id}_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(catalog_data, f, indent=2)
    
    LOG.info(f"Catalog data saved to {filename}")

if __name__ == "__main__":
    try:
        # Discover the scheduled catalog ID
        catalog_id = discover_scheduled_catalog()
        LOG.info(f"Discovered scheduled catalog ID: {catalog_id}")
        
        # Get detailed information about the catalog
        catalog_data = get_catalog_data(catalog_id)
        explore_catalog_structure(catalog_data)
        save_catalog_data(catalog_data)
        
        # Explore different resource types
        resource_types = ["datasets", "requests", "universes", "fieldLists", "triggers"]
        for resource_type in resource_types:
            LOG.info(f"\nExploring {resource_type}...")
            try:
                resource_data = explore_catalog_resources(catalog_id, resource_type)
                save_catalog_data(resource_data, f"bloomberg_{resource_type}_{catalog_id}.json")
            except Exception as e:
                LOG.error(f"Error exploring {resource_type}: {e}")
    
    except Exception as e:
        LOG.error(f"Error in main execution: {e}", exc_info=True)