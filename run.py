import os
import requests
import json
from dotenv import load_dotenv
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class BloombergAPIClient:
    def __init__(self):
        # Load credentials from environment variables
        self.client_id = os.getenv('BLOOMBERG_CLIENT_ID')
        self.client_secret = os.getenv('BLOOMBERG_CLIENT_SECRET')
        self.api_host = os.getenv('BLOOMBERG_API_HOST')
        self.oauth_endpoint = os.getenv('BLOOMBERG_OAUTH_ENDPOINT')
        
        # Initialize token variables
        self.access_token = None
        self.token_expiry = 0
        
    def authenticate(self):
        """Get an OAuth access token from Bloomberg"""
        if self.access_token and time.time() < self.token_expiry - 60:
            return self.access_token
        logger.info("Getting new access token...")
        auth_url = f"{self.oauth_endpoint}"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            response = requests.post(auth_url, headers=headers, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get('access_token')
            # Set expiry time (usually 3600 seconds/1 hour)
            expires_in = token_data.get('expires_in', 3600)
            self.token_expiry = time.time() + expires_in
            
            logger.info(f"Authentication successful, token valid for {expires_in} seconds")
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise

    def get_bloomberg_data_catalog(self):
        """Get the Bloomberg Data catalog"""
        token = self.authenticate()
        
        # The Bloomberg Data catalog URL
        bbg_url = f"{self.api_host}/eap/catalogs/bbg/"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "api-version": "2"
        }
        
        try:
            response = requests.get(bbg_url, headers=headers)
            response.raise_for_status()
            
            catalog_data = response.json()
            logger.info(f"Retrieved Bloomberg Data catalog")
            return catalog_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get Bloomberg Data catalog: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise
    
    def get_data_catalogs(self):
        """Get available data catalogs from Bloomberg EAP"""
        token = self.authenticate()
        
        catalogs_url = f"{self.api_host}/eap/catalogs/"  # Note the trailing slash
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "api-version": "2"
        }
        
        try:
            response = requests.get(catalogs_url, headers=headers)
            response.raise_for_status()
            
            catalogs = response.json()
            logger.info(f"Retrieved {len(catalogs) if isinstance(catalogs, list) else 'catalog'} data")
            return catalogs
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get data catalogs: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise

    def get_datasets_catalog(self):
        """Get the Datasets catalog from Bloomberg"""
        token = self.authenticate()
        
        datasets_url = f"{self.api_host}/eap/catalogs/bbg/datasets/"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "api-version": "2"
        }
        
        try:
            response = requests.get(datasets_url, headers=headers)
            response.raise_for_status()
            
            datasets = response.json()
            logger.info(f"Retrieved datasets catalog")
            return datasets
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get datasets catalog: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise
    
    def get_financial_dataset_info(self, catalog_id):
        """Get information about a specific financial dataset"""
        token = self.authenticate()
        
        dataset_url = f"{self.api_host}/eap/catalogs/{catalog_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(dataset_url, headers=headers)
            response.raise_for_status()
            
            dataset_info = response.json()
            logger.info(f"Retrieved information for dataset {catalog_id}")
            return dataset_info
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get dataset info: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise

    def get_token_info(self):
        """Get information about the current token"""
        token = self.authenticate()
        
        # Some APIs provide an endpoint to check token permissions
        token_info_url = f"{self.api_host}/eap/token-info"  # This is a guess - the actual endpoint may differ
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "api-version": "2"
        }
        
        try:
            response = requests.get(token_info_url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Could not get token info: {response.status_code}")
                return None
        except Exception as e:
            logger.warning(f"Error getting token info: {str(e)}")
            return None

    def explore_available_endpoints(self):
        """Systematically explore available endpoints"""
        token = self.authenticate()
        
        # List of potential endpoints to try
        potential_endpoints = [
            "/eap/catalogs/bbg/datasets",
            "/eap/catalogs/bbg/publishers",
            "/eap/data",
            "/eap/data-license",
            "/eap/catalogs/48408",  # The client-defined resources catalog
            "/eap/catalogs/48408/datasets"
        ]
        
        results = {}
        for endpoint in potential_endpoints:
            url = f"{self.api_host}{endpoint}"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "api-version": "2"
            }
            
            try:
                logger.info(f"Trying endpoint: {url}")
                response = requests.get(url, headers=headers)
                status = response.status_code
                results[endpoint] = {
                    "status": status,
                    "accessible": status < 400
                }
                
                if status < 400:
                    try:
                        data = response.json()
                        results[endpoint]["data_preview"] = str(data)[:200] + "..."
                    except:
                        results[endpoint]["data_preview"] = "Non-JSON response"
                
            except Exception as e:
                results[endpoint] = {
                    "status": "error",
                    "error": str(e),
                    "accessible": False
                }
        
        return results

# Main execution
if __name__ == "__main__":
    client = BloombergAPIClient()
    
    try:
        token = client.authenticate()
        logger.info("Authentication test successful")
        
        # First, try to get token info
        token_info = client.get_token_info()
        if token_info:
            logger.info(f"Token info: {json.dumps(token_info, indent=2)}")
        
        # Explore available endpoints
        logger.info("Exploring available endpoints...")
        endpoints_results = client.explore_available_endpoints()
        
        # Print results
        logger.info("Endpoint accessibility results:")
        for endpoint, result in endpoints_results.items():
            status = result.get("status")
            accessible = result.get("accessible")
            logger.info(f"Endpoint {endpoint}: {'✓' if accessible else '✗'} (Status: {status})")
            
            if accessible:
                preview = result.get("data_preview", "")
                if preview:
                    logger.info(f"  Preview: {preview}")
        
        # If we found accessible endpoints, suggest next steps
        accessible_endpoints = [ep for ep, res in endpoints_results.items() if res.get("accessible")]
        if accessible_endpoints:
            logger.info("\nSuggested next steps:")
            logger.info("Based on the accessible endpoints, you can:")
            for endpoint in accessible_endpoints:
                logger.info(f"- Explore {endpoint} in more detail")
            logger.info("- Create specific functions to extract data from these endpoints")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")