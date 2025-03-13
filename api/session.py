"""
DLRestApiSession - Custom session class for Bloomberg Data License REST API
"""

import json
import logging
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session

class DLRestApiSession(OAuth2Session):
    """Custom session class for making requests to a DL REST API using OAuth2 authentication."""
    
    def __init__(self, *args, **kwargs):
        """Initialize a DLRestApiSession instance."""
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)

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
            self.logger.info("Token expired. Refreshing...")
            self.request_token(kwargs.get('oauth2_endpoint'), kwargs.get('client_secret'))
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
        self.logger.info("Request being sent to HTTP server: %s, %s, %s", 
                     request.method, request.url, request.headers)

        response = super().send(request, **kwargs)

        self.logger.info("Response status: %s", response.status_code)
        self.logger.info("Response x-request-id: %s", response.headers.get("x-request-id"))

        if response.ok:
            # Filter out file download responses and empty responses.
            if not response.headers.get("Content-Disposition") and response.content:
                self.logger.debug("Response content: %s", json.dumps(response.json(), indent=2))
        else:
            raise RuntimeError(
                '\n\tUnexpected response status code: {c}\nDetails: {r}'.format(
                    c=str(response.status_code), r=response.json())
            )

        return response