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

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s',
)
LOG = logging.getLogger(__name__)

BLOOMBERG_CLIENT_ID = os.getenv('BLOOMBERG_CLIENT_ID')
BLOOMBERG_CLIENT_SECRET = os.getenv('BLOOMBERG_CLIENT_SECRET')

if not BLOOMBERG_CLIENT_ID or not BLOOMBERG_CLIENT_SECRET:
    raise ValueError("Bloomberg API credentials not found in environment variables.")

class DLRestApiSession(OAuth2Session):
    """Custom session class for making requests to a DL REST API using OAuth2 authentication."""
    def __init__(self, *args, **kwargs):
        """
        Initialize a DLRestApiSession instance.
        """
        super().__init__(*args, **kwargs)

    def request_token(self):
        """
        Fetch an OAuth2 access token by making a request to the token endpoint.
        """
        oauth2_endpoint = os.getenv('BLOOMBERG_OAUTH_ENDPOINT', 'https://bsso.blpprofessional.com/ext/api/as/token.oauth2')
        self.token = self.fetch_token(
            token_url=oauth2_endpoint,
            client_secret=BLOOMBERG_CLIENT_SECRET
        )

    def request(self, *args, **kwargs):
        """
        Override the parent class method to handle TokenExpiredError by refreshing the token.
        :return: response object from the API request
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
        :param request: prepared request object
        :return: response object from the API request
        """
        LOG.info("Request being sent to HTTP server: %s, %s, %s", request.method, request.url, request.headers)

        response = super().send(request, **kwargs)

        LOG.info("Response status: %s", response.status_code)
        LOG.info("Response x-request-id: %s", response.headers.get("x-request-id"))

        if response.ok:
            # Filter out file download responses and empty responses.
            if not response.headers.get("Content-Disposition") and response.content:
                LOG.info("Response content: %s", json.dumps(response.json(), indent=2))
        else:
            raise RuntimeError('\n\tUnexpected response status code: {c}\nDetails: {r}'.format(
                    c=str(response.status_code), r=response.json()))

        return response

CLIENT = BackendApplicationClient(client_id=BLOOMBERG_CLIENT_ID)

SESSION = DLRestApiSession(client=CLIENT)
SESSION.headers['api-version'] = '2'
SESSION.request_token()

HOST = os.getenv('BLOOMBERG_API_HOST', 'https://api.bloomberg.com')

DOWNLOADS_PATH = os.path.join(os.getcwd(), 'downloads')
os.makedirs(DOWNLOADS_PATH, exist_ok=True)

DATA_PATH = os.path.join(os.getcwd(), 'data')
IDENTIFIERS_FILE = os.path.join(DATA_PATH, 'identifiers.json')

def load_identifiers_json():
    """
    Load identifiers array from the JSON file in the data directory.
    
    Returns:
        list: The list of identifier objects
    """
    try:
        with open(IDENTIFIERS_FILE, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        LOG.error(f"Identifiers file not found: {IDENTIFIERS_FILE}")
        raise
    except json.JSONDecodeError:
        LOG.error(f"Invalid JSON format in identifiers file: {IDENTIFIERS_FILE}")
        raise

if __name__ == '__main__':
    catalogs_url = urljoin(HOST, '/eap/catalogs/')
    response = SESSION.get(catalogs_url)
    catalogs = response.json()['contains']
    for catalog in catalogs:
        if catalog['subscriptionType'] == 'scheduled':
            catalog_id = catalog['identifier']
            break
    else:
        LOG.error('Scheduled catalog not in %r', response.json()['contains'])
        raise RuntimeError('Scheduled catalog not found')

    ############################################################################
    # - Load identifiers from JSON file
    try:
        identifiers = load_identifiers_json()
        LOG.info(f"Successfully loaded {len(identifiers)} identifiers from JSON file")
    except Exception as e:
        LOG.error(f"Failed to load identifiers: {str(e)}")
        raise
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
            'contains': [
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
            ],
        },
        'trigger': {
            '@type': 'SubmitTrigger',
        },
        'formatting': {
            '@type': 'MediaType',
            'outputMediaType': 'application/json',
        },
    }
        
    LOG.info('Request component payload:\n%s', json.dumps(request_payload, indent=2))
    
    catalog_url = urljoin(HOST, '/eap/catalogs/{c}/'.format(c=catalog_id))
    requests_url = urljoin(catalog_url, 'requests/')
    response = SESSION.post(requests_url, json=request_payload)
    
    request_location = response.headers['Location']
    request_url = urljoin(HOST, request_location)
    request_id = json.loads(response.text)['request']['identifier']
    LOG.info('%s resource has been successfully created at %s',
             request_name,
             request_url)
    SESSION.get(request_url)
    responses_url = urljoin(HOST, '/eap/catalogs/{c}/content/responses/'.format(c=catalog_id))
    params = {
        'prefix': request_name,
        'requestIdentifier': request_id,
    }
    reply_timeout_minutes = 45
    reply_timeout = datetime.timedelta(minutes=reply_timeout_minutes)
    expiration_timestamp = datetime.datetime.utcnow() + reply_timeout

    while datetime.datetime.utcnow() < expiration_timestamp:
        content_responses = SESSION.get(responses_url, params=params)
        response_contains = json.loads(content_responses.text)['contains']
        if len(response_contains) > 0 :
            output = response_contains[0]
            LOG.info('Response listing:\n%s', json.dumps(output, indent=2))
            output_key = output['key']
            output_url = urljoin(
                HOST,
                '/eap/catalogs/{c}/content/responses/{key}'.format(c=catalog_id, key=output_key)
            )
            output_file_path = os.path.join(DOWNLOADS_PATH, output_key)
            break
        else:
            LOG.info('Content not ready for download yet. Waiting...')
            time.sleep(30)
    else:
        LOG.info('Response not received within %s minutes. Exiting.', reply_timeout_minutes)
    with SESSION.get(output_url, stream=True) as response:
        output_filename = output_key
        if 'content-encoding' in response.headers:
            if response.headers['content-encoding'] == 'gzip':
                output_filename = output_filename + '.gz'
            elif response.headers['content-encoding'] == '':
                pass
            else:
                raise RuntimeError('Unsupported content encoding received in the response')
    
        output_file_path = os.path.join(DOWNLOADS_PATH, output_filename)
    
        with open(output_file_path, 'wb') as output_file:
            LOG.info('Loading file from: %s (can take a while) ...', output_url)
            shutil.copyfileobj(response.raw, output_file)
    
    LOG.info('File downloaded: %s', output_filename)
    LOG.debug('File location: %s', output_file_path)
    try:
        import pandas
        with open(output_file_path, 'rb') as output_file:
            df = pandas.read_json(output_file, compression='gzip')
            print(df)
    except ImportError:
        LOG.warning("pandas not installed. To view the data, install pandas or manually check the downloaded file.")