import math
import time
import uuid
from urllib.parse import urljoin
import pandas as pd

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

# Bloomberg API credentials
client_id = 'ad66b6dc591ac1134e0c3d88787d6b41'
client_secret = '3d4cc2242371183a456144aa1ee0030f77930b682d5ff65cc16f2954ba5f5204'

host = 'https://api.bloomberg.com'
oauth2_endpoint = 'https://bsso.blpprofessional.com/ext/api/as/token.oauth2'

# Http session
client = BackendApplicationClient(client_id=client_id)
session = OAuth2Session(client=client)
token = session.fetch_token(token_url=oauth2_endpoint, client_secret=client_secret)

print('OAuth2 token expires in {h} hours. Obtain a new one when it expires.'.format(
    h=math.ceil(token.get('expires_in') / 3600)
))

# Request Json. Enter the data field you want here
headers = {'api-version': '2'}

request_payload = {
    '@type': 'DataRequest',
    'title': 'ESGDataRequest_' + str(uuid.uuid1())[:6],
    'description': 'Requesting ESG scores for selected securities',
    'universe': {
        '@type': 'Universe',
        'contains': [
            {
                '@type': 'Identifier',
                'identifierType': 'TICKER',
                'identifierValue': 'AAPL US Equity',
            },
            {
                '@type': 'Identifier',
                'identifierType': 'BB_GLOBAL',
                'identifierValue': 'BBG009S3NB30',  # GOOG US Equity
            },
            {
                '@type': 'Identifier',
                'identifierType': 'ISIN',
                'identifierValue': 'US88160R1014',  # TSLA US Equity
            },
        ]
    },
    'fieldList': {
        '@type': 'DataFieldList',
        'contains': [
            {'mnemonic': 'NAME'},
            {'mnemonic': 'ENVIRONMENTAL_SCORE'},
            {'mnemonic': 'SOCIAL_SCORE'},
            {'mnemonic': 'GOVERNANCE_SCORE'},
            {'mnemonic': 'COMPANY_IS_PRIVATE'}
        ],
    },
    'trigger': {
        "@type": "SubmitTrigger",
    },
    'formatting': {
        '@type': 'MediaType',
        'outputMediaType': 'application/json',
    }
}

requests_url = urljoin(host, '/eap/catalogs/48408/requests/')
response = session.post(requests_url, json=request_payload, headers=headers)

# Add debugging to see the actual response structure
print("Response status code:", response.status_code)
print("Response content:", response.text)

# Then safely try to access the JSON
try:
    response_json = response.json()
    print("JSON structure:", response_json)
    request_id = response_json.get('request', {}).get('identifier')
    if not request_id:
        print("Error: 'request' key or 'identifier' not found in response")
        # You might need to extract the ID from a different location in the response
except Exception as e:
    print(f"Error parsing JSON: {e}")
request_id = response.json()['request']['identifier']
content_responses_url = urljoin(host, f'/eap/catalogs/48408/content/responses/?requestIdentifier={request_id}')

while True:
    response = session.get(content_responses_url, headers=headers)
    if response.status_code == 200:
        responses = response.json()['contains']
        if len(responses) > 0:
            latest_response = responses[0]
            key = latest_response['key']
            snapshot_timestamp = latest_response["metadata"]["DL_SNAPSHOT_START_TIME"]
            data_url = urljoin(host, f'/eap/catalogs/48408/content/responses/{key}')
            data_response = session.get(data_url, headers=headers)

            # Load the data as JSON
            data = data_response.json()
            df = pd.json_normalize(data)
            esg_columns = ['NAME', 'ENVIRONMENTAL_SCORE', 'SOCIAL_SCORE', 'GOVERNANCE_SCORE']
            df_esg = df[esg_columns]
            break
        else:
            print('No generated responses yet. Retrying in 60 seconds...')
            time.sleep(60)
            continue
    else:
        print('Unhandled HTTP status code:', response.status_code)
        print(response.text)
        break

# Output
print(df_esg)

print("hello world")
