from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import google.oauth2.credentials
from singer import metadata
import datetime

def get_dimensions_from_stream(stream):
    #only flat metrics for now
    schema_dict = stream.schema.to_dict()
    metadata_dict = metadata.to_map(stream.metadata)
       
    dimensions = [prop for prop in schema_dict['properties'] if metadata.get(metadata_dict, ("properties", prop), "dimension") is True]
    
    return dimensions


def get_search_console_report(config, stream, date):

    searchconsole_api = initialize_google_sheets(config)
    dimensions = get_dimensions_from_stream(stream)
      
    request = {
        'startDate': date,
        'endDate': date,
        'dimensions': dimensions,
    }

    results = searchconsole_api.searchanalytics().query(
        siteUrl=config["site_name"], body=request).execute()
    
    if 'rows' in results:
        return expand_google_keys(results['rows'], dimensions)
    else:
        return []
        

def expand_google_keys(google_rows, dimensions):
    expanded_keys = []
    for row in google_rows:
        keys_dict = {}
        for index, key in enumerate(row['keys']):
            key_name = dimensions[index]
            keys_dict[key_name] = key
        row.pop('keys')
        expanded_keys.append({**row, **keys_dict})
        
    return expanded_keys

        

def initialize_google_sheets(config):
    """Initializes an Sheets API V4 service object.

    Returns:
      An authorized Sheets API V4 service object.
    """
    _GOOGLE_OAUTH2_ENDPOINT = 'https://accounts.google.com/o/oauth2/token'

    creds = google.oauth2.credentials.Credentials(
        config['developer_token'], refresh_token=config['refresh_token'],
        client_id=config['oauth_client_id'],
        client_secret=config['oauth_client_secret'],
        token_uri=_GOOGLE_OAUTH2_ENDPOINT)

    return build('webmasters', 'v3', credentials=creds)
