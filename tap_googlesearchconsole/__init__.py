#!/usr/bin/env python3
import os
import json
import singer
from tap_googlesearchconsole import google_search_console
from singer import utils, metadata
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer)
from datetime import datetime, date, timedelta
import time

REQUIRED_CONFIG_KEYS = ["start_date", "oauth_client_id", "oauth_client_secret", "developer_token", "refresh_token"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': [],
            'key_properties': []
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []

    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams


def sync(config, state, catalog):

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        stream_alias = stream.stream_alias
        if stream_id in selected_stream_ids:
            end_date = datetime.strptime(config['end_date'], "%Y-%m-%d").date() if 'end_date' in config else date.today()
            start_date = datetime.strptime(config['start_date'][0:10], "%Y-%m-%d").date()

            #gonna use at last 5 day window, some times google takes up to3 days to update gsc
            if start_date == end_date:
                start_date = start_date - timedelta(days=5)

            while start_date <= end_date:
                LOGGER.info('Syncing stream:{} for day {}'.format(stream_id, start_date.isoformat()))

                lines = google_search_console.get_search_console_report(config, stream, start_date.isoformat())
                for line in lines:

                    with Transformer(singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
                        singer_lines = bumble_bee.transform(line, stream_schema.to_dict())

                    singer_lines['extraction_date'] = date.today().isoformat()
                    singer.write_record(stream_id, singer_lines, stream_alias)
                LOGGER.info("waiting 2s for next day")
                time.sleep(2)
                start_date = start_date + timedelta(days=1)
    return


def build_singer_line(metric_line, schema):
    properties = schema.to_dict()['properties']
    singer_line = {}

    for i, value in enumerate(properties.items()):
        field_index = i
        field_name = value[0]
        if field_index >= len(metric_line):
        # fill line array to size of index in case line is missing last value
            metric_line += ['']

        singer_line[field_name] = metric_line[field_index]

    return singer_line



@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
