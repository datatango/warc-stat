from collections import defaultdict
from urllib.parse import urlparse
from warcio.archiveiterator import ArchiveIterator
import argparse
import os
import json
import logging


def setup_logging(log_dir='logs'):
    # create dir if it does not exist
    os.makedirs(log_dir, exist_ok=True)

    # configure logging
    log_file = os.path.join(log_dir, 'warcstat.log')
    logging.basicConfig(
        level = logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)

def main():
    # set up cli argument parsing
    parser = argparse.ArgumentParser(description='Get WARC info & stats') # create argument parser object
    parser.add_argument('warc_file', help='Path to the WARC file') # required argument
    parser.add_argument('-o', '--output', help='Output JSON file path') # optional argument
    parser.add_argument('--log-dir', default='logs', help='Log directory path') # optional argument
    args = parser.parse_args()
    warc_path = args.warc_file

    # set up logging
    logger = setup_logging(args.log_dir) # return logger object
    logger.info(f"Processing: {warc_path}")

    # initialize dictionary
    stats = {
        'total_records': 0, # counts total number of records in WARC
        'total_bytes': 0, # tracks totals size of all records in bytes
        'errors': [], # stores list of non-200 HTTP reponses
        'hosts': defaultdict(int), # count of requests per host
        'http_status_codes': defaultdict(int), # count of HTTP status codes
        'mime_types': defaultdict(int), # count of MIME types
        'record_types': defaultdict(int) # count of WARC records types
    }

    with open(warc_path, "rb") as stream:
        for record in ArchiveIterator(stream):
            stats['total_records'] += 1
            stats['record_types'][record.rec_type] += 1

            if record.length:
                stats['total_bytes'] += record.length
            if record.rec_type == 'response' and record.http_headers: # only records that are HTTP responses & have HTTP headers
                status = record.http_headers.get_statuscode()
                uri = record.rec_headers.get_header('WARC-Target-URI')
                if uri:
                    host = urlparse(uri).netloc # extract domain
                    if host:
                        stats['hosts'][host] += 1 # count number of times a host is seen
                if status:
                    stats['http_status_codes'][status] += 1 # count status code

                    if status != 200:
                        stats['errors'].append({ # save error details in not successful
                            'url': uri,
                            'status': status
                        })

                content_type = record.http_headers.get_header('Content-Type') # "text/html", etc
                if content_type:
                    content_type = content_type.split(';')[0].strip()
                    stats['mime_types'][content_type] += 1 # count of number of MIME type
        
    
    # convert defaultdict objects back to Python dicts
    stats['record_types'] = dict(stats['record_types'])
    stats['mime_types'] = dict(stats['mime_types'])
    stats['http_status_codes'] = dict(stats['http_status_codes'])
    stats['hosts'] = dict(stats['hosts'])
    output = json.dumps(stats, indent=2) # json output

    if args.output: # if user specified output
        output_path = args.output

        if os.path.isdir(output_path): # check if output is a dir
            output_path = os.path.join(output_path, "warcstat_output.json") # create file inside that dir

        with open(output_path, 'w') as f: 
            f.write(output) # write json output
        logger.info(f"Output written to: {output_path}")
    else:
        print(output) # print json output to terminal if no output is defined

if __name__ == "__main__":
    main()
