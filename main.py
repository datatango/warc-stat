from collections import defaultdict
from urllib.parse import urlparse
from warcio.archiveiterator import ArchiveIterator
import argparse
import json


def main():
    parser = argparse.ArgumentParser(description='Get WARC info & stats')
    parser.add_argument('warc_file', help='Path to the WARC file')
    parser.add_argument('-o', '--output', help='Output JSON file path')

    args = parser.parse_args()
    warc_path = args.warc_file

    print(f"Processing: {warc_path}")

    stats = {
        'total_records': 0,
        'total_bytes': 0,
        'hosts': defaultdict(int),
        'http_status_codes': defaultdict(int),
        'mime_types': defaultdict(int),
        'record_types': defaultdict(int)
    }

    with open(warc_path, "rb") as stream:
        for record in ArchiveIterator(stream):
            stats['total_records'] += 1
            stats['record_types'][record.rec_type] += 1

            if record.length:
                stats['total_bytes'] += record.length
            if record.rec_type == 'response' and record.http_headers:
                status = record.http_headers.get_statuscode()
                uri = record.rec_headers.get_header('WARC-Target-URI')
                if uri:
                    host = urlparse(uri).netloc
                    if host:
                        stats['hosts'][host] += 1
                if status:
                    stats['http_status_codes'][status] += 1

                content_type = record.http_headers.get_header('Content-Type')
                if content_type:
                    content_type = content_type.split(';')[0].strip()
                    stats['mime_types'][content_type] += 1

    
    # convert defaultdict objects back to Python dicts
    stats['record_types'] = dict(stats['record_types'])
    stats['mime_types'] = dict(stats['mime_types'])
    stats['http_status_codes'] = dict(stats['http_status_codes'])
    stats['hosts'] = dict(stats['hosts'])
    output = json.dumps(stats, indent=2)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
    else:
        print(output)

if __name__ == "__main__":
    main()
