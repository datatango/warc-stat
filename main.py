from warcio.archiveiterator import ArchiveIterator
import argparse

def main():
    parser = argparse.ArgumentParser(description='Process WARC file')
    parser.add_argument('warc_file', help='Path to the WARC file')
    parser.add_argument('-o', '--output', help='Output file path')

    args = parser.parse_args()
    warc_path = args.warc_file

    print(f"Processing: {warc_path}")

if __name__ == "__main__":
    main()
