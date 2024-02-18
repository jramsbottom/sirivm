import argparse
import logging
from datetime import datetime
import os
import time
from zip_extract import process_data as process_zip_data  # Renamed for clarity
from api_extract import fetch_api

def main():
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(description='Process XML data from ZIP or API.')
    parser.add_argument('mode', choices=['zip', 'api'],
                        help='Choose "zip" to process data from a ZIP file, or "api" to process data from an API.')

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Generate a timestamped database filename once before the loop
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_path = f'vehicles_{timestamp}.db'

    try:
        while True:
            if args.mode == 'zip':
                zip_file_url = 'https://data.bus-data.dft.gov.uk/avl/download/bulk_archive'
                extract_path = f'extracted_{timestamp}/'
                # Ensure the extraction directory exists
                if not os.path.exists(extract_path):
                    os.makedirs(extract_path)
                # Start ZIP processing
                logging.info("Starting ZIP data processing...")
                process_zip_data(zip_file_url, db_path, extract_path)
                logging.info("ZIP data processing and insertion completed. Waiting for next run...")

            elif args.mode == 'api':
                # Fetch the API Key from an Environment Variable
                api_key = os.environ.get('BODS_API_KEY')
                if not api_key:
                    raise ValueError("API key not found. Set the BODS_API_KEY environment variable.")

                # API Request Parameters
                url = "https://data.bus-data.dft.gov.uk/api/v1/datafeed"
                params = {
                    'boundingBox': '2.93,53.374,-3.085,53.453',
                    'api_key': api_key
                }
                # Start API processing
                logging.info("Starting API data processing...")
                fetch_api(url, db_path, params)
                logging.info("API data processing and insertion completed. Waiting for next run...")

            time.sleep(10)  # Adjust the sleep time as necessary

    except KeyboardInterrupt:
        logging.info("Process terminated by user.")

if __name__ == '__main__':
    main()
