import logging
from processor import process_data
import time
from datetime import datetime
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ZIP_FILE_URL = 'https://data.bus-data.dft.gov.uk/avl/download/bulk_archive'

# Generate a timestamped database filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
DB_PATH = f'C:\\Users\\benja\\PycharmProjects\\sirivm\\vehicles_{timestamp}.db'

# Generate a timestamped extraction path
EXTRACT_PATH = f'C:\\Users\\benja\\PycharmProjects\\sirivm\\extracted_{timestamp}\\'

# Ensure the extraction directory exists
if not os.path.exists(EXTRACT_PATH):
    os.makedirs(EXTRACT_PATH)

def main():
    try:
        while True:
            logging.info("Starting data processing...")
            process_data(ZIP_FILE_URL, DB_PATH, EXTRACT_PATH)
            logging.info("Data processing and insertion completed. Waiting for next run...")
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Process terminated by user.")

if __name__ == '__main__':
    main()
