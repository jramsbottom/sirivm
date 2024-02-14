import logging
from api_processor import process_data
import time
from datetime import datetime
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Fetch the API Key from an Environment Variable
API_KEY = os.environ.get('BODS_API_KEY')
if not API_KEY:
    raise ValueError("API key not found. Set the BODS_API_KEY environment variable.")

# API Request Parameters
URL = "https://data.bus-data.dft.gov.uk/api/v1/datafeed"
PARAMS = {
    'boundingBox': '2.93,53.374,-3.085,53.453',
    'api_key': API_KEY
}

# Generate a timestamped database filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
DB_PATH = f'C:\\Users\\benja\\PycharmProjects\\sirivm\\vehicles_{timestamp}.db'

def main():
    try:
        while True:
            logging.info("Starting data processing...")
            # Update this call according to the actual parameters your `process_data` function expects
            process_data(URL, DB_PATH, PARAMS)  # Assuming `process_data` has been updated accordingly
            logging.info("Data processing and insertion completed. Waiting for next run...")
            time.sleep(10)  # Adjust the sleep time as necessary
    except KeyboardInterrupt:
        logging.info("Process terminated by user.")

if __name__ == '__main__':
    main()
