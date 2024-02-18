import requests
from transform_load import validate_data, extract_data, parse_xml, init_db, insert_data, init_cache
import logging

def fetch_api(url, db_path, params):
    init_db(db_path)  # Ensure the database is initialized
    cache = init_cache(db_path)  # Initialize the cache with existing positions

    response = requests.get(url, params=params)
    if response.status_code != 200:
        logging.error(f"Failed to fetch data from API: {response.status_code}")
        return

    # Assuming parse_xml is capable of handling string input directly for XML content
    data = parse_xml(response.content, source_type='string')

    if data:
        valid_data = validate_data(data)
        insert_data(db_path, valid_data, cache)  # Pass the cache to insert_data
        logging.info("Data processing and insertion completed.")
    else:
        logging.error("No data processed due to API fetch error.")
