import requests
import zipfile
from io import BytesIO
from transform_load import validate_data,extract_data,parse_xml,init_db, insert_data, init_cache
import os
import logging
import sqlite3

def download_and_unzip(url, extract_path):
    response = requests.get(url)
    zip_file = zipfile.ZipFile(BytesIO(response.content))
    zip_file.extractall(extract_path)
    return zip_file.namelist()[0]


def process_data(zip_file_url, db_path, extract_path):
    init_db(db_path)
    cache = init_cache(db_path)  # Initialize the cache
    xml_file_name = download_and_unzip(zip_file_url, extract_path)
    xml_file_path = os.path.join(extract_path, xml_file_name)
    data = parse_xml(xml_file_path, source_type='file')
    valid_data = validate_data(data)
    insert_data(db_path, valid_data, cache)  # Pass the cache to insert_data
    logging.info("Data processing and insertion completed.")


