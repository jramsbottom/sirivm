import sqlite3
import requests
import zipfile
from io import BytesIO
from lxml import etree
import os
import logging
from datetime import datetime

# Generate a timestamped log filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f'log_{timestamp}.log'

# Configure logging to file
logging.basicConfig(filename=log_filename, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure info and below are not printed to the console
logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.ERROR)



NS = {'siri': 'http://www.siri.org.uk/siri'}

def download_and_unzip(url, extract_path):
    response = requests.get(url)
    zip_file = zipfile.ZipFile(BytesIO(response.content))
    zip_file.extractall(extract_path)
    return zip_file.namelist()[0]


def parse_xml(file_path):
    tree = etree.parse(file_path)
    root = tree.getroot()
    data = []
    for vehicle_activity in root.findall('.//siri:VehicleActivity', NS):
        recorded_at_time = vehicle_activity.find('.//siri:RecordedAtTime', NS).text
        latitude = vehicle_activity.find('.//siri:VehicleLocation/siri:Latitude', NS).text
        longitude = vehicle_activity.find('.//siri:VehicleLocation/siri:Longitude', NS).text
        line_name_elem = vehicle_activity.find('.//siri:PublishedLineName', NS)
        line_name = line_name_elem.text if line_name_elem is not None else None
        operator_ref = vehicle_activity.find('.//siri:OperatorRef', NS).text
        vehicle_ref = vehicle_activity.find('.//siri:VehicleRef', NS).text

        journey_ref_1_elem = vehicle_activity.find('.//siri:FramedVehicleJourneyRef/siri:DatedVehicleJourneyRef', NS)
        journey_ref_2_elem = vehicle_activity.find('.//siri:VehicleJourneyRef', NS)

        # Safely attempt to access .text of journey_ref elements
        journey_ref_1 = journey_ref_1_elem.text if journey_ref_1_elem is not None else None
        journey_ref_2 = journey_ref_2_elem.text if journey_ref_2_elem is not None else None
        journey_ref = journey_ref_1 or journey_ref_2

        data.append((recorded_at_time, latitude, longitude, line_name, operator_ref, journey_ref, vehicle_ref))
    return data

def validate_data(data):
    valid_data = []
    for record in data:
        try:
            lat, long = float(record[1]), float(record[2])
            valid_data.append(record)
        except ValueError as e:
            logging.error(f"Data validation error: {e}")
    return valid_data

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Adjust the table creation commands to reflect the new schema
    c.execute('''CREATE TABLE IF NOT EXISTS Operators
                 (OperatorID INTEGER PRIMARY KEY AUTOINCREMENT, OperatorRef TEXT UNIQUE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS LineNames
                 (LineID INTEGER PRIMARY KEY AUTOINCREMENT, LineName TEXT, OperatorID INTEGER,
                 UNIQUE(LineName, OperatorID),
                 FOREIGN KEY(OperatorID) REFERENCES Operators(OperatorID))''')
    c.execute('''CREATE TABLE IF NOT EXISTS Journeys
                 (JourneyID INTEGER PRIMARY KEY AUTOINCREMENT, LineID INTEGER, JourneyRef TEXT,
                 FOREIGN KEY(LineID) REFERENCES LineNames(LineID))''')
    c.execute('''CREATE TABLE IF NOT EXISTS Positions
                 (PositionID INTEGER PRIMARY KEY AUTOINCREMENT, JourneyID INTEGER, RecordedAtTime TEXT,
                 Latitude TEXT, Longitude TEXT, VehicleRef TEXT,
                 FOREIGN KEY(JourneyID) REFERENCES Journeys(JourneyID))''')
    conn.commit()
    conn.close()

def insert_data(db_path, data):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for record in data:
        operator_ref = record[4]
        line_name = record[3] if record[3] else 'None'  # Handle empty LineName

        # Insert or find the OperatorID
        c.execute("INSERT OR IGNORE INTO Operators (OperatorRef) VALUES (?)", (operator_ref,))
        conn.commit()
        c.execute("SELECT OperatorID FROM Operators WHERE OperatorRef = ?", (operator_ref,))
        operator_id = c.fetchone()[0]

        # Insert or find the LineID with OperatorID
        c.execute("INSERT OR IGNORE INTO LineNames (LineName, OperatorID) VALUES (?, ?)", (line_name, operator_id))
        conn.commit()
        c.execute("SELECT LineID FROM LineNames WHERE LineName = ? AND OperatorID = ?", (line_name, operator_id))
        line_id = c.fetchone()[0]

        # Insert Journey using LineID and JourneyRef (removing incorrect OperatorID insertion)
        journey_ref = record[5]
        c.execute("INSERT INTO Journeys (LineID, JourneyRef) VALUES (?, ?)", (line_id, journey_ref))
        journey_id = c.lastrowid

        # Insert Position using JourneyID
        recorded_at_time, latitude, longitude, vehicle_ref = record[0], record[1], record[2], record[6]
        c.execute("INSERT INTO Positions (JourneyID, RecordedAtTime, Latitude, Longitude, VehicleRef) VALUES (?, ?, ?, ?, ?)", (journey_id, recorded_at_time, latitude, longitude, vehicle_ref))
        conn.commit()

    conn.close()

def process_data(zip_file_url, db_path, extract_path):
    init_db(db_path)
    xml_file_name = download_and_unzip(zip_file_url, extract_path)
    xml_file_path = os.path.join(extract_path, xml_file_name)
    data = parse_xml(xml_file_path)
    valid_data = validate_data(data)
    insert_data(db_path, valid_data)
    logging.info("Data processing and insertion completed.")
