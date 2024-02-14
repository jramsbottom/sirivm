import requests
import sqlite3
from lxml import etree
import logging
from datetime import datetime

# Setup logging as before
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f'log_{timestamp}.log'
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.INFO)

NS = {'siri': 'http://www.siri.org.uk/siri'}

def fetch_and_parse_api(url, params):
    response = requests.get(url, params=params)
    if response.status_code != 200:
        logging.error(f"Failed to fetch data from API: {response.status_code}")
        return []
    # Parse the XML directly from the response content
    root = etree.fromstring(response.content)
    data = []
    for vehicle_activity in root.findall('.//siri:VehicleActivity', NS):
        # Extract data as before
        recorded_at_time, latitude, longitude, line_name, operator_ref, vehicle_ref, journey_ref = extract_data(vehicle_activity)
        data.append((recorded_at_time, latitude, longitude, line_name, operator_ref, journey_ref, vehicle_ref))
    return data

def extract_data(vehicle_activity):
    # Extracts data from a vehicle activity element, this function is to keep the code DRY
    recorded_at_time = vehicle_activity.find('.//siri:RecordedAtTime', NS).text
    latitude = vehicle_activity.find('.//siri:VehicleLocation/siri:Latitude', NS).text
    longitude = vehicle_activity.find('.//siri:VehicleLocation/siri:Longitude', NS).text
    line_name_elem = vehicle_activity.find('.//siri:PublishedLineName', NS)
    line_name = line_name_elem.text if line_name_elem is not None else None
    operator_ref = vehicle_activity.find('.//siri:OperatorRef', NS).text
    vehicle_ref = vehicle_activity.find('.//siri:VehicleRef', NS).text

    journey_ref_1_elem = vehicle_activity.find('.//siri:FramedVehicleJourneyRef/siri:DatedVehicleJourneyRef', NS)
    journey_ref_2_elem = vehicle_activity.find('.//siri:VehicleJourneyRef', NS)
    journey_ref_1 = journey_ref_1_elem.text if journey_ref_1_elem is not None else None
    journey_ref_2 = journey_ref_2_elem.text if journey_ref_2_elem is not None else None
    journey_ref = journey_ref_1 or journey_ref_2

    return recorded_at_time, latitude, longitude, line_name, operator_ref, vehicle_ref, journey_ref

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
        operator_ref, line_name, journey_ref = record[4], record[3] if record[3] else 'None', record[5]
        recorded_at_time, latitude, longitude, vehicle_ref = record[0], record[1], record[2], record[6]

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

        # Check if a journey with the same LineID and JourneyRef already exists
        c.execute("SELECT JourneyID FROM Journeys WHERE LineID = ? AND JourneyRef = ?", (line_id, journey_ref))
        journey = c.fetchone()
        if journey:
            journey_id = journey[0]
        else:
            # Insert Journey using LineID and JourneyRef
            c.execute("INSERT INTO Journeys (LineID, JourneyRef) VALUES (?, ?)", (line_id, journey_ref))
            conn.commit()
            journey_id = c.lastrowid

        # Check if a position with the same JourneyID, RecordedAtTime, Latitude, and Longitude already exists
        c.execute("SELECT PositionID FROM Positions WHERE JourneyID = ? AND RecordedAtTime = ? AND Latitude = ? AND Longitude = ?", (journey_id, recorded_at_time, latitude, longitude))
        position = c.fetchone()
        if not position:
            # Insert Position using JourneyID
            c.execute("INSERT INTO Positions (JourneyID, RecordedAtTime, Latitude, Longitude, VehicleRef) VALUES (?, ?, ?, ?, ?)", (journey_id, recorded_at_time, latitude, longitude, vehicle_ref))
            conn.commit()

    conn.close()


def process_data(url, db_path, params):
    init_db(db_path)

    # Fetch XML data directly from the API without trying to unzip
    response = requests.get(url, params=params)
    if response.status_code != 200:
        logging.error(f"Failed to fetch data from API: {response.status_code}")
        return

    # Parse the XML directly from the response content
    root = etree.fromstring(response.content)
    data = []
    for vehicle_activity in root.findall('.//siri:VehicleActivity', NS):
        recorded_at_time, latitude, longitude, line_name, operator_ref, vehicle_ref, journey_ref = extract_data(
            vehicle_activity)
        data.append((recorded_at_time, latitude, longitude, line_name, operator_ref, journey_ref, vehicle_ref))

    if data:
        valid_data = validate_data(data)
        insert_data(db_path, valid_data)
        logging.info("Data processing and insertion completed.")
    else:
        logging.error("No data processed due to API fetch error.")
