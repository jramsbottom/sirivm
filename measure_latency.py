import sqlite3
from datetime import datetime
import logging


def parse_timestamp(timestamp_str):
    """Parse an ISO 8601 timestamp string to a datetime object."""
    try:
        return datetime.fromisoformat(timestamp_str.rstrip('Z'))
    except ValueError as e:
        logging.error(f"Timestamp parsing error: {e}")
        return None


def fetch_data_from_db(db_path):
    """Fetch RecordedAtTime, ResponseTimestamp, Latitude, Longitude, and VehicleRef from the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT RecordedAtTime, ResponseTimestamp, Latitude, Longitude, VehicleRef FROM Positions")
    data = cursor.fetchall()
    conn.close()
    return data



def find_record_with_smallest_time_difference(data):
    """Find the record with the smallest difference between RecordedAtTime and ResponseTimestamp."""
    smallest_diff = None
    record_with_smallest_diff = None

    for record in data:
        recorded_at_time, response_timestamp = record[:2]  # Assuming these are the first two fields
        recorded_at_time = parse_timestamp(recorded_at_time)
        response_timestamp = parse_timestamp(response_timestamp)

        if recorded_at_time and response_timestamp:
            time_diff = abs(recorded_at_time - response_timestamp)

            if smallest_diff is None or time_diff < smallest_diff:
                smallest_diff = time_diff
                record_with_smallest_diff = record

    return record_with_smallest_diff

def main():
    db_path = r'C:\Users\benja\PycharmProjects\sirivm\vehicles_20240218_192804.db'
    data = fetch_data_from_db(db_path)

    if not data:
        print("No data found in the database.")
        return

    record_with_smallest_diff = find_record_with_smallest_time_difference(data)
    if record_with_smallest_diff:
        # Assuming the record structure as (RecordedAtTime, ResponseTimestamp, Latitude, Longitude, VehicleRef, ...)
        # Adjust the indices if your actual data structure is different
        recorded_at_time = parse_timestamp(record_with_smallest_diff[0])
        response_timestamp = parse_timestamp(record_with_smallest_diff[1])
        vehicle_ref = record_with_smallest_diff[4]  # Assuming VehicleRef is at index 4

        # Calculate the latency (difference) between RecordedAtTime and ResponseTimestamp
        latency = abs(recorded_at_time - response_timestamp) if recorded_at_time and response_timestamp else None

        print("Record with the smallest time difference:")
        print(f"RecordedAtTime: {record_with_smallest_diff[0]}")
        print(f"ResponseTimestamp: {record_with_smallest_diff[1]}")
        print(f"VehicleRef: {vehicle_ref}")
        if latency is not None:
            print(f"Calculated Latency: {latency.total_seconds()} seconds")
        else:
            print("Latency calculation error.")
    else:
        print("No valid record found with a time difference.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    main()

if __name__ == "__main__":
    main()