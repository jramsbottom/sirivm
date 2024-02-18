import sqlite3
import csv
from datetime import datetime

# Update this path to your actual SQLite database file
db_path = r'C:\Users\benja\PycharmProjects\sirivm\vehicles_20240218_192804.db'
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f'operators_lines_journeys_{timestamp}.csv'

def fetch_data():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Update the query to include DateOfJourney
    query = """
    SELECT o.OperatorRef, ln.LineName, j.JourneyRef, j.DateOfJourney
    FROM Journeys j
    JOIN LineNames ln ON j.LineID = ln.LineID
    JOIN Operators o ON ln.OperatorID = o.OperatorID
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return rows

def generate_csv():
    data = fetch_data()
    with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Update the header row to include JourneyDate
        writer.writerow(['OperatorRef', 'LineName', 'JourneyRef', 'JourneyDate'])  # Write the header row
        for row in data:
            # Convert DateOfJourney to a string if it's a datetime object, or handle as is if it's already a string
            row = list(row)  # Convert tuple to list to be able to modify it
            row[3] = row[3].strftime("%Y-%m-%d") if isinstance(row[3], datetime) else row[3]
            writer.writerow(row)  # Write the data rows

generate_csv()
print(f"CSV file generated: {csv_filename}")
