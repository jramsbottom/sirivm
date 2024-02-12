import sqlite3
import csv
from datetime import datetime

# Update this path to your actual SQLite database file
db_path = r'C:\Users\benja\PycharmProjects\sirivm\vehicles_20240212_210744.db'
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f'operators_lines_journeys_{timestamp}.csv'

def fetch_data():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = """
    SELECT o.OperatorRef, ln.LineName, j.JourneyRef
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
        writer.writerow(['OperatorRef', 'LineName', 'JourneyRef'])  # Write the header row
        writer.writerows(data)  # Write the data rows

generate_csv()
print(f"CSV file generated: {csv_filename}")
