import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime


# Function to parse ISO 8601 timestamps
def parse_timestamp(timestamp_str):
    try:
        return datetime.fromisoformat(timestamp_str.rstrip('Z'))
    except ValueError as e:
        print(f"Timestamp parsing error: {e}")
        return None


# Function to fetch data from the database
def fetch_data_with_operator(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = """
    SELECT p.RecordedAtTime, p.ResponseTimestamp, o.OperatorRef
    FROM Positions p
    JOIN Journeys j ON p.JourneyID = j.JourneyID
    JOIN LineNames ln ON j.LineID = ln.LineID
    JOIN Operators o ON ln.OperatorID = o.OperatorID
    """
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return data


# Function to process data and calculate latencies
def process_data(data):
    processed_data = []
    for record in data:
        recorded_at_time = parse_timestamp(record[0])
        response_timestamp = parse_timestamp(record[1])
        operator_ref = record[2]
        if recorded_at_time and response_timestamp:
            latency_seconds = (response_timestamp - recorded_at_time).total_seconds()
            latency_seconds = min(latency_seconds, 120)  # Cap the latency at 120 seconds
            processed_data.append((recorded_at_time, response_timestamp, latency_seconds, operator_ref))
    return processed_data


# Main function to generate visualizations and output CSV
def main():
    db_path = r'C:\Users\benja\PycharmProjects\sirivm\vehicles_20240223_133219.db'  # Update this with your actual database path
    raw_data = fetch_data_with_operator(db_path)
    processed_data = process_data(raw_data)

    # Convert to DataFrame
    df = pd.DataFrame(processed_data, columns=['RecordedAtTime', 'ResponseTimestamp', 'Latency', 'OperatorRef'])
    df_filtered = df[df['Latency'] <= 120]

    # Bar chart for average latency per operator
    avg_latency = df_filtered.groupby('OperatorRef')['Latency'].mean().sort_values()
    avg_latency.plot(kind='bar', figsize=(12, 6), color='skyblue')
    plt.title('Average Latency by Operator (<= 120 seconds)')
    plt.xlabel('Operator')
    plt.ylabel('Average Latency (seconds)')
    plt.savefig('average_latency_per_operator.png')
    plt.close()

    # Histogram of latency distribution
    plt.figure(figsize=(12, 6))
    plt.hist(df_filtered['Latency'], bins=np.arange(0, 121), color='skyblue', edgecolor='black')
    plt.title('Latency Distribution (capped at 120 seconds)')
    plt.xlabel('Latency (seconds)')
    plt.ylabel('Count')
    plt.savefig('latency_distribution_histogram.png')
    plt.close()


    # Save the top 1000 rows to a CSV file
    top_1000 = df.nlargest(1000, 'Latency')
    top_1000.to_csv('latency_data_top_1000.csv', index=False)


# Run the main function
if __name__ == "__main__":
    main()
