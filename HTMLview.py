import sqlite3
from datetime import datetime

# Update this path to your actual SQLite database file
db_path = r'C:\Users\benja\PycharmProjects\sirivm\vehicles_20240212_210744.db'
output_filename = f'transport_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'


def fetch_data(query, params=()):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows


def generate_html():
    operators = fetch_data("SELECT OperatorID, OperatorRef FROM Operators")

    with open(output_filename, 'w') as file:
        file.write(
            '<html><head><title>Transport Data</title><style>body {font-family: Arial, sans-serif;}</style></head><body>')
        file.write('<h1 id="top">Operators</h1><ul>')

        # List all operators with links to detailed sections
        for operator_id, operator_ref in operators:
            file.write(f'<li><a href="#operator_{operator_id}">{operator_ref}</a></li>')
        file.write('</ul>')

        # Detailed sections for each operator
        for operator_id, operator_ref in operators:
            file.write(f'<h2 id="operator_{operator_id}">{operator_ref}</h2>')
            file.write('<a href="#top">Back to Operators</a><ul>')

            lines = fetch_data("SELECT LineID, LineName FROM LineNames WHERE OperatorID=?", (operator_id,))
            for line_id, line_name in lines:
                file.write(f'<li><a href="#line_{line_id}">{line_name}</a></li>')
            file.write('</ul>')

            # Detailed sections for each line under the current operator
            for line_id, line_name in lines:
                file.write(f'<h3 id="line_{line_id}">{line_name}</h3>')
                file.write(f'<a href="#operator_{operator_id}">Back to {operator_ref}</a><ul>')

                journeys = fetch_data("SELECT JourneyID, JourneyRef FROM Journeys WHERE LineID=?", (line_id,))
                for journey_id, journey_ref in journeys:
                    sql_query = f"SELECT Latitude, Longitude FROM Positions WHERE JourneyID={journey_id}"
                    file.write(f'<li>{journey_ref} - SQL Query: <code>{sql_query}</code></li>')
                file.write('</ul>')

        file.write('</body></html>')


generate_html()
print(f"HTML file generated: {output_filename}")
