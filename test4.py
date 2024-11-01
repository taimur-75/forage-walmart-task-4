import sqlite3

# Path to the SQLite database
database_path = "./shipment_database.db"

# Connect to the database
conn = sqlite3.connect(database_path)
cursor = conn.cursor()

# Fetch and display the contents of the shipment table
cursor.execute("SELECT * FROM shipment;")
rows = cursor.fetchall()

# Check if there are any rows
if rows:
    print("Data in shipment table:")
    for row in rows:
        print(row)
else:
    print("No data in shipment table.")

# Close the connection
conn.close()
