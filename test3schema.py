import sqlite3

# Connect to the database
conn = sqlite3.connect('shipment_database.db')
cursor = conn.cursor()

# Query table info for "shipment" table (replace "shipment" with your table name if different)
cursor.execute("PRAGMA table_info(shipment);")
columns = cursor.fetchall()

# Display the column details
for column in columns:
    print(column)

# Close the connection
conn.close()
