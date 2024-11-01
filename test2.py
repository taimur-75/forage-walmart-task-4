import sqlite3

conn = sqlite3.connect('shipment_database.db')
cursor = conn.cursor()

# Count rows in the shipment table
cursor.execute("SELECT COUNT(*) FROM shipment;")
count = cursor.fetchone()[0]

print("Number of rows in shipment table:", count)

conn.close()
