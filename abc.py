import csv
import sqlite3

class DatabaseConnector:
    """
    Manages a connection to a SQLite database and populates it from CSV files.
    """

    def __init__(self, database_file):
        """
        Initialize the database connection.
        
        :param database_file: The path to the SQLite database file.
        """
        self.connection = sqlite3.connect(database_file)
        self.cursor = self.connection.cursor()

    def populate(self, spreadsheet_folder, filenames):
        """
        Populate the database with data imported from each spreadsheet.
        
        :param spreadsheet_folder: The folder where CSV files are located.
        :param filenames: A list of CSV filenames to process.
        """
        shipment_info = {}  # Dictionary to store shipment information

        # Read all CSV files and gather shipment information
        for filename in filenames:
            with open(f"{spreadsheet_folder}/{filename}", "r") as spreadsheet_file:
                csv_reader = csv.reader(spreadsheet_file)
                headers = next(csv_reader)  # Skip the header row

                # Process the first shipping CSV file differently
                if filename == "shipping_data_0.csv":
                    self.process_first_shipping_data(csv_reader)
                else:
                    # Process other shipping CSV files and store data
                    self.process_other_shipping_data(csv_reader, shipment_info)

        # Insert the aggregated shipment data into the database
        self.insert_shipment_data(shipment_info)

    def process_first_shipping_data(self, csv_reader):
        """
        Process data from the first shipping CSV file.
        
        :param csv_reader: The CSV reader for the first shipping data.
        """
        for row in csv_reader:
            # Extract relevant fields from the row
            product_name = row[2]  # Product name from the row
            product_quantity = row[4]  # Product quantity from the row
            origin = row[0]  # Shipment origin from the row
            destination = row[1]  # Shipment destination from the row

            # Insert the product into the database if it doesn't already exist
            self.insert_product_if_not_exists(product_name)
            # Insert the shipment information into the database
            self.insert_shipment(product_name, product_quantity, origin, destination)

    def process_other_shipping_data(self, csv_reader, shipment_info):
        """
        Process data from the second and third shipping CSV files.
        
        :param csv_reader: The CSV reader for other shipping data.
        :param shipment_info: Dictionary to store shipment information.
        """
        for row in csv_reader:
            shipment_identifier = row[0]  # Shipment identifier from the row
            # Check if this shipment identifier is already in the dictionary
            if shipment_identifier not in shipment_info:
                # Initialize a new entry for this shipment identifier
                shipment_info[shipment_identifier] = {
                    "products": {},  # Store product quantities in this dictionary
                    "origin": row[1],  # Shipment origin from the row
                    "destination": row[2]  # Shipment destination from the row
                }

            product_name = row[1]  # Product name from the row
            # Update the product quantity for this shipment
            shipment_info[shipment_identifier]["products"][product_name] = shipment_info[shipment_identifier]["products"].get(product_name, 0) + 1

    def insert_shipment_data(self, shipment_info):
        """
        Insert aggregated shipment data into the database.
        
        :param shipment_info: Dictionary containing aggregated shipment information.
        """
        for shipment_id, shipment in shipment_info.items():
            origin = shipment["origin"]  # Get the origin for this shipment
            destination = shipment["destination"]  # Get the destination for this shipment
            
            # Iterate through the products for this shipment
            for product_name, product_quantity in shipment["products"].items():
                # Insert the product into the database if it doesn't already exist
                self.insert_product_if_not_exists(product_name)
                # Insert the shipment information into the database
                self.insert_shipment(product_name, product_quantity, origin, destination)

    def insert_product_if_not_exists(self, product_name):
        """
        Insert a new product into the database if it does not already exist.
        
        :param product_name: The name of the product to insert.
        """
        query = "INSERT OR IGNORE INTO product (name) VALUES (?);"  # SQL query to insert a product
        self.cursor.execute(query, (product_name,))  # Execute the query with the product name
        self.connection.commit()  # Commit the transaction to the database

    def insert_shipment(self, product_name, product_quantity, origin, destination):
        """
        Insert a new shipment into the database.
        
        :param product_name: The name of the product being shipped.
        :param product_quantity: The quantity of the product being shipped.
        :param origin: The origin of the shipment.
        :param destination: The destination of the shipment.
        """
        # Query to fetch the product ID from the database
        query = "SELECT id FROM product WHERE name = ?;"
        self.cursor.execute(query, (product_name,))  # Execute the query
        result = self.cursor.fetchone()  # Fetch the result

        if result is not None:
            product_id = result[0]  # Get the product ID
            # SQL query to insert the shipment into the database
            query = """
                INSERT INTO shipment (product_id, quantity, origin, destination)
                VALUES (?, ?, ?, ?);
            """
            self.cursor.execute(query, (product_id, product_quantity, origin, destination))  # Execute the query
            self.connection.commit()  # Commit the transaction to the database

    def close(self):
        """Close the database connection."""
        self.connection.close()


if __name__ == '__main__':
    # List of CSV filenames to process
    filenames = ["shipping_data_0.csv", "shipping_data_1.csv", "shipping_data_2.csv"]
    # Initialize the database connector
    database_connector = DatabaseConnector("shipment_database.db")
    # Populate the database with data from CSV files
    database_connector.populate("./data", filenames)
    # Close the database connection
    database_connector.close()
