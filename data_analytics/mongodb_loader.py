"""
mongodb_loader.py

This script is responsible for loading procurement data from a CSV file into a MongoDB Atlas database.
It performs several crucial tasks to prepare the data for analysis and storage.

Key Features:
1. MongoDB Atlas Connection:
    - Connects to a MongoDB Atlas cluster using a connection URI constructed from environment variables.
        - The required variables are MONGODB_USERNAME, MONGODB_PASSWORD, and MONGODB_CLUSTER_URL
        - The URI is constructed by adding the credentials to the `mongodb+srv` string, and encoding them with `urllib.parse.quote_plus` to handle special characters in them.

2. Data Loading from CSV:
    - Reads data from a CSV file using the pandas library, using `pd.read_csv`.
    - The number of rows to load can be controlled by passing the `--max-rows` command-line parameter.
    - The script also includes a `try-except` statement that makes sure the execution fails gracefully in case any error happens in this operation.

3. Data Cleaning and Transformation:
    - Implements the `clean_price()` method, which remove the currency symbols (e.g., '$') and ensures that the value is properly converted to a float for storage purposes.
    - Implements `parse_date()` to properly parse the date strings on the CSV to Python's `datetime` objects.
    - Handles numerical types using `safe_int` to make sure that `NaN` or `None` values are handled by setting them to zero.
    - Includes the `transform_row()` method, which performs all of the previously described operations and normalizes the names into `snake_case`.

4. Data Insertion into MongoDB:
    - Drops the existing collection, which makes sure the data is always reloaded from the start.
    - Transforms each row using the `transform_row` method to make sure the data is ready to be loaded into the database.
    - Inserts the transformed data into a MongoDB collection in batches (by default with a size of 10000), to avoid any potential memory issues when loading large files.
    - The loading of the data is logged to the console, so the user can see that the data is being processed.
    - Before indexing the database, the current database size is also logged so the user can monitor the quota usage.

5. Data Indexing:
    - Creates essential indexes on key fields (`creation_date`, `department_name`, `supplier_name`, `acquisition_type`), so that the query speed will be improved.

6. Logging:
    - Uses the `logging` module to track progress and any issues, with the timestamp, logging level and messages, which makes the execution easy to follow.
    - The script logs information about:
        - Dropping the existing collection.
        - CSV reading, including number of rows read.
        - Batched insert operations
        - Database size.
        - Index creation
        - Data loading completion.
        - It also logs any errors that may happen during the execution

7. Command Line Interface:
    - Implements a command line interface using `argparse`, which allows the path to the csv to be passed as a positional parameter and the number of rows to load with `--max-rows`. This makes the script more flexible and easier to use.

Workflow:
    1. Ensure you have a CSV file of procurement data, which was analysed previously using `data_profiler.py` to understand its contents.
    2. Ensure your environment variables for MONGODB_USERNAME, MONGODB_PASSWORD, and MONGODB_CLUSTER_URL are correctly set.
    3. Execute the script from the command line by passing the path to the CSV and the number of rows to load.

This script ensures the consistent and effective loading of procurement data into MongoDB Atlas. It's a critical step in the overall data analysis pipeline, preparing the data to be analyzed with the `procurement_analyzer.py` script.
"""
import pandas as pd
from pymongo import MongoClient
import logging
from datetime import datetime
import urllib.parse
import os
# Load environment variables
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProcurementDataLoader:
    def __init__(self, csv_path: str, max_rows: int = 200000):
        # MongoDB Atlas connection parameters
        username = os.getenv('MONGODB_USERNAME')
        password = os.getenv('MONGODB_PASSWORD')
        cluster_url = os.getenv('MONGODB_CLUSTER_URL')
        
        # Added max_rows parameter
        self.max_rows = max_rows
        
        # Encode credentials for URL
        encoded_username = urllib.parse.quote_plus(username)
        encoded_password = urllib.parse.quote_plus(password)
        
        # Construct the full connection string
        self.mongodb_uri = f"mongodb+srv://{encoded_username}:{encoded_password}@{cluster_url}/?retryWrites=true&w=majority&appName=Procurement"
        
        self.csv_path = csv_path
        self.client = MongoClient(self.mongodb_uri)
        self.db = self.client.procurement_db
        self.collection = self.db.procurement_data

    def clean_price(self, price_str):
        """Clean price strings by removing $ and converting to float"""
        try:
            if isinstance(price_str, str):
                return float(price_str.replace('$', '').replace(',', '').strip() or 0)
            return float(price_str) if price_str else 0.0
        except (ValueError, TypeError):
            return 0.0

    def parse_date(self, date_str):
        """Parse date strings into datetime objects"""
        if pd.isna(date_str):
            return None
        try:
            parsed_date = pd.to_datetime(date_str)
            return parsed_date.to_pydatetime() if not pd.isna(parsed_date) else None
        except:
            return None

    def safe_int(self, value, default=0):
        """Safely convert value to int, handling NaN and None"""
        try:
            if pd.isna(value):
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def transform_row(self, row):
        """Transform a row into the correct data types with snake_case field names"""
        return {
            # Date fields
            "creation_date": self.parse_date(row.get("Creation Date")),
            "purchase_date": self.parse_date(row.get("Purchase Date")),
            "fiscal_year": row.get("Fiscal Year"),
            
            # Reference Numbers
            "lpa_number": row.get("LPA Number"),
            "purchase_order_number": row.get("Purchase Order Number"),
            "requisition_number": row.get("Requisition Number"),
            
            # Acquisition Info
            "acquisition_type": row.get("Acquisition Type"),
            "sub_acquisition_type": row.get("Sub-Acquisition Type"),
            "acquisition_method": row.get("Acquisition Method"),
            "sub_acquisition_method": row.get("Sub-Acquisition Method"),
            
            # Organization Info
            "department_name": row.get("Department Name"),
            "location": row.get("Location"),
            
            # Supplier Info
            "supplier_code": self.safe_int(row.get("Supplier Code")),
            "supplier_name": row.get("Supplier Name"),
            "supplier_qualifications": row.get("Supplier Qualifications"),
            "supplier_zip_code": row.get("Supplier Zip Code"),
            "calcard": row.get("CalCard"),
            
            # Item Details
            "item_name": row.get("Item Name"),
            "item_description": row.get("Item Description"),
            "quantity": float(row.get("Quantity", 0) or 0),
            "unit_price": self.clean_price(row.get("Unit Price")),
            "total_price": self.clean_price(row.get("Total Price")),
            
            # Classification
            "classification_codes": [code.strip() for code in str(row.get("Classification Codes", "")).split('\n') if code.strip()],
            "normalized_unspsc": str(row.get("Normalized UNSPSC", "")),
            "commodity_title": row.get("Commodity Title"),
            "class": str(row.get("Class", "")),
            "class_title": row.get("Class Title"),
            "family": str(row.get("Family", "")),
            "family_title": row.get("Family Title"),
            "segment": str(row.get("Segment", "")),
            "segment_title": row.get("Segment Title")
        }

    def load_data(self):
        """Load data from CSV into MongoDB"""
        try:
            # Drop existing collection
            self.collection.drop()
            logger.info("Dropped existing collection")

            # Read CSV with nrows parameter to limit rows
            logger.info(f"Reading CSV from {self.csv_path} (max {self.max_rows} rows)")
            df = pd.read_csv(self.csv_path, nrows=self.max_rows)
            logger.info(f"Read {len(df)} rows")

            # Transform and insert data
            documents = []
            for _, row in df.iterrows():
                doc = self.transform_row(row.to_dict())
                documents.append(doc)

            if documents:
                # Insert in smaller batches to avoid memory issues
                batch_size = 10000
                for i in range(0, len(documents), batch_size):
                    batch = documents[i:i + batch_size]
                    self.collection.insert_many(batch)
                    logger.info(f"Inserted batch of {len(batch)} documents into MongoDB")

            # Check database size before creating indexes
            stats = self.db.command("collstats", self.collection.name)
            size_mb = stats['size'] / (1024 * 1024)
            logger.info(f"Current database size: {size_mb:.2f}MB")
            
            if size_mb > 450:  # Leave buffer for indexes
                logger.warning(f"Database size ({size_mb:.2f}MB) approaching quota")
            
            # Create indexes
            logger.info("Creating indexes...")
            self.collection.create_index("creation_date")
            self.collection.create_index("department_name")
            self.collection.create_index("supplier_name")
            self.collection.create_index("acquisition_type")
            
            logger.info("Data loading complete!")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Load procurement data into MongoDB Atlas')
    parser.add_argument('csv_path', type=str, help='Path to the CSV file')
    parser.add_argument('--max-rows', type=int, default=200000, help='Maximum number of rows to load (default: 200000)')
    
    args = parser.parse_args()
    
    loader = ProcurementDataLoader(args.csv_path, max_rows=args.max_rows)
    loader.load_data()