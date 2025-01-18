"""
data_profiler.py

This script performs initial exploration of a CSV dataset using Pandas. 
It is crucial to explore the data with Pandas before loading it into a database 
(like MongoDB) because:

1. Data Understanding:
    - Inspecting column names and data types is essential before data loading to ensure 
    they are properly recognized in the database. This also enables us to identify if any
    columns needs to be transformed before the insertion.

2. Data Quality Assessment:
    - Identify missing values (NaNs) which can impact data processing and database operations.
    - Find inconsistencies in data format (e.g., date formats, currency symbols).
    - Observe data distributions and identify outliers which can later cause problems if not handled properly.

3. Data Cleaning Planning:
    - Pandas makes it easier to assess which data transformations will be needed to prepare 
    the data for database loading, for example, parsing dates, stripping symbols from numerical values, 
    converting to other types.

4. Efficient Planning:
    - Data exploration helps to make decisions regarding the data types to use in the database.
    - Understanding the size and structure of the dataset is essential to optimize data loading.

5. Iterative Development:
    - It allows for an iterative process, where we explore the data, understand it better, make decisions 
    about transformations, and then implement them with more accuracy.

In short, Pandas exploration provides a vital foundation for writing effective and accurate 
data loading scripts, avoiding common pitfalls and allowing us to make informed decisions for 
the next steps.
"""

import pandas as pd
import argparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def explore_csv(csv_path):
    """
    Explores a CSV file using Pandas to understand its structure and contents.

    Args:
        csv_path (str): The path to the CSV file.
    """

    try:
        logging.info(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)

        logging.info("\n--- Basic Information ---")
        logging.info(f"Shape: {df.shape}")
        logging.info(f"Data types:\n{df.dtypes}")
        
        logging.info("\n--- Sample Data ---")
        logging.info(f"First 5 rows:\n{df.head()}")
        logging.info(f"Last 5 rows:\n{df.tail()}")
        
        logging.info("\n--- Missing Values ---")
        logging.info(f"Missing value counts:\n{df.isnull().sum()}")

        logging.info("\n--- Descriptive Statistics ---")
        logging.info(f"Numerical columns statistics:\n{df.describe()}")

        # Example: Exploring Categorical Columns
        for column in df.columns:
            if df[column].dtype == 'object':  # Check if it's a string/object column
                logging.info(f"\n--- Column: {column} ---")
                logging.info(f"Unique values:\n{df[column].unique()}")
                logging.info(f"Value counts:\n{df[column].value_counts().head(10)}") # first 10 values

        logging.info("\n--- Data exploration complete ---")

    except FileNotFoundError:
        logging.error(f"Error: CSV file not found at: {csv_path}")
    except Exception as e:
         logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Explore a CSV file using Pandas.")
    parser.add_argument("csv_path", type=str, help="Path to the CSV file.")
    args = parser.parse_args()
    explore_csv(args.csv_path)