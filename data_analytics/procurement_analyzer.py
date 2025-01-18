"""
procurement_analyzer.py

This script provides a powerful tool for exploring and analyzing procurement data stored in MongoDB Atlas.
It leverages MongoDB's aggregation framework to perform complex analytical tasks efficiently.
The script is designed to be modular and reusable, following a common data analysis workflow:
initial data exploration (using pandas in explore_csv.py), data loading to MongoDB (data_loader.py),
and then finally this comprehensive data analysis (ProcurementExplorer).

Key Features:
1. Connection to MongoDB Atlas:
   - Connects to a MongoDB Atlas cluster using a connection URI provided as a command-line argument.

2. Comprehensive Data Analysis:
    - Analyzes basic statistics (total records, unique values in key fields) by querying the data.
    - Detects data quality issues such as missing values and price calculation inconsistencies by building pipelines.
    - Explores temporal patterns by using aggregations to extract the minimum and maximum dates.
    - Calculates important financial metrics (total spend, average unit price, etc.) by using aggregations.
    - Identifies the most frequent values in categorical fields, using sorting and limiting.

3. Output Flexibility:
   - Allows outputting results in multiple formats (JSON, TXT, PDF) for easier sharing and integration with other tools.
   - Saves analysis results to a specified output directory.

4. Modular Design:
   - Each analytical task is encapsulated in its own method, making the code more readable and maintainable.

5. Logging and Error Handling:
    - Includes logging to record information and errors during execution.
    - Includes `try...except` blocks to gracefully handle potential exceptions.

6. Command Line Interface:
    - Uses `argparse` to receive input arguments for URI and output format, making the script flexible and easy to use.

Workflow:
    1. Ensure the data has been loaded into MongoDB Atlas by using `data_loader.py`
    2. Use the connection string provided by the Atlas console
    3. Run this script using the required parameters.

This script serves as the final stage of a comprehensive workflow, completing an initial exploration, loading of the data to a database and finally the analysis to draw insight from the data.
"""
import logging
from pymongo import MongoClient
from datetime import datetime
from pathlib import Path
import json
import numpy as np
from typing import Dict, Any
from output_handler import OutputHandler
import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProcurementExplorer:
    def __init__(self, mongodb_uri: str):
        """Initialize MongoDB connection to Atlas using a URI."""
        # Retrieve MongoDB Atlas credentials from environment variables
        username = os.getenv('MONGODB_USERNAME')
        password = os.getenv('MONGODB_PASSWORD')
        cluster_url = os.getenv('MONGODB_CLUSTER_URL')

        # Encode credentials for URL
        encoded_username = urllib.parse.quote_plus(username)
        encoded_password = urllib.parse.quote_plus(password)
        
        # Construct the full connection string
        self.mongodb_uri = f"mongodb+srv://{encoded_username}:{encoded_password}@{cluster_url}/?retryWrites=true&w=majority&appName=Procurement"

        self.client = MongoClient(self.mongodb_uri)
        self.db = self.client.procurement_db
        self.collection = self.db.procurement_data
        self.output_dir = Path("analysis_output")
        self.output_dir.mkdir(exist_ok=True)

    def clean_price(self, price_str) -> float:
        """Clean price strings by removing $ and converting to float"""
        try:
            if isinstance(price_str, str):
                return float(price_str.replace('$', '').replace(',', '').strip() or 0)
            return float(price_str) if price_str else 0.0
        except (ValueError, TypeError):
            return 0.0

    def explore_dataset(self, output_formats=['json']) -> Dict[str, Any]:
        """Main method to explore and analyze the dataset"""
        try:
            analysis = {
                "basic_stats": self._analyze_basic_stats(),
                "data_quality": self._analyze_data_quality(),
                "temporal_patterns": self._analyze_temporal_patterns(),
                "financial_metrics": self._analyze_financial_metrics(),
                "categorical_distribution": self._analyze_categorical_fields()
            }

            # Handle outputs in requested formats
            output_handler = OutputHandler(self.output_dir)
            output_handler.save_outputs(analysis, formats=output_formats)
            
            logger.info("Dataset exploration complete!")
            return analysis

        except Exception as e:
            logger.error(f"Error in dataset exploration: {e}")
            return {}

    def _analyze_basic_stats(self) -> dict:
        """Analyze basic statistics of the dataset"""
        try:
            return {
                "total_records": self.collection.count_documents({}),
                "unique_departments": len(self.collection.distinct("department_name")),
                "unique_suppliers": len(self.collection.distinct("supplier_name")),
                "unique_items": len(self.collection.distinct("item_name")),
                "unique_acquisition_types": len(self.collection.distinct("acquisition_type"))
            }
        except Exception as e:
            logger.error(f"Error in basic stats analysis: {e}")
            return {}

    def _analyze_data_quality(self) -> dict:
        """Analyze data quality and completeness"""
        try:
            key_fields = [
                "creation_date", "purchase_date", "department_name",
                "supplier_name", "item_name", "total_price", "unit_price"
            ]
            
            null_counts = {field: self.collection.count_documents({field: None}) for field in key_fields}
            
            # Check price calculation consistency
            price_mismatches = self.collection.count_documents({
                "$expr": {
                    "$and": [
                        {"$ne": [{"$multiply": ["$unit_price", "$quantity"]}, "$total_price"]},
                        {"$ne": ["$unit_price", None]},
                        {"$ne": ["$quantity", None]},
                        {"$ne": ["$total_price", None]}
                    ]
                }
            })

            return {
                "null_counts": null_counts,
                "price_calculation_mismatches": price_mismatches
            }
        except Exception as e:
            logger.error(f"Error in data quality analysis: {e}")
            return {}

    def _analyze_temporal_patterns(self) -> dict:
        """Analyze temporal aspects of the dataset"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "min_creation_date": {"$min": "$creation_date"},
                        "max_creation_date": {"$max": "$creation_date"},
                        "min_purchase_date": {"$min": "$purchase_date"},
                        "max_purchase_date": {"$max": "$purchase_date"},
                        "fiscal_years": {"$addToSet": "$fiscal_year"}
                    }
                }
            ]
            
            result = list(self.collection.aggregate(pipeline))[0]
            
            return {
                "date_ranges": {
                    "creation_date": {
                        "start": result["min_creation_date"],
                        "end": result["max_creation_date"]
                    },
                    "purchase_date": {
                        "start": result["min_purchase_date"],
                        "end": result["max_purchase_date"]
                    }
                },
                "fiscal_years": sorted(result["fiscal_years"])
            }
        except Exception as e:
            logger.error(f"Error in temporal analysis: {e}")
            return {}

    def _analyze_financial_metrics(self) -> dict:
        """Analyze financial aspects of the dataset"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "total_spend": {"$sum": "$total_price"},
                        "avg_unit_price": {"$avg": "$unit_price"},
                        "min_unit_price": {"$min": "$unit_price"},
                        "max_unit_price": {"$max": "$unit_price"},
                        "total_transactions": {"$sum": 1}
                    }
                }
            ]
            
            result = list(self.collection.aggregate(pipeline))[0]
            
            return {
                "total_spend": result["total_spend"],
                "average_unit_price": result["avg_unit_price"],
                "price_range": {
                    "min": result["min_unit_price"],
                    "max": result["max_unit_price"]
                },
                "total_transactions": result["total_transactions"]
            }
        except Exception as e:
            logger.error(f"Error in financial analysis: {e}")
            return {}

    def _analyze_categorical_fields(self) -> dict:
        """Analyze distribution of categorical fields"""
        try:
            categorical_fields = [
                "department_name",
                "supplier_name",
                "acquisition_type",
                "item_name"
            ]
            
            results = {}
            for field in categorical_fields:
                pipeline = [
                    {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 10}
                ]
                
                top_values = list(self.collection.aggregate(pipeline))
                results[field] = [
                    {"value": item["_id"], "count": item["count"]}
                    for item in top_values
                    if item["_id"] is not None
                ]
            
            return results
        except Exception as e:
            logger.error(f"Error in categorical analysis: {e}")
            return {}

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Explore procurement dataset in MongoDB Atlas')
    #Remove default
    parser.add_argument('--mongodb-uri', type=str,  help='MongoDB Atlas connection URI')
    parser.add_argument('--output-format', type=str, default='json',
                      choices=['json', 'txt', 'pdf', 'all'],
                      help='Output format(s) for analysis results')
    
    args = parser.parse_args()
    
    # Convert 'all' to list of all formats
    formats = ['json', 'txt', 'pdf'] if args.output_format == 'all' else [args.output_format]
    
    explorer = ProcurementExplorer(args.mongodb_uri)
    explorer.explore_dataset(output_formats=formats)