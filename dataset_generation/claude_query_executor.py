"""
claude_query_executor.py

This script processes natural language questions from an input file, converts them
into MongoDB queries using Anthropic's Claude model, executes these queries against
a MongoDB database, and logs the results. The script performs the following steps:

1.  **Initialization**:
    -   Loads environment variables, including the Anthropic API key.
    -   Initializes the `IntegratedQueryGenerator` with the API key and sets up system prompts,
        example prompts and connects to a MongoDB Atlas database.
    -   Sets up logging for info level messages.
    -   Sets up success and error counters.

2.  **Query Generation**:
    -   Reads natural language questions from a specified input text file, one question per line.
    -   Utilizes the `IntegratedQueryGenerator` and Claude to convert each question into a MongoDB query in JSON format.
    -   Includes logic to correct and validate query structure as required.
    -   Normalizes field names to snake_case, as per database conventions.

3.  **Query Execution**:
    -   Executes the generated MongoDB queries against the connected database using the pymongo library.
    -   Handles both `find` and `aggregate` queries correctly.
    -   Tracks query successes and failures.
    -   Captures exceptions during query execution and logs relevant error information.

4. **Output Handling**:
    -   If an output file is specified, it prints the question and the generated query to that file.
    -   If no output file is specified, it prints to the console.
    -   Creates a timestamped json file to store successfully translated queries
    -   Creates a time stamped json file to store failed queries with full error tracking information.

5.  **Error Handling**:
    -   Includes detailed error tracking and logging.
    -   Handles missing API keys, invalid query structures, exceptions during API calls, and connection errors to the database.
    -   Logs each query attempt with success or failure and any error messages.
    -   Logs are created with timestamp on the file name.
    -   Includes a comprehensive summary at the end of the script on the number of questions, successful, and failed queries.

6.  **Validation and Correction**:
    -   Validates the structure of the generated queries to ensure they are valid MongoDB queries.
    -   Implements functions to:
        -   Correct the structure of the generated query (wraps simple dictionaries/lists inside aggregate as needed).
        -   Normalize field names to snake_case from either camel case or other naming conventions if they exist in the `field_mappings`.
    -   Ensures that queries meet a defined valid format, and raises a value error if the format is not met after all correction mechanisms.

7.  **Command-Line Interface**:
    -   Uses `argparse` to handle command-line arguments, allowing the user to specify:
        -   The path to the input text file containing the questions.
        -   An optional output file to log results to.
        -   Optionally pass an API Key for Anthropic (if it's not set on the environment)

8.  **Dependencies**:
    -   Requires the `anthropic`, `pymongo`, `python-dotenv`, `bson` and `typing` libraries.
    -   Ensure these are installed before running the script using `pip install anthropic pymongo python-dotenv bson`

**Classes**:
    -   IntegratedQueryGenerator: This class manages the connection with both the API client and the MongoDB client. It generates the queries and validates them.
    
**Usage:**
    To generate and execute queries:

        python claude_query_executor.py <input_file> [--output <output_file>] [--api-key <api_key>]

    Example:
        Process questions in 'questions.txt', log results to 'output.txt', using an API key:
            python claude_query_executor.py questions.txt --output output.txt --api-key your_anthropic_api_key

        Process questions in 'my_questions.txt', and output to console with API key defined on environment:
            python claude_query_executor.py my_questions.txt
"""

import argparse
import json
import sys
import os
import logging
import urllib.parse
from anthropic import Anthropic
from pymongo import MongoClient
from bson import json_util
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IntegratedQueryGenerator:
    def __init__(self, api_key=None):
        # Initialize Anthropic client
        if not api_key:
            api_key = os.getenv('ANTHROPIC_API_KEY')
            if not api_key:
                raise ValueError("No API key provided. Set ANTHROPIC_API_KEY environment variable or pass key directly.")
        
        self.client = Anthropic(api_key=api_key)
        self.system_prompt = self._create_system_prompt()
        self.examples = self._create_example_prompt()
        
        # Initialize MongoDB connection
        username = os.getenv('MONGODB_USERNAME')
        password = os.getenv('MONGODB_PASSWORD')  # In production, use environment variables
        cluster_url = os.getenv('MONGODB_CLUSTER_URL')
        
        encoded_username = urllib.parse.quote_plus(username)
        encoded_password = urllib.parse.quote_plus(password)
        
        mongodb_uri = f"mongodb+srv://{encoded_username}:{encoded_password}@{cluster_url}/?retryWrites=true&w=majority&appName=Procurement"
        
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client.procurement_db
        self.collection = self.db.procurement_data
        logger.info("Connected to MongoDB Atlas")

        # Initialize error tracking
        self.error_log = []
        self.success_count = 0
        self.error_count = 0

    def _create_system_prompt(self):
        # Your existing system prompt
        return """You are a MongoDB query generator. You create valid MongoDB queries based on natural language questions about a procurement database.

        DATABASE DETAILS:
        The database has a single collection named 'procurement_data' with the following fields:

        Date Fields:
        - creation_date (datetime)
        - purchase_date (datetime)
        - fiscal_year

        Reference Numbers:
        - lpa_number
        - purchase_order_number
        - requisition_number

        Acquisition Info:
        - acquisition_type
        - sub_acquisition_type
        - acquisition_method
        - sub_acquisition_method

        Organization Info:
        - department_name
        - location

        Supplier Info:
        - supplier_code (integer)
        - supplier_name
        - supplier_qualifications
        - supplier_zip_code
        - calcard

        Item Details:
        - item_name
        - item_description
        - quantity (float)
        - unit_price (float)
        - total_price (float)

        Classification:
        - classification_codes (array of strings)
        - normalized_unspsc
        - commodity_title
        - class
        - class_title
        - family
        - family_title
        - segment
        - segment_title

        QUERY RUNNER REQUIREMENTS:
        1. Queries must be valid JSON
        2. For aggregation pipelines, use format:
        {
            "aggregate": true,
            "pipeline": [
            // pipeline stages here
            ]
        }
        3. For find queries, use format:
        {
            // find query here
        }
        4. Use double quotes for all strings
        5. No trailing commas
        6. No single quotes

        YOUR TASK:
        1. Analyze the natural language question
        2. Create a MongoDB query that answers the question
        3. Return only the JSON query, properly formatted
        4. Do not include any explanations or text outside the JSON
        5. Ensure all field names match the snake_case format from the database"""

    def _create_example_prompt(self):
        return """Here are some examples of questions and their corresponding queries:

        Question: "What departments spent more than $10,000 on IT supplies in 2023?"
        {
        "aggregate": true,
        "pipeline": [
            {
            "$match": {
                "fiscal_year": "2023",
                "item_description": {"$regex": "IT", "$options": "i"},
                "total_price": {"$gt": 10000}
            }
            },
            {
            "$group": {
                "_id": "$department_name",
                "total_spent": {"$sum": "$total_price"}
            }
            }
        ]
        }

        Question: "Who are our top 5 suppliers by total purchase amount?"
        {
        "aggregate": true,
        "pipeline": [
            {
            "$group": {
                "_id": "$supplier_name",
                "total_purchases": {"$sum": "$total_price"}
            }
            },
            {
            "$sort": {"total_purchases": -1}
            },
            {
            "$limit": 5
            }
        ]
        }"""

    def run_query(self, query):
        """Execute the MongoDB query and return results"""
        try:
            if isinstance(query, dict) and "aggregate" in query:
                if query["aggregate"]:
                    results = self.collection.aggregate(query["pipeline"])
                else:
                    results = self.collection.find(query)
            else:
                results = self.collection.find(query)
            
            # Convert results to list and validate we can get data
            results_list = list(results)
            return True, results_list
            
        except Exception as e:
            return False, str(e)

    def process_questions_file(self, input_file, output_file=None):
        """Process questions from file with integrated query execution and error tracking"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        successful_queries_file = f'successful_queries_{timestamp}.json'
        error_log_file = f'error_log_{timestamp}.json'
        
        successful_queries = []
        
        try:
            # Setup output handling
            if output_file:
                out_f = open(output_file, 'w')
            else:
                out_f = sys.stdout

            # Process each question
            with open(input_file, 'r') as f:
                for line_number, question in enumerate(f, 1):
                    question = question.strip()
                    if not question:  # Skip empty lines
                        continue
                        
                    print(f"\nProcessing Question {line_number}: {question}", file=out_f)
                    try:
                        # Generate query
                        query = self.generate_query(question)
                        print(f"\nGenerated Query:", file=out_f)
                        print(json.dumps(query, indent=2), file=out_f)
                        
                        # Try to execute the query
                        success, results = self.run_query(query)
                        
                        if success:
                            self.success_count += 1
                            print(f"Query executed successfully", file=out_f)
                            successful_queries.append({
                                'question': question,
                                'query': query,
                                'result_count': len(results)
                            })
                        else:
                            self.error_count += 1
                            error_msg = f"Query execution failed: {results}"
                            print(error_msg, file=out_f)
                            self.error_log.append({
                                'line_number': line_number,
                                'question': question,
                                'query': query,
                                'error': error_msg
                            })
                            
                    except Exception as e:
                        self.error_count += 1
                        error_msg = f"Error processing question: {str(e)}"
                        print(error_msg, file=sys.stderr)
                        self.error_log.append({
                            'line_number': line_number,
                            'question': question,
                            'error': error_msg
                        })

            if output_file:
                out_f.close()

            # Save successful queries
            with open(successful_queries_file, 'w') as f:
                json.dump(successful_queries, f, indent=2)
            
            # Save error log
            with open(error_log_file, 'w') as f:
                json.dump(self.error_log, f, indent=2)
            
            # Print summary
            print(f"\nProcessing Summary:")
            print(f"Total questions processed: {line_number}")
            print(f"Successful queries: {self.success_count}")
            print(f"Failed queries: {self.error_count}")
            print(f"Success rate: {(self.success_count/line_number)*100:.2f}%")
            print(f"\nSuccessful queries saved to: {successful_queries_file}")
            print(f"Error log saved to: {error_log_file}")

        except FileNotFoundError:
            print(f"Error: File '{input_file}' not found.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error processing file: {str(e)}", file=sys.stderr)
            sys.exit(1)
        finally:
            self.mongo_client.close()

    def validate_and_fix_query(self, query):
        """Validate and fix query structure if needed"""
        # If it's just a pipeline array, wrap it properly
        if isinstance(query, list):
            return {
                "aggregate": True,
                "pipeline": query
            }
        
        # If it's a dict but missing the wrapper
        if isinstance(query, dict):
            if "$match" in query or "$group" in query or "$sort" in query:
                return {
                    "aggregate": True,
                    "pipeline": [query]
                }
            # If it's already in correct format
            if "aggregate" in query and "pipeline" in query:
                return query
            # If it's a simple find query
            return query

        raise ValueError("Query format not recognized")

    def normalize_field_names(self, query):
        """Ensure field names are in snake_case"""
        field_mappings = {
            # Date Fields
            "creationDate": "creation_date",
            "purchaseDate": "purchase_date",
            "fiscalYear": "fiscal_year",
            
            # Reference Numbers
            "lpaNumber": "lpa_number",
            "purchaseOrderNumber": "purchase_order_number",
            "requisitionNumber": "requisition_number",
            
            # Acquisition Info
            "acquisitionType": "acquisition_type",
            "subAcquisitionType": "sub_acquisition_type",
            "acquisitionMethod": "acquisition_method",
            "subAcquisitionMethod": "sub_acquisition_method",
            
            # Organization Info
            "departmentName": "department_name",
            
            # Supplier Info
            "supplierCode": "supplier_code",
            "supplierName": "supplier_name",
            "supplierQualifications": "supplier_qualifications",
            "supplierZipCode": "supplier_zip_code",
            
            # Item Details
            "itemName": "item_name",
            "itemDescription": "item_description",
            "unitPrice": "unit_price",
            "totalPrice": "total_price",
            
            # Classification
            "classificationCodes": "classification_codes",
            "normalizedUnspsc": "normalized_unspsc",
            "commodityTitle": "commodity_title",
            "classTitle": "class_title",
            "familyTitle": "family_title",
            "segmentTitle": "segment_title"
        }
        
        def convert_to_snake_case(text):
            """Convert camelCase to snake_case if no mapping exists"""
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        
        def fix_fields(obj):
            """Recursively fix field names in nested structures"""
            if isinstance(obj, dict):
                new_dict = {}
                for key, value in obj.items():
                    # Don't modify MongoDB operators
                    if key.startswith('$'):
                        new_key = key
                    else:
                        # Check mapping or convert to snake_case
                        new_key = field_mappings.get(key, convert_to_snake_case(key))
                    
                    # Handle field references in values
                    if isinstance(value, str) and value.startswith('$'):
                        value = field_mappings.get(value[1:], convert_to_snake_case(value[1:]))
                        value = f"${value}"
                    
                    new_dict[new_key] = fix_fields(value)
                return new_dict
            elif isinstance(obj, list):
                return [fix_fields(item) for item in obj]
            return obj
        
        return fix_fields(query)

    def is_valid_query_structure(self, query):
        """Validate the final query structure"""
        if not isinstance(query, dict):
            return False
            
        if "aggregate" in query:
            return (
                isinstance(query["aggregate"], bool) and
                "pipeline" in query and
                isinstance(query["pipeline"], list)
            )
        
        # For find queries, should be a dict without "aggregate" key
        return True

    def generate_query(self, question):
      """Generate MongoDB query using Claude with validation"""
      try:
          message = self.client.messages.create(
              model=os.getenv('ANTHROPIC_API_KEY')#"claude-3-sonnet-20240229",
              max_tokens=4096,
              temperature=0,
              system=self.system_prompt,
              messages=[
                  {
                      "role": "user",
                      "content": f"{self.examples}\n\nGenerate a MongoDB query for this question:\n{question}\n\nReturn only the JSON query."
                  }
              ]
          )
          
          # Extract and parse the query
          response = message.content[0].text
          query = json.loads(response)
          
          # Validate and fix structure
          query = self.validate_and_fix_query(query)
          
          # Normalize field names
          query = self.normalize_field_names(query)
          
          # Final validation
          if not self.is_valid_query_structure(query):
              raise ValueError("Invalid query structure after fixes")
              
          return query
          
      except Exception as e:
          raise Exception(f"Error generating query: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Generate and execute MongoDB queries using Claude')
    parser.add_argument('input_file', type=str, help='Path to the text file containing questions')
    parser.add_argument('--output', type=str, help='Optional output file path for the generated queries')
    parser.add_argument('--api-key', type=str, help='Anthropic API key (optional if set in environment)')
    
    args = parser.parse_args()
    
    try:
        generator = IntegratedQueryGenerator(api_key=args.api_key)
        generator.process_questions_file(args.input_file, args.output)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()