"""
query_demo.py

This script creates an interactive Gradio demo for querying a MongoDB database
using natural language. It allows users to input questions and receive
corresponding MongoDB queries and their execution results. The script uses
OpenAI's API to generate the MongoDB queries and the pymongo library to
execute those queries and retrieve results.

The script performs the following steps:

1.  **Environment Setup**:
    -   Loads environment variables using `dotenv`, primarily for the OpenAI API key, MongoDB credentials, and the fine tuned model name.
    -   Configures logging to output messages to the console and a log file.

2.  **Database Connection Management**:
    -   Implements a singleton class `DatabaseConnection` to handle MongoDB connections.
        -   Ensures only one connection is initialized to the database, and warms up the database on initialization.
    -   Manages the database connection pool.

3.  **Query Execution**:
    -   Provides a `QueryRunner` class for executing MongoDB queries and aggregations:
        -   It handles both find and aggregate queries.
        -   Returns results as a formatted JSON string.
    -   The `QueryRunner` is a singleton class, that ensures only one instance of the class is created.

4.  **Query Generation and Execution**:
    -   Implements a `QueryAssistant` class for generating queries using OpenAI's API and executing them via the `QueryRunner` class.
    -   It returns the query and the results to display.

5. **Initialization of Global Components**:
    -   Initializes all the global components that are needed to run the application: `DatabaseConnection` and `QueryAssistant`.
    -   It calls the warm-up function of the database to make sure that the connection is ready.

6.  **Gradio Demo Creation**:
    -   Sets up a user interface using Gradio (`create_demo` function):
        -   Text input box for questions
        -   Button for generating and running the query
        -   Code boxes for displaying generated queries and results
        -  Examples box to provide example questions that a user can start with.

7. **User Query Processing**:
     -  Processes and runs the query via the `process_query` function
     -  Returns query and results to be displayed in the ui.
8.  **Error Handling**:
    -   Includes error handling for various operations (API calls, database interactions, JSON parsing, etc.)
    -   Logs detailed error messages to the console and a log file.

9.  **Dependencies**:
    -   Requires the `gradio`, `openai`, `pymongo`, `python-dotenv` and `typing` libraries.
    -  Use `pip install gradio openai pymongo python-dotenv` to install these packages.

**Classes:**
    -   DatabaseConnection: Singleton class that provides and ensures one connection to the database.
    -   QueryRunner: Singleton class that ensures one instance is created to execute mongo queries.
    -   QueryAssistant: Class that uses the openai api to generate queries using the fine tuned model, and also the QueryRunner class to execute them.
**Functions:**
    -   initialize_application: Initialises the global resources for the app.
    -   process_query: takes a question input and calls the `QueryAssistant` to generate the query and execute it.
    -   create_demo: creates the gradio demo ui.
    -   main: Initializes all components and runs the gradio demo app.
**Usage:**
    The script is designed to be run directly to launch an interactive demo.

        python query_demo.py

        This will launch the gradio app with a share link.

"""

import gradio as gr
import json
from openai import OpenAI
from pymongo import MongoClient
import logging
from dotenv import load_dotenv
import os
import urllib.parse
from bson import json_util

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    _instance = None
    client = None
    db = None
    collection = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        if DatabaseConnection.client is None:
            self.initialize_connection()

    def initialize_connection(self):
        """Initialize and verify MongoDB connection"""
        try:
            # MongoDB Atlas connection parameters
            username = os.getenv('MONGODB_USERNAME')
            password = os.getenv('MONGODB_PASSWORD')
            cluster_url = os.getenv('MONGODB_CLUSTER_URL')

            if not all([username, password, cluster_url]):
                raise ValueError("Missing MongoDB connection parameters")

            # Encode credentials for URL
            encoded_username = urllib.parse.quote_plus(username)
            encoded_password = urllib.parse.quote_plus(password)

            # Construct the MongoDB Atlas connection string
            mongodb_uri = f"mongodb+srv://{encoded_username}:{encoded_password}@{cluster_url}/?retryWrites=true&w=majority&appName=Procurement"

            # Initialize connection
            DatabaseConnection.client = MongoClient(mongodb_uri)
            DatabaseConnection.db = DatabaseConnection.client.procurement_db
            DatabaseConnection.collection = DatabaseConnection.db.procurement_data

            logger.info("Successfully connected to MongoDB Atlas")

        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connection: {e}")
            raise

    def warm_up_connection(self):
        """Warm up the database connection with a light query"""
        try:
            # Execute a lightweight query to warm up the connection
            DatabaseConnection.collection.find_one({}, {"_id": 1})
            logger.info("MongoDB connection warmed up successfully")
        except Exception as e:
            logger.error(f"Failed to warm up MongoDB connection: {e}")
            raise

class QueryRunner:
    """Handles execution of MongoDB queries and aggregations."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QueryRunner, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.db_connection = DatabaseConnection.get_instance()
            self.collection = DatabaseConnection.collection
            self.initialized = True

    def run_query(self, query_str: str):
        """Runs a MongoDB query and returns JSON results."""
        try:
            query = json.loads(query_str)
            results = self.collection.find(query)
            return json.dumps(list(results), default=json_util.default, indent=2)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON query: {e}")
            raise
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def run_aggregation(self, pipeline_str: str):
        """Runs a MongoDB aggregation pipeline and returns JSON results."""
        try:
            pipeline = json.loads(pipeline_str)
            results = self.collection.aggregate(pipeline)
            return json.dumps(list(results), default=json_util.default, indent=2)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON pipeline: {e}")
            raise
        except Exception as e:
            logger.error(f"Error executing aggregation: {e}")
            raise

class QueryAssistant:
    """Generates MongoDB queries and executes them."""
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, model_name=None, api_key=None):
        if not hasattr(self, 'initialized'):
            self.client = OpenAI(api_key=api_key or os.getenv('OPENAI_API_KEY'))
            self.model = os.getenv('MODEL_NAME')
            self.query_runner = QueryRunner()
            self.initialized = True

    def generate_and_run_query(self, question: str, progress=gr.Progress()):
        """Generates query and executes it."""
        try:
            progress(0.2, desc="Generating query...")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an assistant that converts natural language questions into MongoDB queries. Return only the JSON query without any explanations or formatting."
                    },
                    {
                        "role": "user",
                        "content": question
                    }
                ],
                temperature=0
            )
            query_str = response.choices[0].message.content.strip()
            logger.info(f"Generated query: {query_str}")
            query = json.loads(query_str)
            
            progress(0.5, desc="Executing Query...")
            if "aggregate" in query and query["aggregate"]:
                results = self.query_runner.run_aggregation(json.dumps(query["pipeline"]))
            else:
                results = self.query_runner.run_query(query_str)
            
            results_json = json.loads(results)
            pretty_results = json.dumps(results_json, indent=2)
            
            progress(1.0, desc="Query Complete")
            return query_str, pretty_results
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse generated query: {e}")
            return str(e), "Query parsing failed"
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return str(e), "Query execution failed"

# Initialize global instances
db_connection = None
query_assistant = None

def initialize_application():
    """Initialize all global components"""
    global db_connection, query_assistant
    
    logger.info("Initializing application components...")
    
    try:
        # Initialize and warm up database connection
        db_connection = DatabaseConnection.get_instance()
        db_connection.warm_up_connection()
        
        # Initialize query assistant
        query_assistant = QueryAssistant()
        
        logger.info("Application initialization completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Application initialization failed: {e}")
        return False

def process_query(question: str, progress=gr.Progress()):
    """Process user question and return query and results."""
    return query_assistant.generate_and_run_query(question, progress)

def create_demo():
    """Creates and returns Gradio demo interface."""
    with gr.Blocks(title="MongoDB Query Assistant") as demo:
        gr.Markdown("""
        # MongoDB Query Assistant
        Convert natural language questions into MongoDB queries and get results from the database.
        
        ## Example questions:
        - What was the total spending in fiscal year 2013-2014?
        - Which suppliers had the highest number of transactions?
        - How many orders were created each month?
        """)
        
        with gr.Row():
            with gr.Column():
                question_input = gr.Textbox(
                    label="Your Question",
                    placeholder="Enter your question about the procurement data...",
                    lines=3
                )
                submit_btn = gr.Button("Generate & Run Query")
        
        with gr.Row():
            with gr.Column():
                query_output = gr.Code(
                    label="Generated MongoDB Query",
                    language="json"
                )
            with gr.Column():
                results_output = gr.Code(
                    label="Query Results",
                    language="json"
                )
        
        submit_btn.click(
            fn=process_query,
            inputs=[question_input],
            outputs=[query_output, results_output],
        )
        
        gr.Examples(
            examples=[
                ["What is the total spending for the supplier Compu-tecture, Inc in the fiscal year 2013-2014?"],
                ["Which department created the most purchase orders?"],
                ["Which supplier has provided the greatest number of unique classification codes?"],
                ["Which supplier had the highest spending for fiscal year 2012-2013"]
            ],
            inputs=question_input
        )
    
    return demo

if __name__ == "__main__":
    logger.info("Starting MongoDB Query Assistant...")
    
    # Initialize application components
    if not initialize_application():
        logger.error("Failed to initialize application. Exiting.")
        raise SystemExit("Application initialization failed")
    
    # Create and launch demo
    demo = create_demo()
    demo.launch(share=True)
