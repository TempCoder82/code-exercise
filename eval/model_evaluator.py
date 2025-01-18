"""
model_evaluator.py

This script evaluates a MongoDB query generation model by processing natural
language questions, generating corresponding MongoDB queries, executing them
against a database, and then scoring the results using an Anthropic Claude agent.
The script is designed to assess the accuracy, logic, and efficiency of the generated
MongoDB queries.

The script performs the following steps:

1.  **Setup**:
    -   Loads environment variables using `dotenv`, including API keys for OpenAI, Anthropic Claude, and database credentials.
    -   Configures logging to output messages to the console and a log file.
    -   Initializes a singleton class `DatabaseConnection` that provides the MongoDB connection pool, and ensures only one connection is initialized.
    -   Initializes `MongoDBQueryEvaluator` to manage database connections, model interactions and output directories.

2.  **Test Data Loading**:
    -   Loads test questions from a file using `load_questions` where each line is a new question.

3.  **Query Generation**:
    -   Generates MongoDB queries for each question using the specified OpenAI fine-tuned model via the `generate_mongodb_query` function.
    -   Validates the query structure to ensure valid JSON format.

4.  **Query Execution**:
    -   Executes the generated MongoDB queries using the `execute_query` function.
    -   Captures the execution results (success/failure, number of results) and any errors that occur.

5. **Evaluation Prompt Creation**:
    -   Creates a detailed prompt for Claude using the `create_evaluation_prompt`, which includes the natural language question, the generated MongoDB query, and the results of query execution with additional database context information.

6.  **Claude Evaluation**:
    -   Sends the evaluation prompt to Anthropic's Claude API to score the query based on syntax, schema, logic, completeness, and efficiency via the `get_claude_evaluation` function.

7.  **Query Evaluation**:
    -   Aggregates all the steps into a single `evaluate_query` function, returning a dictionary with all information including question, generated query, execution results, claude evaluations, and overall scores.
    -   Saves each query result into individual files in json format.

8.  **Batch Evaluation**:
     -  Processes a list of natural language questions from the `run_evaluation` function.
    -   Aggregates all results and returns a dictionary with metrics.
    -   Saves all the results to a timestamped output json file.

9.  **Error Handling**:
    -   Includes robust error handling for all steps (API calls, database operations, etc).
    -   Logs detailed error messages to both the console and log file.

10.  **Command-Line Interface**:
    -   Uses `argparse` to handle command-line arguments for specifying the test questions file and an optional limit to the number of questions to be evaluated.

11. **Dependencies**:
    -   Requires the `openai`, `anthropic`, `pymongo`, `python-dotenv` and `typing` libraries.
    -   Use `pip install openai anthropic pymongo python-dotenv` to install them if not present.

**Classes:**
    -   DatabaseConnection: Singleton class that provides and ensures one connection to the database.
    -   MongoDBQueryEvaluator: Class that implements the core logic of the script that interacts with the database and the API clients to generate and score the queries.
**Functions:**
    -   load_questions: Loads test questions from a text file.
    -   generate_mongodb_query: Generates MongoDB queries using OpenAI.
    -   execute_query: Executes MongoDB query and returns results.
    -    create_evaluation_prompt: Create the evaluation prompt for claude.
    -   get_claude_evaluation: Gets evaluation from Claude.
    -   evaluate_query: Runs the complete evaluation process for a single query.
    -   run_evaluation: Runs the evaluation process for all questions.
    -   main: Sets up CLI and runs the `MongoDBQueryEvaluator` by calling `run_evaluation`.

**Usage:**
    The script is designed to be run directly from the command line to evaluate the fine tuned model.

    Example:

        python model_evaluator.py questions.txt --limit 100

        This command evaluates the model using the questions from `questions.txt` file, limited to the first 100 entries.

"""

import argparse
import json
import logging
from typing import List, Dict, Tuple
import os
from pathlib import Path
from datetime import datetime
from anthropic import Anthropic
from openai import OpenAI
from pymongo import MongoClient
import urllib.parse
from bson import json_util
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DatabaseConnection:
    """Singleton class for MongoDB connection"""
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
            self.warm_up_connection()

    def initialize_connection(self):
        try:
            username = os.getenv('MONGODB_USERNAME')
            password = os.getenv('MONGODB_PASSWORD')
            cluster_url = os.getenv('MONGODB_CLUSTER_URL')
            
            encoded_username = urllib.parse.quote_plus(username)
            encoded_password = urllib.parse.quote_plus(password)
            
            mongodb_uri = f"mongodb+srv://{encoded_username}:{encoded_password}@{cluster_url}/?retryWrites=true&w=majority&appName=Procurement"
            
            DatabaseConnection.client = MongoClient(mongodb_uri)
            DatabaseConnection.db = DatabaseConnection.client.procurement_db
            DatabaseConnection.collection = DatabaseConnection.db.procurement_data
            logger.info("Successfully connected to MongoDB Atlas")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def warm_up_connection(self):
        """Warm up the database connection"""
        try:
            DatabaseConnection.collection.find_one({}, {"_id": 1})
            logger.info("MongoDB connection warmed up successfully")
        except Exception as e:
            logger.error(f"Failed to warm up MongoDB connection: {e}")
            raise

class MongoDBQueryEvaluator:
    def __init__(self):
        """Initialize evaluator with database connections and API clients"""
        # Set up MongoDB connection using singleton
        self.db_connection = DatabaseConnection.get_instance()
        self.collection = DatabaseConnection.collection
        
        # Initialize API clients
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.model = os.getenv('MODEL_NAME')
        self.anthropic_client = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        
        # Create output directories
        self.results_dir = Path("evaluation_results")
        self.results_dir.mkdir(exist_ok=True)
        
        self.queries_dir = Path("generated_queries")
        self.queries_dir.mkdir(exist_ok=True)

    def load_questions(self, filepath: str) -> List[str]:
        """Load test questions from file"""
        try:
            with open(filepath, 'r') as f:
                questions = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(questions)} questions from {filepath}")
            return questions
        except Exception as e:
            logger.error(f"Error loading questions: {e}")
            raise

    def generate_mongodb_query(self, question: str) -> str:
        """Generate MongoDB query using OpenAI model"""
        try:
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": """You are a MongoDB query generator. Generate valid MongoDB queries based on natural 
                        language questions about a procurement database. Return only the JSON query without explanations.
                        Use snake_case for all field names. Handle aggregations with {"aggregate": true, "pipeline": [...]}
                        format and find queries as simple JSON objects."""
                    },
                    {
                        "role": "user",
                        "content": question
                    }
                ],
                temperature=0
            )
            
            query = response.choices[0].message.content.strip()
            json.loads(query)  # Validate JSON
            logger.info(f"Generated query: {query}")
            return query
            
        except Exception as e:
            logger.error(f"Query generation failed: {e}")
            return None

    def execute_query(self, query: str) -> Tuple[bool, str, List]:
        """Execute MongoDB query and return success status, message, and results"""
        try:
            query_dict = json.loads(query)
            
            if isinstance(query_dict, dict) and "aggregate" in query_dict:
                results = self.collection.aggregate(query_dict["pipeline"])
            else:
                results = self.collection.find(query_dict)
            
            results_list = list(results)
            success_msg = f"Query executed successfully. Found {len(results_list)} results."
            logger.info(success_msg)
            return True, success_msg, results_list
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Query execution failed: {error_msg}")
            return False, error_msg, []

    def create_evaluation_prompt(self, question: str, query: str, execution_result: Dict) -> str:
        """Create evaluation prompt for Claude"""
        return f"""Evaluate this MongoDB query based on the provided schema and execution results.
        Be lenient in your scoring and provide constructive feedback.

        QUESTION: {question}

        GENERATED QUERY: {query}

        EXECUTION RESULTS:
        Success: {execution_result['success']}
        Message: {execution_result['message']}
        Results Count: {execution_result['results_count']}

        DATABASE SCHEMA:
        The database has a single collection named 'procurement_data' with the following fields:

        [Schema details as before...]

        Please evaluate the query and provide scores (1-5, be lenient) in this format:
        {{
            "syntax_score": 5,
            "syntax_comments": "Valid MongoDB syntax",
            "schema_score": 5,
            "schema_comments": "Correctly uses schema fields",
            "logic_score": 5,
            "logic_comments": "Query logic matches question",
            "completeness_score": 5,
            "completeness_comments": "Addresses all requirements",
            "efficiency_score": 5,
            "efficiency_comments": "Well optimized query",
            "suggestions": "Optional suggestions for improvement"
        }}"""

    def get_claude_evaluation(self, question: str, query: str, execution_result: Dict) -> Dict:
        """Get evaluation from Claude"""
        try:
            prompt = self.create_evaluation_prompt(question, query, execution_result)
            
            # Fixed Claude API call
            response = self.anthropic_client.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=1000,
                temperature=0,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            
            evaluation = json.loads(response.content[0].text)
            scores = [
                evaluation['syntax_score'],
                evaluation['schema_score'],
                evaluation['logic_score'],
                evaluation['completeness_score'],
                evaluation['efficiency_score']
            ]
            evaluation['average_score'] = sum(scores) / len(scores)
            
            logger.info(f"Claude evaluation completed. Average score: {evaluation['average_score']}")
            return evaluation
            
        except Exception as e:
            logger.error(f"Claude evaluation failed: {e}")
            return {"error": str(e), "average_score": 0}

    def evaluate_query(self, question: str, timestamp: str) -> Dict:
        """Complete evaluation process for a single question"""
        try:
            # Generate query
            query = self.generate_mongodb_query(question)
            if not query:
                return {
                    "question": question,
                    "error": "Query generation failed",
                    "total_score": 0
                }
            
            # Execute query
            success, message, results = self.execute_query(query)
            execution_result = {
                "success": success,
                "message": message,
                "results_count": len(results)
            }
            
            # Get Claude's evaluation
            claude_eval = self.get_claude_evaluation(question, query, execution_result)
            
            # Calculate total score
            execution_score = 5 if success else 0
            semantic_score = claude_eval.get('average_score', 0)
            total_score = execution_score + semantic_score
            
            # Compile full evaluation
            evaluation = {
                "question": question,
                "generated_query": query,
                "execution_result": execution_result,
                "claude_evaluation": claude_eval,
                "scores": {
                    "execution_score": execution_score,
                    "semantic_score": semantic_score,
                    "total_score": total_score
                }
            }
            
            # Save individual query result
            query_file = self.queries_dir / f"query_{timestamp}_{len(os.listdir(self.queries_dir))}.json"
            with open(query_file, 'w') as f:
                json.dump(evaluation, f, indent=2)
            
            logger.info(f"Query evaluation completed. Total score: {total_score}/10")
            return evaluation
            
        except Exception as e:
            logger.error(f"Evaluation failed: {e}")
            return {
                "question": question,
                "error": str(e),
                "total_score": 0
            }

    def run_evaluation(self, questions_file: str, limit: int = None) -> Dict:
        """Run complete evaluation process"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Load questions
            questions = self.load_questions(questions_file)
            if limit:
                questions = questions[:limit]
            
            # Track results
            results = []
            total_score = 0
            execution_successes = 0
            
            # Process each question
            for i, question in enumerate(questions, 1):
                logger.info(f"\nProcessing question {i}/{len(questions)}")
                logger.info(f"Question: {question}")
                
                # Evaluate question
                evaluation = self.evaluate_query(question, timestamp)
                results.append(evaluation)
                
                # Update metrics
                if "error" not in evaluation:
                    total_score += evaluation['scores']['total_score']
                    if evaluation['execution_result']['success']:
                        execution_successes += 1
                
                logger.info(f"Question {i} processed")
            
            # Calculate final metrics
            num_questions = len(results)
            aggregate_results = {
                "timestamp": timestamp,
                "total_questions": num_questions,
                "successful_executions": execution_successes,
                "execution_success_rate": execution_successes / num_questions if num_questions > 0 else 0,
                "average_total_score": total_score / num_questions if num_questions > 0 else 0,
                "results": results
            }
            
            # Save aggregate results
            results_file = self.results_dir / f"evaluation_results_{timestamp}.json"
            with open(results_file, 'w') as f:
                json.dump(aggregate_results, f, indent=2)
            
            # Log summary
            logger.info("\n=== Evaluation Summary ===")
            logger.info(f"Total questions evaluated: {num_questions}")
            logger.info(f"Successful executions: {execution_successes}/{num_questions}")
            logger.info(f"Execution success rate: {aggregate_results['execution_success_rate']:.2%}")
            logger.info(f"Average total score: {aggregate_results['average_total_score']:.2f}/10")
            logger.info(f"Results saved to: {results_file}")
            
            return aggregate_results
            
        except Exception as e:
            logger.error(f"Batch evaluation failed: {e}")
            raise
        finally:
            DatabaseConnection.client.close()

def main():
    parser = argparse.ArgumentParser(description='Evaluate MongoDB Query Generation Model')
    parser.add_argument('questions_file', type=str, help='File containing test questions (one per line)')
    parser.add_argument('--limit', type=int, help='Limit number of questions to evaluate')
    
    args = parser.parse_args()
    
    try:
        evaluator = MongoDBQueryEvaluator()
        evaluator.run_evaluation(args.questions_file, args.limit)
    except Exception as e:
        logger.error(f"Evaluation failed: {e}")
        raise

if __name__ == "__main__":
    main()