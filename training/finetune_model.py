"""
finetune_model.py

This script orchestrates the fine-tuning of an OpenAI model using
a dataset of natural language questions and their corresponding
MongoDB query equivalents. It handles the uploading of training
and validation datasets, initiating the fine-tuning job, and
checking the job status.

The script performs the following steps:

1.  **Environment Setup**:
    -   Loads environment variables using `dotenv`, primarily for the OpenAI API key.
    -   Configures logging to output messages to both a file (`finetune.log`) and the console, which records the steps and potential issues during fine-tuning.

2.  **Dataset Upload**:
    -   Uploads training and validation data files to OpenAI for fine-tuning using the `upload_to_openai` function.
    -   Handles file upload errors with detailed logging messages.

3.  **Fine-Tuning Initiation**:
    -   Starts the fine-tuning job using the uploaded file IDs by calling `initiate_fine_tuning` and sets the `n_epochs` parameter to 3 for training.
    -   Logs the fine-tuning job ID and status for monitoring.
    -   Allows setting a base model, from a default, a passed parameter or a variable from the .env.

4.  **Status Monitoring**:
    -   Allows initial status checking using the job ID by calling `check_fine_tuning_status`, allowing to follow the initial progress of the job.

5.  **Command-Line Interface**:
    -   Uses `argparse` to handle command-line arguments for custom file paths and base model to be used for fine-tuning.

6.  **Error Handling**:
    -   Includes error handling for various steps, such as file upload, fine-tuning job initiation, and status retrieval.
    -   Logs all errors to both the console and the log file.

7.  **Dependencies**:
    -   Requires the `openai`, `python-dotenv`, `argparse` and `logging` libraries.
    -   Use `pip install openai python-dotenv` to install them if they are not present.
   
**Functions:**
    -   upload_to_openai: Uploads a JSONL file to OpenAI for fine tuning.
    -   initiate_fine_tuning: Starts the fine tuning process on openai.
    -   check_fine_tuning_status: Checks the status of a fine tuning process.
    -   main: main entry point for the script that orchestrates the fine tuning process.

**Usage:**
   The script is designed to be run from the command line, mainly for fine tuning the openai model.

   Example:

        python finetune_model.py --train data/train.jsonl --val data/val.jsonl --model gpt-4o-mini-2024-07-18

        This will upload the train.jsonl and val.jsonl, then it will start the fine tuning using model gpt-4o-mini-2024-07-18.

    You can customize the input file paths and the model to be used by using the parameters in the command line.
    By default it will use data/train.jsonl and data/val.jsonl as training and validation files respectively, and the model gpt-4o-mini-2024-07-18.
"""

import argparse
import logging
import os
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('finetune.log'),
        logging.StreamHandler()
    ]
)

def upload_to_openai(file_path: str) -> str:
    """Upload a JSONL file to OpenAI."""
    client = OpenAI(api_key=OPENAI_API_KEY)
    try:
        with open(file_path, "rb") as file:
            response = client.files.create(
                file=file,
                purpose="fine-tune"
            )
        logging.info(f"Successfully uploaded {file_path}")
        logging.info(f"File ID: {response.id}")
        print(f"File Info: {response}")
        return response.id
    except Exception as e:
        logging.error(f"Error uploading {file_path}: {str(e)}")
        return None

def initiate_fine_tuning(training_file_id: str, 
                        validation_file_id: str, 
                        base_model: str = "gpt-4o-mini-2024-07-18") -> str:
    """Start the fine-tuning process."""
    client = OpenAI()
    base_model = base_model or os.getenv('BASE_MODEL', 'gpt-4o-mini-2024-07-18')
    try:
        model = client.fine_tuning.jobs.create(
            training_file=training_file_id,
            validation_file=validation_file_id,
            model=base_model,
            hyperparameters={
                "n_epochs": 3
            }
        )
        
        job_id = model.id
        status = model.status
        
        logging.info("Fine-tuning initiated successfully")
        logging.info(f"Job ID: {job_id}")
        print(f'Fine-tuning model with jobID: {job_id}.')
        print(f"Training Response: {model}")
        print(f"Training Status: {status}")
        
        return job_id
    except Exception as e:
        logging.error(f"Error initiating fine-tuning: {str(e)}")
        return None

def check_fine_tuning_status(job_id: str):
    """Check the status of a fine-tuning job."""
    client = OpenAI()
    try:
        status = client.fine_tuning.jobs.retrieve(job_id)
        logging.info(f"Fine-tuning status: {status}")
        return status
    except Exception as e:
        logging.error(f"Error checking fine-tuning status: {str(e)}")
        return None

def main(train_file: str, val_file: str, base_model: str):
    """Main function to handle the fine-tuning process."""
    # Ensure files exist
    if not os.path.exists(train_file):
        logging.error(f"Training file not found: {train_file}")
        return
    if not os.path.exists(val_file):
        logging.error(f"Validation file not found: {val_file}")
        return

    # Step 1: Upload files
    logging.info("Uploading training file...")
    train_file_id = upload_to_openai(train_file)
    if not train_file_id:
        logging.error("Failed to upload training file. Aborting.")
        return

    logging.info("Uploading validation file...")
    val_file_id = upload_to_openai(val_file)
    if not val_file_id:
        logging.error("Failed to upload validation file. Aborting.")
        return

    # Step 2: Start fine-tuning
    logging.info("Initiating fine-tuning process...")
    job_id = initiate_fine_tuning(train_file_id, val_file_id, base_model)
    
    if job_id:
        logging.info("Fine-tuning process started successfully")
        # Check initial status
        status = check_fine_tuning_status(job_id)
        logging.info(f"Initial status: {status}")
    else:
        logging.error("Failed to start fine-tuning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fine-tune OpenAI model with MongoDB query data')
    parser.add_argument('--train', type=str, default='data/train.jsonl',
                        help='Path to training data JSONL file')
    parser.add_argument('--val', type=str, default='data/val.jsonl',
                        help='Path to validation data JSONL file')
    parser.add_argument('--model', type=str, default='gpt-4o-mini-2024-07-18',
                        help='Base model to use for fine-tuning')
    
    args = parser.parse_args()
    
    main(args.train, args.val, args.model)