"""
data_formatter.py

This script converts a JSON file containing successful natural language question
and MongoDB query pairs into the format required for OpenAI fine-tuning.
It also splits the data into training and validation sets.

The script performs the following steps:

1.  **Data Loading**:
    -   Loads a JSON file containing a list of successful query results. Each result
        is expected to have a 'question' field (string) and a 'query' field (dict).

2.  **Data Formatting**:
    -   Transforms each item into a dictionary in the format required for OpenAI
        fine-tuning using a messages array. The structure includes:
        -   A system message that defines the behavior of the model (conversion of NL to MongoDB query).
        -   A user message containing the natural language question.
        -   An assistant message containing the corresponding MongoDB query as a string.

3.  **Data Splitting**:
    -   Shuffles all the formatted entries using random.shuffle with seed.
    -   Splits the formatted data into training and validation sets based on a given
        `train_ratio` (default is 0.8).

4.  **Data Saving**:
    -   Saves the training data to a `.jsonl` file (JSON Lines), where each line contains one training example in json format.
    -   Saves the validation data to a `.jsonl` file in the same format.

5.  **Output and Summary**:
    -   Prints a summary indicating the number of training and validation examples
        and the paths where the files were saved and an example of the format.

6.  **Command-Line Execution**:
    -   The script includes a `if __name__ == "__main__":` block for testing.
    -   This block sets up a fixed random seed for reproducibility.
    -   It defines input and output file names and uses the `convert_to_training_format` function to perform the transformation, split, and saving of data.

7.  **Dependencies**:
    -   Requires the `json` and `random` libraries, which are part of the Python Standard Library.
    -   No external libraries need to be installed.
   
**Functions:**
    -   convert_to_training_format: This function takes the json file and converts to openai format. It splits into training and validation files.

**Usage:**
   The script is designed to be run directly, primarily for preparing data for fine-tuning.

   Example (within the script itself):
    The script is called in the main function with the parameters for the input file, training and validation file names. And a train ratio.

   You can customize the input, output paths and training ration directly inside the main function in this file.

"""

import json
import random

def convert_to_training_format(json_file: str, train_file: str, val_file: str, train_ratio: float = 0.8):
    """
    Convert successful queries to OpenAI fine-tuning format and split into train/val sets.
    
    Args:
        json_file: Path to JSON file containing successful queries
        train_file: Path to save training data
        val_file: Path to save validation data
        train_ratio: Ratio of data to use for training (default: 0.8)
    """
    # Read JSON file
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Convert each example to training format
    formatted_data = []
    for item in data:
        example = {
            "messages": [
                {
                    "role": "system",
                    "content": "You are an assistant that converts natural language questions into MongoDB queries. Ensure the query is properly formatted and uses the correct MongoDB operators. Return only the query without any explanations."
                },
                {
                    "role": "user",
                    "content": item["question"]
                },
                {
                    "role": "assistant",
                    "content": json.dumps(item["query"], indent=2)
                }
            ]
        }
        formatted_data.append(example)
    
    # Shuffle and split data
    random.shuffle(formatted_data)
    split_idx = int(len(formatted_data) * train_ratio)
    train_data = formatted_data[:split_idx]
    val_data = formatted_data[split_idx:]
    
    # Write training data
    with open(train_file, 'w', encoding='utf-8') as f:
        for example in train_data:
            f.write(json.dumps(example) + '\n')
    
    # Write validation data
    with open(val_file, 'w', encoding='utf-8') as f:
        for example in val_data:
            f.write(json.dumps(example) + '\n')
    
    print(f"Dataset split into:")
    print(f"- Training: {len(train_data)} examples saved to {train_file}")
    print(f"- Validation: {len(val_data)} examples saved to {val_file}")
    print(f"\nSample format:")
    print(json.dumps(train_data[0], indent=2))

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    
    input_file = "successful_queries_20250117_211113.json"
    train_file = "train.jsonl"
    val_file = "val.jsonl"
    train_ratio = 0.8
    
    convert_to_training_format(
        json_file=input_file,
        train_file=train_file,
        val_file=val_file,
        train_ratio=train_ratio
    )