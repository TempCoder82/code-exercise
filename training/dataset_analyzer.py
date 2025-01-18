"""
dataset_analyzer.py

This script analyzes datasets prepared for OpenAI fine-tuning, providing insights
into token usage, query types, and recommending the appropriate number of training epochs.
It processes both the training and validation JSONL datasets.

The script performs the following steps:

1.  **Dataset Loading**:
    -   Loads a JSONL (JSON Lines) file, where each line is a JSON object.
    -   The expected structure for each line is:
        ```json
        {
            "messages": [
              {
                "role": "system",
                "content": "..."
              },
              {
                "role": "user",
                "content": "..."
              },
              {
                "role": "assistant",
                "content": "..."
              }
            ]
        }
        ```
         *   Converts this to a `List[Dict[str, Any]]` for further processing.

2.  **Token Counting**:
    -   Uses the `tiktoken` library to count the number of tokens in both
        the `user` prompt and the `assistant` response of each data point.
    -   Calculates the total tokens, sum of prompt and response tokens.

3.  **Dataset Analysis**:
    -   Calculates descriptive statistics for token counts (min, max, mean, median, 5th and 95th percentiles).
    -   Analyzes the type of queries ('aggregate', 'find', or 'other') present in the response.
    -   Counts the number of pipeline stages for the aggregate queries
    -   Estimates the total billable tokens, capped at the max context length supported by openai models (16385).

4.  **Training Epoch Recommendation**:
    -   Recommends a number of training epochs based on the dataset size with a default of 3 epochs, with adjustments for very small or very large datasets.
    -   Calculates the total training tokens.

5.  **Output**:
    -   Prints detailed analysis statistics for the train and val datasets, including:
        -   Total number of examples.
        -   Distribution of tokens (min/max, mean/median, p5/p95).
        -   Distribution of query types.
        -   Distribution of pipeline stages for aggregation queries.
        -   Total estimated billable tokens.
        -   Recommended training epochs.
        -   Total training tokens.

6.  **Command-Line Execution**:
    -   The script includes a `if __name__ == "__main__":` block, which calls all functions to load and analyze the train and val files present in the `data` folder.

7.  **Dependencies**:
    -   Requires the `json`, `tiktoken`, and `numpy` libraries to be installed.
    -   Use `pip install tiktoken numpy` to install these packages.
   
**Functions:**
    -   load_dataset: Loads the dataset from JSONL file.
    -   count_tokens: Counts the number of tokens in prompt and response.
    -   analyze_dataset: Analyzes the dataset and prints statistics.
    -   main: orchestrates the loading and analyzing of train and val files.

**Usage:**
   The script is designed to be run directly to analyze the data, mainly for fine tuning data analysis. It's intended to run after the data formatting step.

   Example:
   The `main` function is the main entry point of the script. Which takes no arguments, it loads the train and val datasets, then calls `analyse_dataset` for each.

"""

import json
import tiktoken
import numpy as np
from collections import defaultdict
from typing import List, Dict, Any

def load_dataset(file_path: str) -> List[Dict[str, Any]]:
    """Load dataset from JSONL file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f]

def count_tokens(example: Dict[str, Any], encoding: tiktoken.Encoding) -> Dict[str, int]:
    """Count tokens in a prompt/response pair."""
    prompt_tokens = len(encoding.encode(example["prompt"]))
    response_tokens = len(encoding.encode(json.dumps(example["response"])))
    return {
        "prompt_tokens": prompt_tokens,
        "response_tokens": response_tokens,
        "total_tokens": prompt_tokens + response_tokens
    }

def analyze_dataset(dataset: List[Dict[str, Any]], name: str) -> None:
    """Analyze dataset and print statistics."""
    print(f"\n=== Analyzing {name} dataset ===")
    print(f"Number of examples: {len(dataset)}")
    
    # Token statistics
    encoding = tiktoken.get_encoding("cl100k_base")
    
    prompt_lens = []
    response_lens = []
    total_lens = []
    pipeline_stages = []
    
    # Query type analysis
    query_types = defaultdict(int)
    
    for ex in dataset:
        # Token counts
        tokens = count_tokens(ex, encoding)
        prompt_lens.append(tokens["prompt_tokens"])
        response_lens.append(tokens["response_tokens"])
        total_lens.append(tokens["total_tokens"])
        
        # Query type and pipeline analysis
        response = ex["response"]
        if "aggregate" in response:
            query_types["aggregate"] += 1
            if "pipeline" in response:
                pipeline_stages.append(len(response["pipeline"]))
        elif "find" in response:
            query_types["find"] += 1
        else:
            query_types["other"] += 1
    
    def print_distribution(values: List[float], name: str) -> None:
        print(f"\nDistribution of {name}:")
        print(f"min / max: {min(values)}, {max(values)}")
        print(f"mean / median: {np.mean(values):.1f}, {np.median(values):.1f}")
        print(f"p5 / p95: {np.quantile(values, 0.05):.1f}, {np.quantile(values, 0.95):.1f}")
    
    print_distribution(prompt_lens, "prompt tokens")
    print_distribution(response_lens, "response tokens")
    print_distribution(total_lens, "total tokens per example")
    if pipeline_stages:
        print_distribution(pipeline_stages, "pipeline stages in aggregate queries")
    
    print("\nQuery type distribution:")
    for query_type, count in query_types.items():
        percentage = (count / len(dataset)) * 100
        print(f"{query_type}: {count} ({percentage:.1f}%)")
    
    # Training metrics
    total_tokens = sum(min(16385, length) for length in total_lens)
    print(f"\nToken usage estimation:")
    print(f"Total billable tokens: {total_tokens:,}")
    
    # Calculate recommended epochs based on dataset size
    n_examples = len(dataset)
    n_epochs = 3  # default target
    
    if n_examples < 100:
        n_epochs = min(25, 100 // n_examples)
    elif n_examples > 25000:
        n_epochs = max(1, 25000 // n_examples)
    
    print(f"Recommended epochs: {n_epochs}")
    print(f"Total training tokens: {n_epochs * total_tokens:,}")

def main():
    # Analyze training dataset
    train_dataset = load_dataset("data/train.jsonl")
    analyze_dataset(train_dataset, "Training")
    
    # Analyze validation dataset
    val_dataset = load_dataset("data/val.jsonl")
    analyze_dataset(val_dataset, "Validation")

if __name__ == "__main__":
    main()