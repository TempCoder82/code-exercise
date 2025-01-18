"""
natural_lanaguage_query_generator.py

This script automates the generation of natural language query prompts for a procurement
database using OpenAI's GPT-4 model. These prompts are designed to be subsequently
translated into MongoDB queries. The script performs the following steps:

1.  **Initialization**:
    -   Loads environment variables, including the OpenAI API key.
    -   Initializes the `GPT4QueryGenerator` with the API key and defines the 
        context for the database schema used to create the natural language prompts.
    -   Initializes the `QueryPromptFileHandler` for writing prompts to files.
2.  **Prompt Generation**:
    -   Utilizes the `GPT4QueryGenerator` to generate a specified number of 
        natural language query prompts.
    -   The generator uses the database context to create prompts that cover various 
        query types including aggregation, filtering, and complex queries.
    -   Prompts are generated in small batches (1-2 at a time) with a short simulated thinking 
        time between batches, and exponential backoff retry logic in case of failure.
    -   The output is a list of natural language queries.
3.  **File Handling**:
    -   Uses the `QueryPromptFileHandler` to write generated prompts to a specified output file (default is 'prompts.txt' in the 'query_prompts' directory).
    -   If specified via command-line arguments, the handler can also split the generated prompts into training and testing files, with a specified ratio.
    -   The generated file is also automatically removed after splitting it.
    -   The output training and testing files are named with `_train.txt` and `_test.txt` postfix.

4.  **Command-Line Interface**:
    -   Uses `argparse` to handle command-line arguments, allowing the user to control:
        -   The number of prompts to generate.
        -   The output file name.
        -   Whether to split the output into training and test sets.
        -   The ratio of prompts to use for training when splitting the prompts.
        -   The maximum number of retries to attempt when making OpenAI API calls.

5.  **Error Handling**:
    -   Includes error handling for cases such as a missing OpenAI API key.
    -   Includes retry mechanisms with exponential backoff for OpenAI API calls that fail.
    -   Gracefully handles the case where the OpenAI API call fails after multiple retries, and
        returns an empty string to indicate that the call failed.

6.  **Dependencies**:
    -   Requires the `openai`, `python-dotenv`, and `typing` libraries. 
    -   Ensure these are installed before running the script using `pip install openai python-dotenv`
    
**Classes**:
    -   GPT4QueryGenerator: This class is responsible for calling the OpenAI API and generating prompts given a database context.
    -   QueryPromptFileHandler: This class manages file handling, including writing and splitting prompts into files.
    
**Usage:**
    To generate prompts and optionally split them into train/test sets:

        python natural_lanaguage_query_generator.py <num_prompts> [--output <output_file>] [--split] [--train-ratio <ratio>] [--max-retries <retries>]

    Example:
        Generate 100 prompts and store them in 'my_prompts.txt':
            python natural_lanaguage_query_generator.py 100 --output my_prompts.txt

        Generate 50 prompts, split into train/test sets with an 80/20 ratio, and use 5 retries:
            python natural_lanaguage_query_generator.py 50 --split --train-ratio 0.8 --max-retries 5

"""
import random
import argparse
import time
import os
from typing import List, Optional
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

class GPT4QueryGenerator:
    def __init__(self, max_retries: int = 3):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OpenAI API key not found in environment variables")
        
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-4o"
        self.max_retries = max_retries
        
        # Database schema and context for GPT-4
        self.db_context = """
        MongoDB Database Schema for Procurement Data:
        - Fields:
            - Dates: creation_date, purchase_date
            - Amounts: unit_price, total_price, quantity
            - Categories: acquisition_type, sub_acquisition_type, acquisition_method, sub_acquisition_method
            - Organization: department_name, location
            - Supplier: supplier_code, supplier_name, supplier_qualifications, supplier_zip_code, calcard
            - Item: item_name, item_description
            - Classification: classification_codes, normalized_unspsc, commodity_title, class, class_title, family, family_title, segment, segment_title
            - Reference: lpa_number, purchase_order_number, requisition_number
            - Temporal: fiscal_year
        
        Known Values:
        - Acquisition Types: NON-IT Goods, NON-IT Services, IT Goods, IT Services, IT Telecommunications
        - Fiscal Years: 2012-2013, 2013-2014, 2014-2015
        - Major Departments: Corrections and Rehabilitation, Water Resources, Correctional Health Care Services
        
        Generate natural language queries that can be translated to MongoDB queries. Focus on:
        1. Aggregation queries (grouping, counting, averaging)
        2. Find queries (filtering, matching)
        3. Complex queries (multiple operations, joins, comparisons)
        """

    def _call_openai_with_retry(self, prompt: str, max_tokens: int = 1000) -> Optional[str]:
        """Call OpenAI API with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self.db_context},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content
            except Exception as e:
                if attempt == self.max_retries - 1:
                    print(f"Failed after {self.max_retries} attempts: {e}")
                    return None
                print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def generate_prompts(self, num_prompts: int) -> List[str]:
        """Generate prompts using GPT-4"""
        prompts = []
        
        while len(prompts) < num_prompts:
            # Generate 1-2 prompts at a time
            batch_size = random.randint(1, 2)
            batch_size = min(batch_size, num_prompts - len(prompts))
            
            prompt = f"Generate {batch_size} unique, natural language queries that could be used to analyze this procurement database. Make the queries specific and varied in complexity."
            
            response_content = self._call_openai_with_retry(prompt, max_tokens=150 * batch_size)
            
            if response_content:
                # Extract queries from response
                new_prompts = response_content.split('\n')
                new_prompts = [p.strip() for p in new_prompts if p.strip()]
                prompts.extend(new_prompts[:batch_size])
                
                print(f"Generated {len(new_prompts)} new queries...")
                
                # Simulate thinking time between batches
                if len(prompts) < num_prompts:
                    time.sleep(random.uniform(1.0, 2.0))
            
        return prompts[:num_prompts]  # Ensure we return exactly num_prompts

class QueryPromptFileHandler:
    def __init__(self, output_dir: str = "query_prompts"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def write_prompts(self, prompts: List[str], filename: str):
        """Write prompts to a file, one per line"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w') as f:
            for prompt in prompts:
                f.write(f"{prompt}\n")
    
    def split_prompts(self, input_file: str, train_ratio: float = 0.8):
        """Split prompts into train and test files"""
        input_path = os.path.join(self.output_dir, input_file)
        
        with open(input_path, 'r') as f:
            prompts = f.readlines()
        
        # Shuffle prompts
        random.shuffle(prompts)
        
        # Calculate split point
        split_idx = int(len(prompts) * train_ratio)
        
        # Split prompts
        train_prompts = prompts[:split_idx]
        test_prompts = prompts[split_idx:]
        
        # Write train and test files
        train_file = input_file.replace('.txt', '_train.txt')
        test_file = input_file.replace('.txt', '_test.txt')
        
        self.write_prompts(train_prompts, train_file)
        self.write_prompts(test_prompts, test_file)
        
        return train_file, test_file

def main():
    parser = argparse.ArgumentParser(description='Generate MongoDB query prompts using GPT-4')
    parser.add_argument('num_prompts', type=int, help='Number of prompts to generate')
    parser.add_argument('--output', type=str, default='prompts.txt', 
                        help='Output filename (default: prompts.txt)')
    parser.add_argument('--split', action='store_true',
                        help='Split output into train and test files')
    parser.add_argument('--train-ratio', type=float, default=0.8,
                        help='Ratio for train/test split (default: 0.8)')
    parser.add_argument('--max-retries', type=int, default=3,
                        help='Maximum number of retry attempts for API calls')
    
    args = parser.parse_args()
    
    # Generate prompts using GPT-4
    generator = GPT4QueryGenerator(max_retries=args.max_retries)
    print(f"Generating {args.num_prompts} prompts using GPT-4...")
    prompts = generator.generate_prompts(args.num_prompts)
    
    # Handle file output
    file_handler = QueryPromptFileHandler()
    file_handler.write_prompts(prompts, args.output)
    print(f"Prompts written to {args.output}")
    
    # Split if requested
    if args.split:
        train_file, test_file = file_handler.split_prompts(args.output, args.train_ratio)
        print(f"Split prompts into {train_file} and {test_file}")
        # Remove original file after splitting
        os.remove(os.path.join(file_handler.output_dir, args.output))

if __name__ == "__main__":
    main()
