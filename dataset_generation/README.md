# Natural Language to MongoDB Query Generator

This project demonstrates a system for converting natural language questions into executable MongoDB queries using AI language models. It showcases integration with OpenAI's GPT-4 and Anthropic's Claude, along with MongoDB database operations and error handling.

## Project Overview

The system consists of two main components:

1. **Natural Language Query Generator** (`natural_language_query_generator.py`):
   - Generates training data using GPT-4
   - Creates diverse question sets for database queries
   - Supports splitting data into training/testing sets

2. **Query Executor** (`claude_query_executor.py`):
   - Converts natural language to MongoDB queries using Claude
   - Validates and executes queries against MongoDB
   - Provides comprehensive error tracking and logging

## Setup Instructions

1. **Environment Setup**
   ```bash
   # Create and activate a virtual environment (recommended)
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Configuration**
   Create a `.env` file in the project root with:
   ```plaintext
   OPENAI_API_KEY="your_openai_api_key"
   ANTHROPIC_API_KEY="your_anthropic_api_key"
   MONGODB_USERNAME="your_mongodb_username"
   MONGODB_PASSWORD="your_mongodb_password"
   MONGODB_CLUSTER_URL="your_mongodb_cluster_url"
   ```

## Usage

### Generate Natural Language Questions
```bash
python natural_language_query_generator.py <num_prompts> [--output <output_file>] [--split] [--train-ratio <ratio>] [--max-retries <retries>]

# Example: Generate 100 prompts with train/test split
python natural_language_query_generator.py 100 --output prompts.txt --split --train-ratio 0.8
```

### Execute and Validate Queries
```bash
python claude_query_executor.py <input_file> [--output <output_file>] [--api-key <api_key>]

# Example: Process questions from prompts.txt
python claude_query_executor.py prompts.txt --output results.txt
```

## Key Features

- **Robust Error Handling**: Comprehensive error tracking and logging
- **Data Validation**: Query structure validation and field name normalization
- **Flexible Output**: Support for various output formats and split datasets
- **Retry Logic**: Implements exponential backoff for API calls
- **Database Integration**: Direct validation against MongoDB

## Project Structure
```
├── natural_language_query_generator.py  # GPT-4 based prompt generator
├── claude_query_executor.py            # Query translator and executor
├── requirements.txt                    # Project dependencies
├── query_prompts/                      # Generated prompts directory
└── README.md                           # This file
```

## Output Files

The system generates several output files:
- `successful_queries_{timestamp}.json`: Successfully executed queries
- `error_log_{timestamp}.json`: Detailed error tracking
- Split datasets: `*_train.txt` and `*_test.txt` when using split option

## Technical Decisions

1. **Two-Stage Process**: Separating generation and execution allows for better error handling and validation.
2. **API Retry Logic**: Implements exponential backoff to handle rate limits and temporary failures.
3. **Field Normalization**: Ensures consistent naming conventions across queries.
4. **Comprehensive Logging**: Enables detailed analysis of system performance.

## Skills Demonstrated

- API Integration (OpenAI, Anthropic)
- Database Operations (MongoDB)
- Error Handling and Logging
- Configuration Management
- Command Line Interface Design
- Type Hinting and Documentation