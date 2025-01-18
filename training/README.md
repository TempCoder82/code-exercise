# Model Fine-Tuning Pipeline

This repository contains tools for preparing and fine-tuning an OpenAI model to convert natural language questions into MongoDB queries. The pipeline handles data formatting, dataset analysis, and model training.

## Pipeline Overview

1. **Data Formatting** (`data_formatter.py`):
   - Converts successful query pairs into OpenAI's fine-tuning format
   - Creates JSONL files with the required message structure:
     ```json
     {
       "messages": [
         {"role": "system", "content": "You are an assistant..."},
         {"role": "user", "content": "How many orders were placed in 2023?"},
         {"role": "assistant", "content": "{\"$match\": {\"fiscal_year\": \"2023\"}}"}
       ]
     }
     ```
   - Splits data into training and validation sets

2. **Dataset Analysis** (`dataset_analyzer.py`):
   - Analyzes token usage for training cost estimation
   - Provides statistics on query types and complexity
   - Recommends optimal number of training epochs
   - Validates dataset formatting

3. **Model Fine-tuning** (`finetune_model.py`):
   - Handles dataset upload to OpenAI
   - Initiates and monitors fine-tuning process
   - Configures training parameters based on analysis

## Setup

1. **Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configuration**
   Create a `.env` file:
   ```plaintext
   OPENAI_API_KEY="your_openai_api_key"
   BASE_MODEL="gpt-4o-mini-2024-07-18"  # Optional
   ```

## Usage Guide

### 1. Format Training Data
```bash
python data_formatter.py \
  --input successful_queries.json \
  --train-output train.jsonl \
  --val-output val.jsonl \
  --train-ratio 0.8
```

This converts the query pairs into the format required for fine-tuning.

### 2. Analyze Dataset
```bash
python dataset_analyzer.py \
  --train data/train.jsonl \
  --val data/val.jsonl
```

This provides insights about the dataset:
- Token counts and distribution
- Query type analysis
- Recommended number of epochs
- Estimated training costs

### 3. Fine-tune Model
```bash
python finetune_model.py \
  --train data/train.jsonl \
  --val data/val.jsonl \
  --model gpt-4o-mini-2024-07-18
```

This initiates the fine-tuning process and monitors its status.

## Output Files

- `train.jsonl`, `val.jsonl`: Formatted datasets
- `finetune.log`: Training progress and errors
- Analysis reports with dataset statistics

## Key Features

1. **Data Preprocessing**
   - Converts JSON to JSONL format
   - Validates message structure
   - Configurable train/validation split

2. **Dataset Analysis**
   - Token counting using `tiktoken`
   - Query complexity metrics
   - Training cost estimation
   - Epoch optimization

3. **Training Management**
   - Automated file upload
   - Progress monitoring
   - Error handling with retries
   - Detailed logging

## Technical Notes

- Uses OpenAI's fine-tuning endpoint
- Implements exponential backoff for API calls
- Supports custom base models
- Provides detailed logging for debugging
- Validates input data format