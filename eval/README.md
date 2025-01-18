# Model Evaluation and Demo

This sub-repository provides tools for evaluating and demonstrating the fine-tuned MongoDB query generation model. It includes both an automated evaluation pipeline and an interactive demo interface.

## Scripts Overview

### 1. Model Evaluator (`model_evaluator.py`)
Comprehensive evaluation pipeline that:
- Processes test questions and generates MongoDB queries
- Executes queries against a test database
- Uses Claude to score query quality
- Generates detailed evaluation reports

Features:
- Multi-metric evaluation (syntax, schema, logic, completeness, efficiency)
- Query execution validation
- Detailed per-query analysis
- Aggregate performance metrics

### 2. Interactive Demo (`query_demo.py`)
Gradio-based web interface that:
- Accepts natural language questions
- Shows generated MongoDB queries
- Displays query execution results
- Provides example questions

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
   ANTHROPIC_API_KEY="your_anthropic_api_key"
   MONGODB_USERNAME="your_mongodb_username"
   MONGODB_PASSWORD="your_mongodb_password"
   MONGODB_CLUSTER_URL="your_mongodb_cluster_url"
   MODEL_NAME="your_fine_tuned_model_id"
   ```

## Usage

### Running Evaluation
```bash
python model_evaluator.py questions.txt --limit 100
```
This will:
- Process the first 100 questions
- Generate evaluation metrics
- Create detailed reports in:
  - `evaluation_results/evaluation_results_{timestamp}.json`
  - `generated_queries/query_{timestamp}_{n}.json`

### Launching Demo
```bash
python query_demo.py
```
This will:
- Start the Gradio interface
- Provide a local URL
- Generate a public share link

## Output Files

### Evaluation Results
- **Individual Queries**: `generated_queries/query_{timestamp}_{n}.json`
  ```json
  {
    "question": "...",
    "generated_query": "...",
    "execution_result": {
      "success": true,
      "message": "...",
      "results_count": 42
    },
    "claude_evaluation": {
      "syntax_score": 5,
      "schema_score": 5,
      ...
    }
  }
  ```

- **Aggregate Results**: `evaluation_results/evaluation_results_{timestamp}.json`
  ```json
  {
    "timestamp": "20250118_120000",
    "total_questions": 100,
    "successful_executions": 95,
    "execution_success_rate": 0.95,
    "average_total_score": 8.7
  }
  ```

## Key Features

1. **Comprehensive Evaluation**
   - Query syntax validation
   - Schema compliance checking
   - Execution success verification
   - Semantic accuracy scoring
   - Performance metrics

2. **Interactive Testing**
   - Real-time query generation
   - Immediate execution feedback
   - Example questions
   - JSON result formatting

3. **Detailed Logging**
   - Per-query analysis
   - Execution statistics
   - Error tracking
   - Performance metrics