# Procurement Data Assistant

This repository contains my solution for the AI-driven procurement assistant assessment. I've broken down the implementation into four focused sub-repositories, each handling a specific aspect of the solution.

## Project Structure

```
procurement-assistant/
├── data_analytics/          # Data exploration and MongoDB integration
├── dataset_generation/      # Training data creation and formatting
├── model_training/          # Fine-tuning OpenAI model
└── evaluation_and_demo/     # Model evaluation and interactive demo
```

## Sub-Repositories Overview

### 1. Data Analytics
- Initial data exploration and profiling
- MongoDB integration with size optimization (200K records)
- Comprehensive data analysis, including:
  - Basic statistics
  - Data quality assessment
  - Temporal patterns
  - Financial metrics
  - Categorical distributions

### 2. Dataset Generation
- Generation of natural language queries using GPT-4
- Conversion of queries to MongoDB syntax
- Dataset validation and cleaning
- Training/validation set splitting
- Supports both find and aggregate queries

### 3. Model Training
- Data formatting for OpenAI fine-tuning
- Dataset analysis for token usage and epochs
- Fine-tuning process management
- Training progress monitoring

### 4. Evaluation and Demo
- Comprehensive model evaluation pipeline
- Interactive Gradio-based demo interface
- Query execution validation
- Performance metrics tracking

## Assessment Requirements Mapping

The solution addresses the key requirements from the assessment:

1. **Data Exploration** → `data_analytics/`
   - MongoDB integration ✓
   - Dataset structure analysis ✓
   - Feature exploration ✓

2. **Model Development** → `dataset_generation/` & `model_training/`
   - NLP implementation ✓
   - Query parsing algorithms ✓
   - Model training ✓

3. **Chatting Assistant** → `evaluation_and_demo/`
   - Conversational interface ✓
   - Query handling ✓
   - Sample queries support ✓

4. **Evaluation** → `evaluation_and_demo/`
   - Accuracy testing ✓
   - Performance metrics ✓

## Getting Started

Each sub-repository contains its own README with specific setup and usage instructions. The general workflow is:

1. Start with `data_analytics` to understand and load the data
2. Use `dataset_generation` to create training examples
3. Run `model_training` to fine-tune the model
4. Deploy and test using `evaluation_and_demo`

## Technology Stack

- MongoDB Atlas for data storage
- Python with pandas for data analysis
- OpenAI API for model training
- Claude for dataset generation and model evaluation
- Gradio for demo interface
- Various ML libraries (see individual READMEs)