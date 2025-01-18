# Data Analytics

A comprehensive system for loading, analyzing, and exploring procurement data using MongoDB Atlas and Python.

## Project Structure
```
procurement-analysis/
├── src/                      # Source code directory
│   ├── data_profiler.py      # Data profiling script
│   ├── mongodb_loader.py     # Data loading script
│   ├── procurement_analyzer.py # Data analysis script
│   └── output_handler.py     # Output utilities
├── requirements.txt          # Python dependencies
├── analysis_output/          # Output directory for analysis results
│   ├── json/                 # JSON output files
│   ├── text/                 # TXT output files
│   └── pdf/                  # PDF output files
└── README.md                 # This README file
```

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
   MONGODB_USERNAME=your_mongodb_username
   MONGODB_PASSWORD=your_mongodb_password
   MONGODB_CLUSTER_URL=your_mongodb_cluster_url
   ```

## Important Note on Data Size

The original dataset contains over 340,000 procurement records. However, due to MongoDB Atlas free tier's 512MB storage limit, this implementation is configured to work with a subset of 200,000 records. This ensures compatibility with MongoDB Atlas free tier while maintaining a representative sample for analysis.

## Usage

### 1. Profile Data
Profile the complete dataset structure:
```bash
python src/data_profiler.py path/to/your/procurement_data.csv
```
Identifies:
- Column data types and distributions
- Missing values
- Data quality issues

### 2. Load to MongoDB
Load a subset of data to MongoDB Atlas:
```bash
python src/mongodb_loader.py path/to/your/procurement_data.csv --max-rows 200000
```
Features:
- Loads first 200,000 records (configurable via --max-rows)
- Monitors database size to stay within Atlas free tier limit
- Creates indexes on key fields
- Handles batched inserts

### 3. Analyze Data
Generate insights from loaded data:
```bash
python src/procurement_analyzer.py --output-format all
```

## Analysis Insights

Analysis of the 200,000 record subset provides comprehensive insights:

### Basic Statistics
- Total Records: 200,000 (subset of 340,000+ total records)
- Unique Departments: 110
- Unique Suppliers: 20,538
- Unique Items: 109,151
- Acquisition Types: 5

### Data Quality Metrics
- Missing Values:
  - Purchase Date: 9,532 missing entries
  - Other key fields: Complete data
- Price Calculation Mismatches: 10,372 records

### Temporal Coverage
- Creation Dates: July 2012 - June 2015
- Fiscal Years: 2012-2013, 2013-2014, 2014-2015

### Categorical Insights
Top Departments:
1. Corrections and Rehabilitation (30,285 records)
2. Water Resources (21,752 records)
3. Correctional Health Care Services (18,782 records)

Top Suppliers:
1. Voyager Fleet Systems Inc (13,756 records)
2. Western Blue (5,023 records)
3. Grainger Industrial Supply (4,516 records)

Acquisition Types Distribution:
1. NON-IT Goods: 123,796
2. NON-IT Services: 39,352
3. IT Goods: 30,217
4. IT Services: 6,563
5. IT Telecommunications: 72

## Output Formats

Analysis results are saved in three formats:

### JSON (`json/`)
Detailed structured data including:
```json
{
  "basic_stats": {
    "total_records": 200000,
    "unique_departments": 110,
    "unique_suppliers": 20538
  },
  "data_quality": {
    "null_counts": {
      "purchase_date": 9532
    },
    "price_calculation_mismatches": 10372
  }
}
```

### Text Report (`text/`)
Human-readable summary:
```
PROCUREMENT DATA ANALYSIS
========================
Total Records: 200,000 (subset of 340,000+ total records)
Unique Departments: 110
Top Department: Corrections and Rehabilitation (30,285 records)
```

### PDF Report (`pdf/`)
Professional report including:
- Executive summary
- Key metrics
- Top categories visualization