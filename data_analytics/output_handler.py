"""
output_handler.py

This script defines the OutputHandler class, which is responsible for managing the output of analysis
results from the procurement_analyzer.py script. It handles saving analysis data in multiple formats,
including JSON, human-readable text reports, and PDF documents.

Key Features:
1. Output Directory Management:
    - Initializes the output directory structure, creating subdirectories for 'json', 'text', and 'pdf' outputs.
    - This structure helps keep the results well organized.

2. Multiple Output Formats:
    - Supports saving analysis results in JSON, TXT (human-readable text), and PDF formats, offering flexibility
        for various use cases.
    - Provides a consistent method to output the results regardless of the desired format.
    - The formats are passed as an argument and can be `json`, `txt`, `pdf` or `all`.

3. JSON Output:
    - Saves the raw analysis results as a JSON file in the 'json' subdirectory, using `json.dump`, also using the `str` function in the default to avoid errors related to non-serializable data.

4. Text Report Generation:
    - Generates a human-readable text report by writing the analysis data in a structured format to a '.txt' file.
    - Includes formatting for clarity and readability: headers for sections, bullet point format for lists.
    - Shows basic stats, data quality, financial analysis and the top 5 values on each category.

5. PDF Report Generation:
    - Generates a basic PDF report using the `fpdf` library, showing some of the most relevant information.
    - It only includes title, basic stats and financial analysis, and is intended to be a simple example.
    - Includes error handling to ensure PDF generation is skipped if the `fpdf` is not installed.
    - Uses a `try-except` statement that allows the execution to continue if the library `fpdf` is not installed, logging a warning.
    - This allows for more flexible executions, even if not all of the dependencies are installed.

6. Timestamped Output Files:
    - Uses timestamps in output filenames to prevent overwriting previous analysis results.
    - It formats the timestamp using `datetime.now().strftime("%Y%m%d_%H%M%S")` to generate unique names, including the date and time.

7. Error Handling:
    - Includes try-except blocks to handle exceptions that may occur during file operations, printing logs that notify the user of any problems.
    - This ensures that the execution can continue even if some operations have failed.

8. Logging:
    - Uses the logging module to record messages about successful output creation or any potential errors, following the best practices for logging.
    - Logs information about:
        - Successful generation of the JSON, TXT and PDF outputs.
        - Any errors during the generation of these outputs.

Workflow:
    1. This script is designed to be used after the analysis is completed on `procurement_analyzer.py`
    2. The `OutputHandler` is initialized with the base output directory where subfolders for json, txt and pdf will be created.
    3. The `save_outputs` method is called with a dictionary with the analyzed data and a list with the desired output formats.
    4. Results will be stored in their own subdirectories and with unique filenames.

This script contributes to the project by providing an easy, flexible and reusable way to store the analysis results, using different formats for different applications.
"""
from pathlib import Path
import json
import logging
from datetime import datetime
from typing import Dict, Any

try:
    from fpdf import FPDF
except ImportError:
    logging.warning("fpdf not installed. PDF output will not be available.")

class OutputHandler:
    def __init__(self, output_dir: Path):
        """Initialize output handler with base directory"""
        self.output_dir = output_dir
        # Create output subdirectories
        for subdir in ['json', 'text', 'pdf']:
            (self.output_dir / subdir).mkdir(parents=True, exist_ok=True)

    def save_outputs(self, analysis_data: Dict[str, Any], formats: list = ['json']):
        """Save analysis results in specified formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for format_type in formats:
            if format_type == 'json':
                self._save_json(analysis_data, f"dataset_exploration_{timestamp}.json")
            elif format_type == 'txt':
                self._save_text_report(analysis_data, f"exploration_report_{timestamp}.txt")
            elif format_type == 'pdf':
                self._save_pdf_report(analysis_data, f"analysis_summary_{timestamp}.pdf")

    def _save_json(self, data: dict, filename: str):
        """Save raw analysis results as JSON"""
        try:
            output_path = self.output_dir / 'json' / filename
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logging.info(f"Saved JSON output to {output_path}")
        except Exception as e:
            logging.error(f"Error saving JSON output: {e}")

    def _save_text_report(self, data: dict, filename: str):
        """Generate and save a human-readable text report"""
        try:
            output_path = self.output_dir / 'text' / filename
            with open(output_path, 'w') as f:
                # Header
                f.write("PROCUREMENT DATA ANALYSIS REPORT\n")
                f.write("=" * 30 + "\n\n")
                
                # Basic Stats Section
                f.write("BASIC STATISTICS\n")
                f.write("-" * 20 + "\n")
                stats = data.get('basic_stats', {})
                for key, value in stats.items():
                    f.write(f"{key.replace('_', ' ').title()}: {value}\n")
                f.write("\n")
                
                # Data Quality Section
                f.write("DATA QUALITY ASSESSMENT\n")
                f.write("-" * 20 + "\n")
                quality = data.get('data_quality', {})
                if 'null_counts' in quality:
                    f.write("Missing Values:\n")
                    for field, count in quality['null_counts'].items():
                        f.write(f"  - {field}: {count}\n")
                if 'price_calculation_mismatches' in quality:
                    f.write(f"\nPrice Calculation Mismatches: {quality['price_calculation_mismatches']}\n")
                f.write("\n")
                
                # Financial Metrics Section
                f.write("FINANCIAL ANALYSIS\n")
                f.write("-" * 20 + "\n")
                financial = data.get('financial_metrics', {})
                if financial:
                    f.write(f"Total Spend: ${financial.get('total_spend', 0):,.2f}\n")
                    f.write(f"Average Unit Price: ${financial.get('average_unit_price', 0):,.2f}\n")
                    price_range = financial.get('price_range', {})
                    f.write(f"Price Range: ${price_range.get('min', 0):,.2f} - ${price_range.get('max', 0):,.2f}\n")
                f.write("\n")
                
                # Categorical Distribution Section
                f.write("TOP CATEGORIES\n")
                f.write("-" * 20 + "\n")
                categories = data.get('categorical_distribution', {})
                for field, values in categories.items():
                    f.write(f"\n{field}:\n")
                    for item in values[:5]:  # Show top 5
                        f.write(f"  - {item['value']}: {item['count']} records\n")
                
            logging.info(f"Saved text report to {output_path}")
        except Exception as e:
            logging.error(f"Error saving text report: {e}")

    def _save_pdf_report(self, data: dict, filename: str):
        """Generate and save a PDF report with formatting and charts"""
        try:
            if 'fpdf' not in globals():
                logging.error("PDF generation requires fpdf package. Please install with: pip install fpdf2")
                return

            pdf = FPDF()
            pdf.add_page()
            
            # Title
            pdf.set_font("Arial", "B", 16)
            pdf.cell(0, 10, "Procurement Data Analysis Report", ln=True, align='C')
            pdf.ln(10)
            
            # Basic Stats
            pdf.set_font("Arial", "B", 14)
            pdf.cell(0, 10, "Basic Statistics", ln=True)
            pdf.set_font("Arial", "", 12)
            stats = data.get('basic_stats', {})
            for key, value in stats.items():
                pdf.cell(0, 10, f"{key.replace('_', ' ').title()}: {value}", ln=True)
            pdf.ln(5)
            
            # Financial Metrics
            pdf.set_font("Arial", "B", 14)
            pdf.cell(0, 10, "Financial Analysis", ln=True)
            pdf.set_font("Arial", "", 12)
            financial = data.get('financial_metrics', {})
            if financial:
                pdf.cell(0, 10, f"Total Spend: ${financial.get('total_spend', 0):,.2f}", ln=True)
                pdf.cell(0, 10, f"Average Unit Price: ${financial.get('average_unit_price', 0):,.2f}", ln=True)
            
            # Save PDF
            output_path = self.output_dir / 'pdf' / filename
            pdf.output(str(output_path))
            logging.info(f"Saved PDF report to {output_path}")
        except Exception as e:
            logging.error(f"Error saving PDF report: {e}")

if __name__ == "__main__":
    # This script is not meant to be run directly
    pass