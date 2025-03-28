# E2Duck: Excel to DuckDB Converter

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.7%2B-blue)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/yourusername/e2duck/graphs/commit-activity)

A robust and efficient tool for converting Excel files to DuckDB databases with comprehensive validation and optimization.

## Features

- **Universal Compatibility**: Process any Excel file regardless of format or structure
- **Multi-sheet Support**: Convert each worksheet to a separate database table
- **Intelligent Type Inference**: Automatically detect and map data types
- **Safe Mode**: Ensure data integrity with conservative type conversion
- **Large File Handling**: Process large Excel files with reservoir sampling
- **Batch Processing**: Efficiently import data in configurable batches
- **Comprehensive Validation**: Verify data integrity after import
- **Performance Optimization**: Automatically optimize tables with indexes
- **Detailed Logging**: Track the entire process with comprehensive logs
- **Parallel Processing**: Utilize multi-threading for faster Excel analysis
- **Memory-Aware**: Dynamically adjust batch sizes based on available system memory
- **Robust Error Handling**: Gracefully handle various error scenarios with fallback mechanisms

## Installation

### Prerequisites

- Python 3.7 or higher
- [uv](https://github.com/astral-sh/uv) - Fast Python package installer and resolver

### Installing uv

If you don't have uv installed, you can install it using:

```bash
# On macOS/Linux using curl
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows using PowerShell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Using pip
pip install uv
```

### Setting up the project

```bash
# Clone the repository
git clone https://github.com/yourusername/e2duck.git
cd e2duck

# Create a virtual environment using uv
uv venv

# Activate the virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install dependencies using uv
uv pip install -r requirements.txt

# Or install dependencies directly
uv pip install pandas duckdb psutil
```

### Verifying Installation

To verify that everything is installed correctly:

```bash
# Make sure you're in the virtual environment
python -c "import pandas; import duckdb; import psutil; print('Installation successful!')"
```

## Usage

### Command Line Interface

```bash
python e2duck.py
```

Follow the interactive prompts to specify the Excel file path and DuckDB database path.

### As a Module

```python
from e2duck import ExcelToDuckDB

# Initialize the converter
converter = ExcelToDuckDB(
    excel_path="path/to/your/file.xlsx",
    db_path="path/to/output.duckdb",
    sample_size=100,  # Number of rows to sample for validation
    safe_mode=True    # Use TEXT type for all columns to ensure data integrity
)

# Run the full import process
result = converter.run()

# Check the result
if result['success']:
    print("Import successful!")
    
    # Access validation results
    validation_results = result.get('validation_results', [])
    for validation in validation_results:
        print(f"Sheet '{validation['sheet_name']}': {validation['overall_status']}")
else:
    print(f"Import failed: {result.get('error', 'Unknown error')}")
```

## Advanced Usage

### Dynamic Batch Size

The tool automatically adjusts batch sizes based on available system memory:

```python
# Let the system determine optimal batch size based on memory
result = converter.import_data()

# Or specify a custom batch size
result = converter.import_data(batch_size=10000)
```

### Parallel Sheet Processing

For multi-sheet Excel files, sheets are processed in parallel:

```python
# Analysis phase uses parallel processing for multiple sheets
sheets_info = converter.analyze_excel()
```

## Workflow

1. **Connection**: Establish connection to DuckDB with optimized settings
2. **Analysis**: Scan Excel structure and extract sample data using parallel processing
3. **Table Creation**: Create DuckDB tables based on Excel structure
4. **Data Import**: Transfer data using memory-aware batch processing
5. **Validation**: Verify data integrity and completeness
6. **Optimization**: Add indexes and optimize table structure

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `excel_path` | Path to the Excel file | Required |
| `db_path` | Path to the DuckDB database | Required |
| `sample_size` | Number of rows to sample for validation | 100 |
| `safe_mode` | Use TEXT type for all columns | True |
| `batch_size` | Number of rows to process in each batch | Dynamic based on memory |

## Performance Considerations

- **Memory Usage**: The tool uses batch processing and reservoir sampling to minimize memory usage
- **Dynamic Batch Sizing**: Automatically adjusts batch size based on available system memory
- **Processing Speed**: Configurable batch size to balance between speed and memory usage
- **Database Optimization**: Automatic index creation for frequently queried columns
- **Parallel Processing**: Multi-threaded Excel analysis and DuckDB's parallel processing capabilities
- **Excel Engine Selection**: Attempts to use the most efficient Excel engine for each file format
- **File Format**: `.xlsx` files process faster than `.xls` files due to better engine support

## Error Handling

The tool provides comprehensive error handling with:

- Detailed error messages with specific error types
- Standardized logging with appropriate log levels
- Transaction-based imports to prevent partial data imports
- Multiple fallback mechanisms for data import (COPY, bulk INSERT, row-by-row INSERT)
- Validation to ensure data integrity

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Language Versions

This project is available in two language versions:

- **English Version** (`e2duck.py`): The main version with all comments, logs, and documentation in English.
- **Chinese Version** (`e2duck_cn.py`): Original version with comments, logs, and documentation in Chinese.

Both versions have identical functionality and code structure, differing only in the language of comments and text messages. Developers can choose the version that best suits their language preference.
