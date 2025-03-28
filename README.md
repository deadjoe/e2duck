# E2Duck: Excel to DuckDB Converter

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.7%2B-blue)](https://www.python.org/downloads/)

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
- Required packages: pandas, duckdb, psutil

### Setting up the project

```bash
# Clone the repository
git clone https://github.com/yourusername/e2duck.git
cd e2duck

# Create and activate a virtual environment (optional but recommended)
python -m venv venv
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# For development (includes testing tools)
pip install -r requirements-dev.txt
```

### Verifying Installation

To verify that everything is installed correctly:

```bash
python -c "import pandas; import duckdb; import psutil; print('Installation successful!')"
```

## Usage

### Command Line Interface

```bash
python run.py path/to/excel_file.xlsx path/to/output.duckdb [options]
```

Available options:
- `--sample-size SIZE`: Number of rows to sample for validation (default: 100)
- `--no-safe-mode`: Disable safe mode (default: enabled)

### As a Module

```python
from e2duck.e2duck import ExcelToDuckDB

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
        print(f"Sheet '{validation['sheet']}': {validation['overall_status']}")
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

## Testing

The project includes comprehensive test coverage (62%) to ensure functionality and reliability:

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_initialization.py

# Run tests with coverage report
pytest --cov=e2duck tests/

# Generate detailed HTML coverage report
pytest --cov=e2duck --cov-report=html tests/
```

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