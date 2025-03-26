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

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/e2duck.git
cd e2duck

# Create a virtual environment (using uv)
uv venv

# Activate the virtual environment
source .venv/bin/activate  # On Unix/macOS
# OR
.venv\Scripts\activate  # On Windows

# Install dependencies
uv pip install pandas duckdb
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
else:
    print(f"Import failed: {result.get('error', 'Unknown error')}")
```

## Workflow

1. **Analysis**: Scan Excel structure and extract sample data
2. **Table Creation**: Create DuckDB tables based on Excel structure
3. **Data Import**: Transfer data using batch processing
4. **Validation**: Verify data integrity and completeness
5. **Optimization**: Add indexes and optimize table structure

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `excel_path` | Path to the Excel file | Required |
| `db_path` | Path to the DuckDB database | Required |
| `sample_size` | Number of rows to sample for validation | 100 |
| `safe_mode` | Use TEXT type for all columns | True |
| `batch_size` | Number of rows to process in each batch | 5000 |

## Performance Considerations

- **Memory Usage**: The tool uses batch processing and reservoir sampling to minimize memory usage
- **Processing Speed**: Configurable batch size to balance between speed and memory usage
- **Database Optimization**: Automatic index creation for frequently queried columns
- **Parallel Processing**: DuckDB's parallel processing capabilities are enabled by default

## Error Handling

The tool provides comprehensive error handling with:
- Detailed error messages
- Extensive logging
- Transaction-based imports to prevent partial data imports
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
