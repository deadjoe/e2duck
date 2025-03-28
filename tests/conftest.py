import os
import pytest
import pandas as pd
import duckdb
from unittest.mock import MagicMock

@pytest.fixture
def sample_excel_path():
    """Return path to a sample Excel file for testing."""
    return os.path.join(os.path.dirname(__file__), "data/input/xlsx/sample_small.xlsx")

@pytest.fixture
def sample_xls_path():
    """Return path to a sample XLS file for testing."""
    return os.path.join(os.path.dirname(__file__), "data/input/xls/sample_small.xls")

@pytest.fixture
def memory_db_path():
    """Return path for in-memory DuckDB database."""
    return ":memory:"

@pytest.fixture
def mock_excel_data():
    """Create mock Excel data for testing."""
    # Create a dictionary representing sheets in an Excel file
    mock_data = {
        "Sheet1": pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Item 1", "Item 2", "Item 3", "Item 4", "Item 5"],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5],
            "date": pd.date_range(start="2023-01-01", periods=5)
        }),
        "Sheet2": pd.DataFrame({
            "product_id": [101, 102, 103],
            "description": ["Product A", "Product B", "Product C"],
            "in_stock": [True, False, True],
            "quantity": [100, 0, 50]
        })
    }
    return mock_data

@pytest.fixture
def mock_excel_file(mocker, mock_excel_data):
    """Mock pandas ExcelFile and read_excel functions."""
    # Mock ExcelFile
    mock_excel = mocker.patch("pandas.ExcelFile")
    mock_excel.return_value.sheet_names = list(mock_excel_data.keys())
    
    # Mock read_excel
    def mock_read_excel(io, sheet_name=0, **kwargs):
        if sheet_name in mock_excel_data:
            return mock_excel_data[sheet_name]
        raise ValueError(f"Sheet {sheet_name} not found")
    
    mocker.patch("pandas.read_excel", side_effect=mock_read_excel)
    
    return mock_excel

@pytest.fixture
def mock_duckdb_connection(mocker):
    """Mock DuckDB connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.execute.return_value = mock_cursor
    
    # Mock duckdb.connect
    mocker.patch("duckdb.connect", return_value=mock_conn)
    
    return mock_conn
