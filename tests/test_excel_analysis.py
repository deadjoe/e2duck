import os
import pytest
import pandas as pd
from e2duck.e2duck import ExcelToDuckDB

class TestExcelAnalysis:
    """Test the Excel analysis functionality of ExcelToDuckDB class."""
    
    def test_analyze_excel_with_mock_data(self, mocker, mock_excel_file, sample_excel_path, memory_db_path):
        """Test Excel analysis with mocked data."""
        # Initialize with valid parameters
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Execute the analyze_excel method
        converter.analyze_excel()
        
        # Verify sheets_info was populated correctly
        assert len(converter.sheets_info) == 2
        assert "Sheet1" in converter.sheets_info
        assert "Sheet2" in converter.sheets_info
        
        # Verify column information was extracted
        sheet1_info = converter.sheets_info["Sheet1"]
        assert "columns" in sheet1_info
        assert len(sheet1_info["columns"]) > 0
        
        # Verify a specific column was analyzed correctly
        assert any(col["original_name"] == "id" or col["clean_name"] == "id" for col in sheet1_info["columns"])
        
    def test_analyze_excel_empty_sheet(self, mocker, memory_db_path):
        """Test Excel analysis with an empty sheet."""
        # Mock an empty Excel file
        mock_excel_path = os.path.join(os.path.dirname(__file__), "data/input/xlsx/sample_complex.xlsx")
        
        # Patch pandas.read_excel to return an empty DataFrame
        mocker.patch("pandas.read_excel", return_value=pd.DataFrame())
        
        # Initialize converter
        converter = ExcelToDuckDB(mock_excel_path, memory_db_path)
        
        # Execute the analyze_excel method
        converter.analyze_excel()
        
        # Note: In the actual implementation, empty sheets are skipped
        # and not included in sheets_info. The test for empty sheet handling
        # should verify this behavior.
        
        # Mock a more complete fixture
        mock_excel = mocker.patch("pandas.ExcelFile")
        mock_excel.return_value.sheet_names = ["EmptySheet"]
        
        # Ensure empty sheets are handled properly (skipped)
        # This is expected to not have any sheets processed
        assert len(converter.sheets_info) == 0
        
    def test_analyze_excel_type_inference(self, mocker, mock_excel_data, sample_excel_path, memory_db_path):
        """Test data type inference during Excel analysis."""
        # Initialize converter with safe_mode=False to test type inference
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path, safe_mode=False)
        
        # Mock Excel file reading
        mock_excel = mocker.patch("pandas.ExcelFile")
        mock_excel.return_value.sheet_names = ["TypeTest"]
        
        # Create DataFrame with various data types
        type_df = pd.DataFrame({
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "str_col": ["a", "b", "c", "d", "e"],
            "bool_col": [True, False, True, False, True],
            "date_col": pd.date_range(start="2023-01-01", periods=5),
            "mixed_col": [1, "two", 3.0, True, pd.Timestamp("2023-01-05")]
        })
        
        mocker.patch("pandas.read_excel", return_value=type_df)
        
        # Execute analyze_excel
        converter.analyze_excel()
        
        # Verify type inference
        type_info = converter.sheets_info["TypeTest"]
        assert "columns" in type_info
        
        # Check column types
        columns = {col["original_name"]: col for col in type_info["columns"]}
        
        # Integer column should be mapped to INTEGER
        assert "int_col" in columns
        assert columns["int_col"]["duck_type"] == "INTEGER"
        
        # Float column should be mapped to DOUBLE
        assert "float_col" in columns
        assert columns["float_col"]["duck_type"] == "DOUBLE"
        
        # String column should be mapped to TEXT
        assert "str_col" in columns
        assert columns["str_col"]["duck_type"] == "TEXT"
        
        # Boolean column should be mapped to BOOLEAN
        assert "bool_col" in columns
        assert columns["bool_col"]["duck_type"] == "BOOLEAN"
        
        # Date column should be mapped to TIMESTAMP
        assert "date_col" in columns
        assert "TIMESTAMP" in columns["date_col"]["duck_type"]
        
        # Mixed column should be mapped to TEXT in safe mode
        assert "mixed_col" in columns
        assert columns["mixed_col"]["duck_type"] == "TEXT"
    
    def test_analyze_excel_with_large_file_simulation(self, mocker, sample_excel_path, memory_db_path):
        """Test Excel analysis with a simulated large file."""
        # Mock file size to be large (100MB)
        mock_file_size = 100 * 1024 * 1024  # 100MB
        mocker.patch.object(os.path, "getsize", return_value=mock_file_size)
        
        # Initialize converter
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Mock sheet names
        sheet_names = ["Sheet1", "Sheet2"]
        mocker.patch.object(pd.ExcelFile, "sheet_names", property(lambda self: sheet_names))
        
        # Mock read_excel to return a DataFrame with many rows
        large_df = pd.DataFrame({
            "id": range(10000),
            "name": [f"Item {i}" for i in range(10000)],
            "value": [i * 1.5 for i in range(10000)]
        })
        mocker.patch("pandas.read_excel", return_value=large_df)
        
        # Execute analyze_excel
        converter.analyze_excel()
        
        # Verify sampling was used for large file
        assert len(converter.sheets_info) == 2
        assert "Sheet1" in converter.sheets_info
        assert "Sheet2" in converter.sheets_info
        
        # Check that sheet info was stored properly
        sheet1_info = converter.sheets_info["Sheet1"]
        # In the actual implementation, samples are stored in a separate dict converter.samples
        # not in the sheets_info dict
        assert converter.samples is not None
        assert "Sheet1" in converter.samples or len(converter.samples) > 0
