import pytest
import pandas as pd
import hashlib
from e2duck.e2duck import ExcelToDuckDB

class TestUtilFunctions:
    """Test the utility functions of the ExcelToDuckDB class."""
    
    def test_clean_column_name(self, sample_excel_path, memory_db_path):
        """Test _clean_column_name function."""
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Test basic cleaning
        assert converter._clean_column_name("Column Name") == "Column_Name"
        
        # Test with special characters
        assert converter._clean_column_name("Column-Name!") == "Column_Name"
        
        # Test with numeric start
        assert converter._clean_column_name("123Column") == "col_123Column"
        
        # Test empty name
        assert converter._clean_column_name("") == "column"
        
        # Test non-string (it actually converts to string first and then applies rules)
        assert converter._clean_column_name(123) == "col_123"
    
    def test_compute_sample_hash(self, sample_excel_path, memory_db_path):
        """Test _compute_sample_hash function."""
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Create a simple DataFrame
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        
        # Calculate expected hash
        expected_hash = hashlib.md5(df.to_csv(index=False).encode()).hexdigest()
        
        # Compute hash with the function
        actual_hash = converter._compute_sample_hash(df)
        
        # Verify they match
        assert actual_hash == expected_hash
        
        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        empty_hash = converter._compute_sample_hash(empty_df)
        assert isinstance(empty_hash, str)
    
    def test_validate_parameter(self, sample_excel_path, memory_db_path):
        """Test _validate_parameter function."""
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Test with valid integer
        result = converter._validate_parameter("test_param", 10, int, min_value=1, max_value=100)
        assert result == 10
        
        # Test with valid string
        result = converter._validate_parameter("test_param", "test", str)
        assert result == "test"
        
        # Test with None value and default
        result = converter._validate_parameter("test_param", None, int, default_value=5)
        assert result == 5
        
        # Test type conversion
        result = converter._validate_parameter("test_param", "10", int, default_value=5)
        assert result == 10
        assert isinstance(result, int)
        
        # Test value below minimum with default
        result = converter._validate_parameter("test_param", 5, int, min_value=10, default_value=10)
        assert result == 10
        
        # Test value above maximum with default
        result = converter._validate_parameter("test_param", 20, int, max_value=10, default_value=10)
        assert result == 10
        
        # Test value below minimum with no default (should raise ValueError)
        with pytest.raises(ValueError):
            converter._validate_parameter("test_param", 5, int, min_value=10)
            
        # Test value above maximum with no default (should raise ValueError)
        with pytest.raises(ValueError):
            converter._validate_parameter("test_param", 20, int, max_value=10)
            
        # Test invalid type with no default (should raise ValueError)
        with pytest.raises(ValueError):
            converter._validate_parameter("test_param", "not_an_int", int)
    
    def test_build_excel_params(self, sample_excel_path, memory_db_path):
        """Test _build_excel_params function."""
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Test with basic sheet name
        params = converter._build_excel_params("Sheet1")
        assert params == {"sheet_name": "Sheet1"}
        
        # Test with engine
        params = converter._build_excel_params("Sheet1", "openpyxl")
        assert params == {"sheet_name": "Sheet1", "engine": "openpyxl"}
        
        # Test with additional parameters
        params = converter._build_excel_params(
            "Sheet1", 
            excel_engine="openpyxl", 
            skiprows=5, 
            nrows=10,
            usecols="A:C"
        )
        assert params == {
            "sheet_name": "Sheet1", 
            "engine": "openpyxl",
            "skiprows": 5,
            "nrows": 10,
            "usecols": "A:C"
        }
        
        # Test with None values (should be excluded)
        params = converter._build_excel_params(
            "Sheet1",
            excel_engine=None,
            skiprows=None,
            nrows=10
        )
        assert params == {"sheet_name": "Sheet1", "nrows": 10}
    
    def test_map_dtype_to_duck(self, sample_excel_path, memory_db_path):
        """Test _map_dtype_to_duck function."""
        # Test with safe mode on (should always return TEXT)
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path, safe_mode=True)
        
        # Create sample series
        int_series = pd.Series([1, 2, 3])
        float_series = pd.Series([1.0, 2.0, 3.0])
        str_series = pd.Series(['a', 'b', 'c'])
        bool_series = pd.Series([True, False, True])
        
        # In safe mode, all should map to TEXT
        assert converter._map_dtype_to_duck('int64', int_series) == 'TEXT'
        assert converter._map_dtype_to_duck('float64', float_series) == 'TEXT'
        assert converter._map_dtype_to_duck('object', str_series) == 'TEXT'
        assert converter._map_dtype_to_duck('bool', bool_series) == 'TEXT'
        
        # Test with safe mode off
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path, safe_mode=False)
        
        # Test integer mapping
        assert converter._map_dtype_to_duck('int64', int_series) == 'INTEGER'
        
        # Test float mapping
        assert converter._map_dtype_to_duck('float64', float_series) == 'DOUBLE'
        
        # Test string mapping
        assert converter._map_dtype_to_duck('object', str_series) == 'TEXT'
        
        # Test boolean mapping
        assert converter._map_dtype_to_duck('bool', bool_series) == 'BOOLEAN'
        
        # Test fallback type
        assert converter._map_dtype_to_duck('unknown_type', pd.Series()) == 'TEXT'
        
        # Test large integers (should map to BIGINT)
        large_int_series = pd.Series([2**31, 2**32])
        assert converter._map_dtype_to_duck('int64', large_int_series) == 'BIGINT'
        
        # Test strings in numeric columns (should map to TEXT for safety)
        mixed_series = pd.Series([1, 2, '3'])
        assert converter._map_dtype_to_duck('int64', mixed_series) == 'TEXT'