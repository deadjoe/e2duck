import os
import pytest
import tempfile
from e2duck.e2duck import ExcelToDuckDB

class TestInitialization:
    """Test the initialization and parameter validation of ExcelToDuckDB class."""
    
    def test_valid_parameters(self, sample_excel_path, memory_db_path):
        """Test initialization with valid parameters."""
        # Create a temporary Excel file if sample doesn't exist yet
        if not os.path.exists(sample_excel_path):
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp:
                sample_excel_path = temp.name
        
        # Initialize with valid parameters
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Verify attributes are set correctly
        assert converter.excel_path == sample_excel_path
        assert converter.db_path == memory_db_path
        # Using the module constant instead of class attribute
        from e2duck.e2duck import DEFAULT_SAMPLE_SIZE
        assert converter.sample_size == DEFAULT_SAMPLE_SIZE
        assert converter.safe_mode is True
    
    def test_invalid_excel_path_type(self, memory_db_path):
        """Test initialization with invalid excel_path type."""
        with pytest.raises(TypeError) as excinfo:
            ExcelToDuckDB(123, memory_db_path)  # Not a string
        
        assert "excel_path must be a string type" in str(excinfo.value)
    
    def test_invalid_db_path_type(self, sample_excel_path):
        """Test initialization with invalid db_path type."""
        with pytest.raises(TypeError) as excinfo:
            ExcelToDuckDB(sample_excel_path, 123)  # Not a string
        
        assert "db_path must be a string type" in str(excinfo.value)
    
    def test_invalid_sample_size_type(self, sample_excel_path, memory_db_path):
        """Test initialization with invalid sample_size type."""
        with pytest.raises(TypeError) as excinfo:
            ExcelToDuckDB(sample_excel_path, memory_db_path, sample_size="100")  # Not an integer
        
        assert "sample_size must be an integer type" in str(excinfo.value)
    
    def test_invalid_safe_mode_type(self, sample_excel_path, memory_db_path):
        """Test initialization with invalid safe_mode type."""
        with pytest.raises(TypeError) as excinfo:
            ExcelToDuckDB(sample_excel_path, memory_db_path, safe_mode="True")  # Not a boolean
        
        assert "safe_mode must be a boolean type" in str(excinfo.value)
    
    def test_nonexistent_excel_file(self, memory_db_path):
        """Test initialization with non-existent Excel file."""
        with pytest.raises(FileNotFoundError) as excinfo:
            ExcelToDuckDB("nonexistent_file.xlsx", memory_db_path)
        
        assert "Excel file does not exist" in str(excinfo.value)
    
    def test_invalid_excel_extension(self, memory_db_path):
        """Test initialization with invalid Excel file extension."""
        # Create a temporary file with wrong extension
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as temp:
            invalid_path = temp.name
        
        try:
            with pytest.raises(ValueError) as excinfo:
                ExcelToDuckDB(invalid_path, memory_db_path)
            
            assert "Unsupported Excel file format" in str(excinfo.value)
        finally:
            # Clean up
            os.unlink(invalid_path)
    
    def test_invalid_db_directory(self, sample_excel_path):
        """Test initialization with invalid database directory."""
        with pytest.raises(ValueError) as excinfo:
            ExcelToDuckDB(sample_excel_path, "/nonexistent/directory/db.duckdb")
        
        assert "Database directory does not exist" in str(excinfo.value)
    
    def test_negative_sample_size(self, sample_excel_path, memory_db_path):
        """Test initialization with negative sample size."""
        with pytest.raises(ValueError) as excinfo:
            ExcelToDuckDB(sample_excel_path, memory_db_path, sample_size=-10)
        
        assert "sample_size must be greater than 0" in str(excinfo.value)
