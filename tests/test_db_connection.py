import pytest
import duckdb
from unittest.mock import MagicMock, patch
from e2duck.e2duck import ExcelToDuckDB

class TestDBConnection:
    """Test the database connection functionality of ExcelToDuckDB class."""
    
    def test_connect_db_success(self, mocker, sample_excel_path, memory_db_path):
        """Test successful database connection."""
        # Create mock and patch before test
        mock_connect = mocker.patch('duckdb.connect')
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Initialize
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Execute the connection
        result = converter.connect_db()
        
        # Verify 
        assert result is True
        assert converter.is_connected is True
        assert converter.conn is mock_conn
        assert mock_connect.call_count == 1
        
        # Verify pragmas were set
        assert mock_conn.execute.call_count >= 3  # Verify at least 3 pragmas were set
        
        # Check for specific pragma calls
        calls = [args[0][0] for args in mock_conn.execute.call_args_list]
        assert any('PRAGMA threads=' in call for call in calls)
        assert any('PRAGMA memory_limit=' in call for call in calls)
        assert any('PRAGMA enable_progress_bar=true' in call for call in calls)
    
    def test_connect_db_failure(self, mocker, sample_excel_path, memory_db_path):
        """Test failed database connection."""
        # Mock duckdb.connect to raise an exception
        mocker.patch('duckdb.connect', side_effect=Exception("Connection failed"))
        
        # Initialize
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Execute the connection
        result = converter.connect_db()
        
        # Verify
        assert result is False
        assert converter.is_connected is False
        assert converter.conn is None
    
    def test_close_connection(self, mocker, sample_excel_path, memory_db_path):
        """Test closing database connection."""
        # Create mock
        mock_conn = MagicMock()
        
        # Initialize
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        converter.conn = mock_conn
        
        # Execute close
        converter.close()
        
        # Verify mock was called
        assert mock_conn.close.call_count == 1
    
    def test_close_no_connection(self, sample_excel_path, memory_db_path):
        """Test closing when no connection exists."""
        # Initialize
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        converter.conn = None
        
        # This should not raise any exception
        converter.close()
    
    @patch('psutil.virtual_memory')
    def test_memory_limit_large(self, mock_vm, mocker, sample_excel_path, memory_db_path):
        """Test memory limit for large memory machines."""
        # Mock 32GB memory
        mock_vm.return_value.total = 32 * 1024**3
        
        # Mock connection
        mock_conn = MagicMock()
        mocker.patch('duckdb.connect', return_value=mock_conn)
        
        # Initialize and connect
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        converter.connect_db()
        
        # Check for memory limit pragma
        calls = [args[0][0] for args in mock_conn.execute.call_args_list]
        assert any("PRAGMA memory_limit='16GB'" in call for call in calls)
    
    @patch('psutil.virtual_memory')
    def test_memory_limit_medium(self, mock_vm, mocker, sample_excel_path, memory_db_path):
        """Test memory limit for medium memory machines."""
        # Mock 16GB memory
        mock_vm.return_value.total = 16 * 1024**3
        
        # Mock connection
        mock_conn = MagicMock()
        mocker.patch('duckdb.connect', return_value=mock_conn)
        
        # Initialize and connect
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        converter.connect_db()
        
        # Check for memory limit pragma
        calls = [args[0][0] for args in mock_conn.execute.call_args_list]
        assert any("PRAGMA memory_limit='8GB'" in call for call in calls)
    
    @patch('psutil.virtual_memory')
    def test_memory_limit_small(self, mock_vm, mocker, sample_excel_path, memory_db_path):
        """Test memory limit for small memory machines."""
        # Mock 4GB memory
        mock_vm.return_value.total = 4 * 1024**3
        
        # Mock connection
        mock_conn = MagicMock()
        mocker.patch('duckdb.connect', return_value=mock_conn)
        
        # Initialize and connect
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        converter.connect_db()
        
        # Check for memory limit pragma
        calls = [args[0][0] for args in mock_conn.execute.call_args_list]
        assert any("PRAGMA memory_limit='4GB'" in call for call in calls)
    
    @patch('psutil.virtual_memory')
    def test_memory_detection_exception(self, mock_vm, mocker, sample_excel_path, memory_db_path):
        """Test handling of exception during memory detection."""
        # Mock an exception during memory detection
        mock_vm.side_effect = Exception("Memory detection failed")
        
        # Mock connection
        mock_conn = MagicMock()
        mocker.patch('duckdb.connect', return_value=mock_conn)
        
        # Initialize and connect
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        converter.connect_db()
        
        # Check for default memory limit pragma
        calls = [args[0][0] for args in mock_conn.execute.call_args_list]
        default_memory_call = any("PRAGMA memory_limit='4GB'" in call for call in calls)
        assert default_memory_call, "Should use default memory limit when detection fails"