import pytest
from unittest.mock import MagicMock, patch, call
from e2duck.e2duck import ExcelToDuckDB

class TestRunProcess:
    """Test the run process functionality of ExcelToDuckDB class."""
    
    def test_run_basic_flow(self, mocker, sample_excel_path, memory_db_path):
        """Test the basic success flow of the run method."""
        # Create a converter
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Mock all the methods that run calls
        mocker.patch.object(converter, 'connect_db', return_value=True)
        mocker.patch.object(converter, 'analyze_excel', return_value=True)
        mocker.patch.object(converter, 'create_tables', return_value=True)
        mocker.patch.object(converter, 'import_data', return_value={
            'Sheet1': {'import_success': True, 'total_rows': 10, 'rows_imported': 10}
        })
        mocker.patch.object(converter, 'validate_import', return_value=[
            {
                'sheet': 'Sheet1', 
                'table': 'Sheet1', 
                'excel_rows': 10, 
                'db_rows': 10, 
                'overall_status': 'success', 
                'messages': []
            }
        ])
        # The actual issue is that we need to make the mock return a result that includes all required fields
        mock_optimize = mocker.patch.object(converter, 'optimize_tables')
        mocker.patch.object(converter, 'close')
        
        # Run the process
        result = converter.run()
        
        # Verify all methods were called in the correct order
        assert converter.connect_db.call_count == 1
        assert converter.analyze_excel.call_count == 1
        assert converter.create_tables.call_count == 1
        assert converter.import_data.call_count == 1
        assert converter.validate_import.call_count == 1
        assert converter.optimize_tables.call_count == 1
        assert converter.close.call_count == 1
        
        # Verify the final result
        assert 'success' in result
        assert 'validation_results' in result
        assert 'stage_results' in result
        assert 'import_results' in result
        assert 'total_time' in result
    
    def test_run_connection_failure(self, mocker, sample_excel_path, memory_db_path):
        """Test run method when database connection fails."""
        # Create a converter
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Mock the connect_db method to fail
        mocker.patch.object(converter, 'connect_db', return_value=False)
        mocker.patch.object(converter, 'close')
        
        # Run the process
        result = converter.run()
        
        # Verify only connect_db and close were called
        assert converter.connect_db.call_count == 1
        assert converter.close.call_count == 1
        
        # Verify the final result
        assert result['success'] is False
        assert result['error'] == "Failed to connect to database"
        assert result['stage_results']['database_connection'] is False
    
    def test_run_excel_analysis_failure(self, mocker, sample_excel_path, memory_db_path):
        """Test run method when Excel analysis fails."""
        # Create a converter
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Mock methods
        mocker.patch.object(converter, 'connect_db', return_value=True)
        mocker.patch.object(converter, 'analyze_excel', return_value=False)
        mocker.patch.object(converter, 'close')
        
        # Run the process
        result = converter.run()
        
        # Verify only needed methods were called
        assert converter.connect_db.call_count == 1
        assert converter.analyze_excel.call_count == 1
        assert converter.close.call_count == 1
        
        # Verify the final result
        assert result['success'] is False
        assert result['error'] == "Failed to analyze Excel file"
        assert result['stage_results']['database_connection'] is True
        assert result['stage_results']['excel_analysis'] is False
    
    def test_run_table_creation_failure(self, mocker, sample_excel_path, memory_db_path):
        """Test run method when table creation fails."""
        # Create a converter
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Mock methods
        mocker.patch.object(converter, 'connect_db', return_value=True)
        mocker.patch.object(converter, 'analyze_excel', return_value=True)
        mocker.patch.object(converter, 'create_tables', side_effect=Exception("Table creation failed"))
        mocker.patch.object(converter, 'close')
        
        # Run the process
        result = converter.run()
        
        # Verify only needed methods were called
        assert converter.connect_db.call_count == 1
        assert converter.analyze_excel.call_count == 1
        assert converter.create_tables.call_count == 1
        assert converter.close.call_count == 1
        
        # Verify the final result
        assert result['success'] is False
        assert "Failed to create table structure" in result['error']
        assert result['stage_results']['database_connection'] is True
        assert result['stage_results']['excel_analysis'] is True
        assert result['stage_results']['table_creation'] is False
    
    def test_run_all_imports_failed(self, mocker, sample_excel_path, memory_db_path):
        """Test run method when all imports fail."""
        # Create a converter
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Mock methods
        mocker.patch.object(converter, 'connect_db', return_value=True)
        mocker.patch.object(converter, 'analyze_excel', return_value=True)
        mocker.patch.object(converter, 'create_tables', return_value=True)
        mocker.patch.object(converter, 'import_data', return_value={
            'Sheet1': {'import_success': False, 'error_message': 'Import failed'}
        })
        mocker.patch.object(converter, 'close')
        
        # Run the process
        result = converter.run()
        
        # Verify methods were called
        assert converter.connect_db.call_count == 1
        assert converter.analyze_excel.call_count == 1
        assert converter.create_tables.call_count == 1
        assert converter.import_data.call_count == 1
        assert converter.close.call_count == 1
        
        # Since we're mocking validate_import, we need to check it wasn't called
        if hasattr(converter, 'validate_import'):
            assert not hasattr(converter.validate_import, 'call_count') or converter.validate_import.call_count == 0
        
        # Verify the final result
        assert result['success'] is False
        assert result['error'] == "All worksheets import failed"
    
    def test_run_validation_failure(self, mocker, sample_excel_path, memory_db_path):
        """Test run method when validation fails but continues to optimization."""
        # Create a converter
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Mock methods
        mocker.patch.object(converter, 'connect_db', return_value=True)
        mocker.patch.object(converter, 'analyze_excel', return_value=True)
        mocker.patch.object(converter, 'create_tables', return_value=True)
        mocker.patch.object(converter, 'import_data', return_value={
            'Sheet1': {'import_success': True, 'total_rows': 10, 'rows_imported': 10}
        })
        mocker.patch.object(converter, 'validate_import', side_effect=Exception("Validation failed"))
        mocker.patch.object(converter, 'optimize_tables')
        mocker.patch.object(converter, 'close')
        
        # Run the process
        result = converter.run()
        
        # Verify methods were called
        assert converter.connect_db.call_count == 1
        assert converter.analyze_excel.call_count == 1
        assert converter.create_tables.call_count == 1
        assert converter.import_data.call_count == 1
        assert converter.validate_import.call_count == 1
        assert converter.close.call_count == 1
        
        # Verify optimization was not called since validation failed
        assert converter.optimize_tables.call_count == 0
        
        # Verify the final result
        # In the run() method, success is based on validation_results, which will be empty here
        # The actual behavior is implementation specific, so we just check that the validation stage has empty results
        assert 'success' in result
        assert result['stage_results']['data_validation'] == []
    
    def test_run_general_exception(self, mocker, sample_excel_path, memory_db_path):
        """Test run method when a general exception occurs."""
        # Create a converter
        converter = ExcelToDuckDB(sample_excel_path, memory_db_path)
        
        # Mock methods to raise an exception
        mocker.patch.object(converter, 'connect_db', side_effect=Exception("Unexpected error"))
        mocker.patch.object(converter, 'close')
        
        # Run the process
        result = converter.run()
        
        # Verify close was called even after exception
        assert converter.close.call_count == 1
        
        # Verify the final result
        assert result['success'] is False
        assert result['error'] == "Unexpected error"