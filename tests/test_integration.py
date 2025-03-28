import os
import pytest
import pandas as pd
import duckdb
from e2duck.e2duck import ExcelToDuckDB

class TestIntegration:
    """Integration tests for the ExcelToDuckDB class."""
    
    def test_end_to_end_small_file(self):
        """Test the complete workflow with a small Excel file."""
        # Skip if test data doesn't exist
        excel_path = os.path.join(os.path.dirname(__file__), "../test_data/sample_small.xlsx")
        if not os.path.exists(excel_path):
            pytest.skip("Test data file not found")
        
        # Use in-memory database for testing
        db_path = ":memory:"
        
        # Initialize converter
        converter = ExcelToDuckDB(excel_path, db_path)
        
        # Run the full import process
        result = converter.run()
        
        # Verify the result
        assert result['success'] is True
        assert 'imported_sheets' in result
        assert len(result['imported_sheets']) == 2  # Two sheets in sample_small.xlsx
        
        # Verify Sheet1 was imported correctly
        assert 'Sheet1' in result['imported_sheets']
        assert result['imported_sheets']['Sheet1']['rows_imported'] == 5
        
        # Verify Sheet2 was imported correctly
        assert 'Sheet2' in result['imported_sheets']
        assert result['imported_sheets']['Sheet2']['rows_imported'] == 3
        
        # Verify data validation results
        assert 'validation_results' in result
        assert len(result['validation_results']) == 2
        
        # Connect to the database and verify the data
        conn = duckdb.connect(db_path)
        
        # Check Sheet1 data
        sheet1_data = conn.execute("SELECT * FROM Sheet1").fetchall()
        assert len(sheet1_data) == 5
        
        # Check Sheet2 data
        sheet2_data = conn.execute("SELECT * FROM Sheet2").fetchall()
        assert len(sheet2_data) == 3
        
        # Close the connection
        conn.close()
    
    def test_end_to_end_medium_file(self):
        """Test the complete workflow with a medium-sized Excel file."""
        # Skip if test data doesn't exist
        excel_path = os.path.join(os.path.dirname(__file__), "../test_data/sample_medium.xlsx")
        if not os.path.exists(excel_path):
            pytest.skip("Test data file not found")
        
        # Use in-memory database for testing
        db_path = ":memory:"
        
        # Initialize converter
        converter = ExcelToDuckDB(excel_path, db_path)
        
        # Run the full import process
        result = converter.run()
        
        # Verify the result
        assert result['success'] is True
        assert 'imported_sheets' in result
        assert len(result['imported_sheets']) == 3  # Three sheets in sample_medium.xlsx
        
        # Verify Products sheet was imported correctly
        assert 'Products' in result['imported_sheets']
        assert result['imported_sheets']['Products']['rows_imported'] == 100
        
        # Verify Orders sheet was imported correctly
        assert 'Orders' in result['imported_sheets']
        assert result['imported_sheets']['Orders']['rows_imported'] == 200
        
        # Verify Customers sheet was imported correctly
        assert 'Customers' in result['imported_sheets']
        assert result['imported_sheets']['Customers']['rows_imported'] == 50
        
        # Connect to the database and verify the data
        conn = duckdb.connect(db_path)
        
        # Check Products data
        products_data = conn.execute("SELECT * FROM Products").fetchall()
        assert len(products_data) == 100
        
        # Check Orders data
        orders_data = conn.execute("SELECT * FROM Orders").fetchall()
        assert len(orders_data) == 200
        
        # Check Customers data
        customers_data = conn.execute("SELECT * FROM Customers").fetchall()
        assert len(customers_data) == 50
        
        # Verify data types were correctly inferred
        schema_info = conn.execute("DESCRIBE Products").fetchall()
        column_types = {col[0]: col[1] for col in schema_info}
        
        # Check specific column types (if not in safe mode)
        if not converter.safe_mode:
            assert column_types['product_id'].upper() == 'INTEGER'
            assert column_types['price'].upper() in ('DOUBLE', 'FLOAT')
            assert column_types['in_stock'].upper() == 'BOOLEAN'
        
        # Close the connection
        conn.close()
    
    def test_end_to_end_complex_file(self):
        """Test the complete workflow with a complex Excel file containing edge cases."""
        # Skip if test data doesn't exist
        excel_path = os.path.join(os.path.dirname(__file__), "../test_data/sample_complex.xlsx")
        if not os.path.exists(excel_path):
            pytest.skip("Test data file not found")
        
        # Use in-memory database for testing
        db_path = ":memory:"
        
        # Initialize converter
        converter = ExcelToDuckDB(excel_path, db_path)
        
        # Run the full import process
        result = converter.run()
        
        # Verify the result
        assert result['success'] is True
        assert 'imported_sheets' in result
        
        # Verify EdgeCases sheet was imported correctly
        assert 'EdgeCases' in result['imported_sheets']
        assert result['imported_sheets']['EdgeCases']['rows_imported'] == 5
        
        # Verify MixedTypes sheet was imported correctly
        assert 'MixedTypes' in result['imported_sheets']
        assert result['imported_sheets']['MixedTypes']['rows_imported'] == 5
        
        # Verify empty sheets were handled correctly
        assert 'EmptySheet' in result['imported_sheets']
        assert result['imported_sheets']['EmptySheet']['rows_imported'] == 0
        
        assert 'HeadersOnly' in result['imported_sheets']
        assert result['imported_sheets']['HeadersOnly']['rows_imported'] == 0
        
        # Connect to the database and verify the data
        conn = duckdb.connect(db_path)
        
        # Check EdgeCases data
        edge_cases_data = conn.execute("SELECT * FROM EdgeCases").fetchall()
        assert len(edge_cases_data) == 5
        
        # Check MixedTypes data
        mixed_types_data = conn.execute("SELECT * FROM MixedTypes").fetchall()
        assert len(mixed_types_data) == 5
        
        # Verify empty tables were created correctly
        empty_sheet_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='EmptySheet'"
        ).fetchone()
        assert empty_sheet_exists is not None
        
        headers_only_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='HeadersOnly'"
        ).fetchone()
        assert headers_only_exists is not None
        
        # Close the connection
        conn.close()
    
    def test_safe_mode_vs_normal_mode(self):
        """Compare safe mode and normal mode with the same input file."""
        # Skip if test data doesn't exist
        excel_path = os.path.join(os.path.dirname(__file__), "../test_data/sample_small.xlsx")
        if not os.path.exists(excel_path):
            pytest.skip("Test data file not found")
        
        # Test with safe_mode=True
        db_path_safe = ":memory:"
        converter_safe = ExcelToDuckDB(excel_path, db_path_safe, safe_mode=True)
        result_safe = converter_safe.run()
        
        # Test with safe_mode=False
        db_path_normal = ":memory:"
        converter_normal = ExcelToDuckDB(excel_path, db_path_normal, safe_mode=False)
        result_normal = converter_normal.run()
        
        # Both should succeed
        assert result_safe['success'] is True
        assert result_normal['success'] is True
        
        # Connect to both databases and compare schemas
        conn_safe = duckdb.connect(db_path_safe)
        conn_normal = duckdb.connect(db_path_normal)
        
        # Get schema for Sheet1 in safe mode
        schema_safe = conn_safe.execute("DESCRIBE Sheet1").fetchall()
        column_types_safe = {col[0]: col[1] for col in schema_safe}
        
        # Get schema for Sheet1 in normal mode
        schema_normal = conn_normal.execute("DESCRIBE Sheet1").fetchall()
        column_types_normal = {col[0]: col[1] for col in schema_normal}
        
        # In safe mode, all columns should be TEXT
        for col, type_name in column_types_safe.items():
            assert type_name.upper() == 'TEXT', f"Column {col} in safe mode should be TEXT"
        
        # In normal mode, types should be inferred
        if 'price' in column_types_normal:
            assert column_types_normal['price'].upper() in ('DOUBLE', 'FLOAT'), "Price should be numeric in normal mode"
        
        # Close connections
        conn_safe.close()
        conn_normal.close()
