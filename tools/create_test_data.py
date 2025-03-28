"""
Create test data files for e2duck testing.
This script generates various Excel files for testing different scenarios.
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import shutil
import xlwt

# Define test data directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XLSX_DIR = os.path.join(BASE_DIR, 'tests', 'data', 'input', 'xlsx')
XLS_DIR = os.path.join(BASE_DIR, 'tests', 'data', 'input', 'xls')

# Ensure test data directories exist
os.makedirs(XLSX_DIR, exist_ok=True)
os.makedirs(XLS_DIR, exist_ok=True)

# 1. Create small sample file
def create_small_sample():
    print("Creating small XLSX sample file...")
    
    # Sheet 1: Basic data types
    df1 = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
        'price': [10.5, 20.75, 30.0, 40.25, 50.5],
        'date': pd.date_range(start='2023-01-01', periods=5)
    })
    
    # Sheet 2: Another simple table
    df2 = pd.DataFrame({
        'category_id': [101, 102, 103],
        'category_name': ['Electronics', 'Clothing', 'Food'],
        'active': [True, False, True]
    })
    
    # Write to Excel file
    xlsx_path = os.path.join(XLSX_DIR, 'sample_small.xlsx')
    with pd.ExcelWriter(xlsx_path) as writer:
        df1.to_excel(writer, sheet_name='Sheet1', index=False)
        df2.to_excel(writer, sheet_name='Sheet2', index=False)
    
    print(f"Created {xlsx_path} (contains 2 worksheets)")

# 2. Create medium sample file
def create_medium_sample():
    print("Creating medium XLSX sample file...")
    
    # Sheet 1: Products (100 rows)
    np.random.seed(42)  # For reproducibility
    products = pd.DataFrame({
        'product_id': range(1, 101),
        'product_name': [f'Product {i}' for i in range(1, 101)],
        'category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Books', 'Home'], 100),
        'price': np.random.uniform(10, 1000, 100).round(2),
        'in_stock': np.random.choice([True, False], 100, p=[0.8, 0.2]),
        'created_date': [datetime(2023, 1, 1) + timedelta(days=i) for i in range(100)]
    })
    
    # Sheet 2: Orders (200 rows)
    orders = pd.DataFrame({
        'order_id': range(1, 201),
        'customer_id': np.random.randint(1, 51, 200),
        'order_date': [datetime(2023, 1, 1) + timedelta(days=np.random.randint(0, 365)) for _ in range(200)],
        'total_amount': np.random.uniform(50, 5000, 200).round(2),
        'status': np.random.choice(['Pending', 'Shipped', 'Delivered', 'Cancelled'], 200),
        'payment_method': np.random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Cash'], 200)
    })
    
    # Sheet 3: Customers (50 rows)
    customers = pd.DataFrame({
        'customer_id': range(1, 51),
        'name': [f'Customer {i}' for i in range(1, 51)],
        'email': [f'customer{i}@example.com' for i in range(1, 51)],
        'registration_date': [datetime(2022, 1, 1) + timedelta(days=i*7) for i in range(50)],
        'vip': np.random.choice([True, False], 50, p=[0.2, 0.8])
    })
    
    # Write to Excel file
    xlsx_path = os.path.join(XLSX_DIR, 'sample_medium.xlsx')
    with pd.ExcelWriter(xlsx_path) as writer:
        products.to_excel(writer, sheet_name='Products', index=False)
        orders.to_excel(writer, sheet_name='Orders', index=False)
        customers.to_excel(writer, sheet_name='Customers', index=False)
    
    print(f"Created {xlsx_path} (contains 3 worksheets)")

# 3. Create complex sample file
def create_complex_sample():
    print("Creating complex XLSX sample file...")
    
    # Sheet 1: Edge cases and special values
    edge_cases = pd.DataFrame({
        'nulls': [None, np.nan, None, 'Not Null', None],
        'extreme_ints': [0, -2147483648, 2147483647, 9223372036854775807, -9223372036854775808],
        'extreme_floats': [0.0, np.finfo(np.float64).min, np.finfo(np.float64).max, np.inf, -np.inf],
        'special_strings': ['', 'Normal text', 'Text with \n newline', 'Text with "quotes"', 'Text with \'apostrophes\''],
        'special_chars': ['!@#$%^&*()', 'áéíóú', '你好世界', '😀🚀🌍', '\\path\\to\\file']
    })
    
    # Sheet 2: Mixed data types
    mixed_types = pd.DataFrame({
        'mixed_column1': [1, 'string', 3.14, True, None],
        'mixed_column2': ['2023-01-01', 123, 'text', datetime(2023, 1, 1), False],
        'mixed_column3': [None, None, None, 'Not Null', None]
    })
    
    # Sheet 3: Empty table
    empty_sheet = pd.DataFrame()
    
    # Sheet 4: Headers only, no data
    headers_only = pd.DataFrame(columns=['col1', 'col2', 'col3'])
    
    # Write to Excel file
    xlsx_path = os.path.join(XLSX_DIR, 'sample_complex.xlsx')
    with pd.ExcelWriter(xlsx_path) as writer:
        edge_cases.to_excel(writer, sheet_name='EdgeCases', index=False)
        mixed_types.to_excel(writer, sheet_name='MixedTypes', index=False)
        empty_sheet.to_excel(writer, sheet_name='EmptySheet', index=False)
        headers_only.to_excel(writer, sheet_name='HeadersOnly', index=False)
    
    print(f"Created {xlsx_path} (contains 4 worksheets)")

# 4. Create XLS format sample files
def create_xls_samples():
    """Create .xls format test files"""
    print("Creating .xls format test files...")
    
    # Use xlwt to directly create real .xls files
    import xlwt
    
    # 1. Small XLS file
    print("Creating small XLS sample file...")
    wb1 = xlwt.Workbook()
    
    # Sheet1
    ws1 = wb1.add_sheet('Sheet1')
    # Add headers
    headers1 = ['id', 'name', 'price']
    for col, header in enumerate(headers1):
        ws1.write(0, col, header)
    
    # Add data
    data1 = [
        [1, 'Product A', 10.5],
        [2, 'Product B', 20.75],
        [3, 'Product C', 30.0],
        [4, 'Product D', 40.25],
        [5, 'Product E', 50.5]
    ]
    for row_idx, row_data in enumerate(data1, 1):
        for col_idx, cell_value in enumerate(row_data):
            ws1.write(row_idx, col_idx, cell_value)
    
    # Sheet2
    ws2 = wb1.add_sheet('Sheet2')
    # Add headers
    headers2 = ['category_id', 'category_name', 'active']
    for col, header in enumerate(headers2):
        ws2.write(0, col, header)
    
    # Add data
    data2 = [
        [101, 'Electronics', 1],
        [102, 'Clothing', 0],
        [103, 'Food', 1]
    ]
    for row_idx, row_data in enumerate(data2, 1):
        for col_idx, cell_value in enumerate(row_data):
            ws2.write(row_idx, col_idx, cell_value)
    
    # Save file
    xls_path = os.path.join(XLS_DIR, 'sample_small.xls')
    wb1.save(xls_path)
    print(f"Created {xls_path} (real .xls format)")
    
    # 2. Medium XLS file
    print("Creating medium XLS sample file...")
    wb2 = xlwt.Workbook()
    
    # Products table
    ws_products = wb2.add_sheet('Products')
    # Add headers
    product_headers = ['product_id', 'product_name', 'category', 'price', 'in_stock']
    for col, header in enumerate(product_headers):
        ws_products.write(0, col, header)
    
    # Add data (only add 50 rows because xls has row limitations)
    np.random.seed(42)
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Home']
    for row in range(1, 51):
        ws_products.write(row, 0, row)  # product_id
        ws_products.write(row, 1, f'Product {row}')  # product_name
        ws_products.write(row, 2, categories[row % 5])  # category
        ws_products.write(row, 3, round(np.random.uniform(10, 1000), 2))  # price
        ws_products.write(row, 4, 1 if np.random.random() > 0.2 else 0)  # in_stock
    
    # Orders table
    ws_orders = wb2.add_sheet('Orders')
    # Add headers
    order_headers = ['order_id', 'customer_id', 'total_amount']
    for col, header in enumerate(order_headers):
        ws_orders.write(0, col, header)
    
    # Add data (only add 100 rows)
    for row in range(1, 101):
        ws_orders.write(row, 0, row)  # order_id
        ws_orders.write(row, 1, np.random.randint(1, 26))  # customer_id
        ws_orders.write(row, 2, round(np.random.uniform(50, 5000), 2))  # total_amount
    
    # Save file
    xls_path = os.path.join(XLS_DIR, 'sample_medium.xls')
    wb2.save(xls_path)
    print(f"Created {xls_path} (real .xls format)")
    
    # 3. Complex XLS file
    print("Creating complex XLS sample file...")
    wb3 = xlwt.Workbook()
    
    # EdgeCases table
    ws_edge = wb3.add_sheet('EdgeCases')
    # Add headers
    edge_headers = ['nulls', 'extreme_ints', 'special_strings']
    for col, header in enumerate(edge_headers):
        ws_edge.write(0, col, header)
    
    # Add data
    edge_data = [
        ['', 0, ''],  # Empty string
        ['', -32768, 'Normal text'],  # Smallest integer supported by xlwt
        ['', 32767, 'Text with quotes'],  # Largest integer supported by xlwt
        ['Not Null', 10000, 'Special chars'],
        ['', -10000, '!@#$%^&*()']
    ]
    for row_idx, row_data in enumerate(edge_data, 1):
        for col_idx, cell_value in enumerate(row_data):
            ws_edge.write(row_idx, col_idx, cell_value)
    
    # MixedTypes table
    ws_mixed = wb3.add_sheet('MixedTypes')
    # Add headers
    mixed_headers = ['mixed_column1', 'mixed_column2']
    for col, header in enumerate(mixed_headers):
        ws_mixed.write(0, col, header)
    
    # Add data
    mixed_data = [
        [1, '2023-01-01'],
        ['string', 123],
        [3.14, 'text'],
        [1, '2023-01-01'],  # xlwt doesn't support boolean values, use 1 instead of True
        ['', 0]  # xlwt doesn't support None, use empty string instead
    ]
    for row_idx, row_data in enumerate(mixed_data, 1):
        for col_idx, cell_value in enumerate(row_data):
            ws_mixed.write(row_idx, col_idx, cell_value)
    
    # Empty table
    wb3.add_sheet('EmptySheet')
    
    # Save file
    xls_path = os.path.join(XLS_DIR, 'sample_complex.xls')
    wb3.save(xls_path)
    print(f"Created {xls_path} (real .xls format)")

if __name__ == "__main__":
    create_small_sample()
    create_medium_sample()
    create_complex_sample()
    create_xls_samples()  # Add this line
    print("All test data files created successfully!")