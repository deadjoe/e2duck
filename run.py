#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Excel to DuckDB Converter Main Run Script
Usage: python run.py [excel_path] [db_path] [--sample-size SIZE] [--no-safe-mode]
"""

import sys
import argparse
from e2duck.e2duck import ExcelToDuckDB, DEFAULT_SAMPLE_SIZE

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Convert Excel files to DuckDB database')
    parser.add_argument('excel_path', help='Path to Excel file')
    parser.add_argument('db_path', help='Path to DuckDB database')
    parser.add_argument('--sample-size', type=int, default=DEFAULT_SAMPLE_SIZE,
                        help=f'Sample size for analysis (default: {DEFAULT_SAMPLE_SIZE})')
    parser.add_argument('--no-safe-mode', action='store_true',
                        help='Disable safe mode (default: enabled)')
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_arguments()
    
    try:
        # Create converter instance
        converter = ExcelToDuckDB(
            excel_path=args.excel_path,
            db_path=args.db_path,
            sample_size=args.sample_size,
            safe_mode=not args.no_safe_mode
        )
        
        # Execute conversion process using the provided run method
        result = converter.run()
        
        if not result['success']:
            error_msg = result.get('error', 'Unknown error')
            print(f"Error: {error_msg}", file=sys.stderr)
            return 1
        
        print(f"Conversion complete! Data saved to: {args.db_path}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())