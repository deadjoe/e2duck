import pandas as pd
import duckdb
import os
import logging
import time
import hashlib
import random
from datetime import datetime
import sys
import traceback
import multiprocessing
import psutil  # For system memory detection
import concurrent.futures
import threading

# Log level standards
# DEBUG: Detailed development debugging information, such as variable values, intermediate calculation results, etc.
# INFO: Normal operation information, such as starting/completing tasks, configuration information, etc.
# WARNING: Potential issues or situations that need attention but do not affect main functionality
# ERROR: Errors that cause functionality failure, such as files that cannot be opened, data processing failures, etc.
# CRITICAL: Serious errors that prevent the program from continuing to run

# Set up logging
logging.basicConfig(
    filename=f'excel_to_duckdb_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define constants
DEFAULT_SAMPLE_SIZE = 100  # Default sample size
DEFAULT_BATCH_SIZE = 5000  # Default batch size
DEFAULT_MEMORY_LIMIT = '4GB'  # Default memory limit
MAX_SAMPLE_ROWS = 1000  # Maximum sample rows
MAX_UNIQUE_VALUES = 30  # Maximum unique values
RESERVOIR_BATCH_SIZE = 1000  # Reservoir sampling batch size

class ExcelToDuckDB:
    def __init__(self, excel_path, db_path, sample_size=DEFAULT_SAMPLE_SIZE, safe_mode=True):
        """
        Initialize Excel to DuckDB converter
        
        Parameters:
            excel_path (str): Path to the Excel file
            db_path (str): Path to the DuckDB database file
            sample_size (int): Sample size for validation, default is 100
            safe_mode (bool): Whether to use safe mode (all columns use TEXT type), default is True
            
        Exceptions:
            ValueError: If parameters are invalid
            TypeError: If parameter types are incorrect
        """
        # Validate parameter type
        if not isinstance(excel_path, str):
            raise TypeError("excel_path must be a string type")
        if not isinstance(db_path, str):
            raise TypeError("db_path must be a string type")
        if not isinstance(sample_size, int):
            raise TypeError("sample_size must be an integer type")
        if not isinstance(safe_mode, bool):
            raise TypeError("safe_mode must be a boolean type")
            
        # Validate parameter values
        if not excel_path:
            raise ValueError("excel_path cannot be empty")
        if not db_path:
            raise ValueError("db_path cannot be empty")
        if sample_size <= 0:
            raise ValueError("sample_size must be greater than 0")
            
        # Validate Excel file path
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file does not exist: {excel_path}")
        
        # Validate Excel file extension
        _, ext = os.path.splitext(excel_path)
        if ext.lower() not in ['.xlsx', '.xls', '.xlsm']:
            raise ValueError(f"Unsupported Excel file format: {ext}. Please use .xlsx, .xls, or .xlsm format")
            
        # Validate database path
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            raise ValueError(f"Database directory does not exist: {db_dir}")
            
        self.excel_path = excel_path
        self.db_path = db_path
        self.conn = None
        self.sheets_info = {}
        self.sample_size = sample_size
        self.samples = {}
        self.safe_mode = safe_mode  # safe mode - default enabled
        self.is_connected = False

    def connect_db(self):
        """
        Connect to DuckDB database
        
        Returns:
            bool: Whether the connection is successful
            
        Exceptions:
            Will not throw exceptions, but returns False and logs errors
        """
        try:
            # Try to connect to existing database, create if it doesn't exist
            self.conn = duckdb.connect(self.db_path)
            logging.info(f"Successfully connected to database: {self.db_path}")

            # Get CPU core count and set appropriate thread count
            cpu_count = multiprocessing.cpu_count()
            thread_count = min(cpu_count, 16)  # Avoid excessive thread switching overhead
            self.conn.execute(f"PRAGMA threads={thread_count}")
            logging.info(f"Set DuckDB parallel processing threads to{thread_count}")

            # Adjust memory limit based on machine memory size
            try:
                total_memory = psutil.virtual_memory().total / (1024**3)  # Convert to GB
                if total_memory >= 32:
                    memory_limit = "16GB"  # For machines with 32GB or more memory
                elif total_memory >= 16:
                    memory_limit = "8GB"   # For machines with 16GB memory
                else:
                    memory_limit = "4GB"   # Default setting
                
                self.conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
                logging.info(f"Based on system memory({total_memory:.1f}GB)Set DuckDB memory limit to{memory_limit}")
            except Exception as e:
                # If memory detection fails, use default setting
                self.conn.execute(f"PRAGMA memory_limit='{DEFAULT_MEMORY_LIMIT}'")
                logging.warning(f"Memory detection failed, using default memory limit{DEFAULT_MEMORY_LIMIT}: {str(e)}")
                
            # Enable progress bar
            self.conn.execute("PRAGMA enable_progress_bar=true")
            
            self.is_connected = True
            return True

        except Exception as e:
            logging.error(f"Error connecting to database: {str(e)}")
            logging.error(traceback.format_exc())
            self.is_connected = False
            return False

    def analyze_excel(self):
        """
        Analyze Excel file structure and extract sample data for validation
        
        Returns:
            bool: Whether the analysis is successful
            
        Exceptions:
            Will not throw exceptions, but returns False and logs errors
        """
        try:
            # File check has been completed in __init__, no need to check again here
            
            # Check file size
            file_size = os.path.getsize(self.excel_path) / (1024*1024)  # Convert to MB
            logging.info(f"Excel file size: {file_size:.2f} MB")

            start_time = time.time()
            logging.info(f"Start analyzing Excel file: {self.excel_path}")

            # Using default engine to read Excel file
            excel_engine = None
            try:
                # Directly use default engine
                xl = pd.ExcelFile(self.excel_path)
                logging.info("Using default engine to read Excel")
            except Exception as e:
                logging.error(f"Failed to read Excel file: {str(e)}")
                return False

            sheet_names = xl.sheet_names
            logging.info(f"Excel file contains the following worksheets: {sheet_names}")

            if not sheet_names:
                logging.error("Excel file does not contain any worksheets")
                return False
                
            # create thread lock to protect shared resources
            samples_lock = threading.Lock()
            
            # define worksheet process function
            def process_sheet(sheet_name):
                try:
                    logging.info(f"Analyzing worksheet: {sheet_name}")
                    
                    # get row count - using multiple methods and validate
                    total_rows = None
                    
                    # Method 1: Try using openpyxl to directly get row count
                    if excel_engine == 'openpyxl':
                        try:
                            sheet = xl.book[sheet_name]
                            # find the last non-empty row
                            max_row = sheet.max_row
                            # validate if the last row actually has data
                            has_header = True  # assume there is a header row
                            for row in range(max_row, 0, -1):
                                if any(sheet.cell(row=row, column=col).value is not None 
                                      for col in range(1, sheet.max_column + 1)):
                                    if has_header:
                                        total_rows = row - 1  # subtract header row
                                    else:
                                        total_rows = row
                                    break
                            logging.info(f"Using openpyxl to get worksheet '{sheet_name}' row count: {total_rows}")
                        except Exception as e:
                            logging.warning(f"Using openpyxl to get row count failed: {str(e)}")
                    
                    # Method 2: Only read the first column to get row count
                    if total_rows is None:
                        try:
                            # Build parameter dictionary, only add engine parameter when excel_engine is not None
                            excel_params = self._build_excel_params(sheet_name, excel_engine, usecols=[0], header=0)
                            df_count = pd.read_excel(
                                self.excel_path, 
                                **excel_params
                            )
                            total_rows = len(df_count)
                            logging.info(f"Using first column to get worksheet '{sheet_name}' row count: {total_rows}")
                        except Exception as e:
                            logging.warning(f"Using first column to get row count failed: {str(e)}")
                    
                    # Method 3: Traditional method - if both previous methods failed
                    if total_rows is None:
                        try:
                            # Build parameter dictionary, only add engine parameter when excel_engine is not None
                            excel_params = self._build_excel_params(sheet_name, excel_engine)
                            df_count = pd.read_excel(
                                self.excel_path, 
                                **excel_params
                            )
                            total_rows = len(df_count)
                            logging.info(f"Using traditional method to get worksheet '{sheet_name}' row count: {total_rows}")
                        except Exception as e:
                            logging.error(f"Get worksheet '{sheet_name}' row countfailure: {str(e)}")
                            return None
                    
                    # read sample rows for analysis - ensure reading enough rows
                    sample_size = min(MAX_SAMPLE_ROWS, total_rows)
                    
                    # Build parameter dictionary, only add engine parameter when excel_engine is not None
                    excel_params = self._build_excel_params(sheet_name, excel_engine, nrows=sample_size)
                    df_sample = pd.read_excel(
                        self.excel_path, 
                        **excel_params
                    )

                    # check if there are columns
                    if len(df_sample.columns) == 0:
                        logging.warning(f"worksheet '{sheet_name}' does not contain any columns, skipping")
                        return None

                    # get column names and data types
                    columns_info = []
                    column_stats = {}

                    # process column statistics: - keep key statistical information
                    for col_name in df_sample.columns:
                        # clean column names
                        clean_col_name = self._clean_column_name(col_name)

                        # inferdatatype
                        col_data = df_sample[col_name]
                        pandas_dtype = str(col_data.dtype)
                        duck_type = self._map_dtype_to_duck(pandas_dtype, col_data)

                        logging.info(f"Column '{col_name}' inferred type: {duck_type}")

                        # Collect statistics: - ensure key statistical information is complete
                        try:
                            if pd.api.types.is_numeric_dtype(col_data) and not any(isinstance(x, str) for x in col_data.dropna().head(100)):
                                # for numeric columns, keep complete statistics:
                                stats = {
                                    'min': col_data.min(),
                                    'max': col_data.max(),
                                    'mean': col_data.mean(),
                                    'null_count': col_data.isna().sum()
                                }
                            else:
                                # for non-numeric columns, calculate basic information
                                stats = {
                                    'unique_count': col_data.nunique(),
                                    'null_count': col_data.isna().sum()
                                }

                                # for small columns, collect unique values but keep information needed for validation
                                if col_data.nunique() < MAX_UNIQUE_VALUES:  # Keep more unique values
                                    unique_vals = col_data.dropna().unique().tolist()[:MAX_UNIQUE_VALUES]
                                    stats['unique_values'] = [str(val) for val in unique_vals]

                        except Exception as e:
                            stats = {'error': str(e)}
                            logging.warning(f"Collecting column '{col_name}' statistics: error: {str(e)}")

                        column_stats[col_name] = stats

                        columns_info.append({
                            'original_name': col_name,
                            'clean_name': clean_col_name,
                            'pandas_type': pandas_dtype,
                            'duck_type': duck_type,
                            'has_nulls': col_data.isna().any()
                        })

                    # Using reservoir sampling - this is the safest method, ensuring randomness and representativeness
                    sample_df = self._reservoir_sample(sheet_name, total_rows)
                    
                    # Thread-safely update samples
                    with samples_lock:
                        self.samples[sheet_name] = sample_df

                    sheet_info = {
                        'columns': columns_info,
                        'column_stats': column_stats,
                        'row_count': total_rows,
                    }

                    logging.info(f"worksheet '{sheet_name}' analysis completed, contains {len(columns_info)} columns, {total_rows} rows")
                    return (sheet_name, sheet_info)

                except Exception as sheet_error:
                    logging.error(f"Analyzing worksheet '{sheet_name}' error: {str(sheet_error)}")
                    logging.error(traceback.format_exc())
                    return None

            # Using thread pool to process worksheets in parallel - limit thread count to avoid resource contention
            max_workers = min(len(sheet_names), os.cpu_count() or 4)
            logging.info(f"Using {max_workers} threads to process  {len(sheet_names)} worksheets in parallel")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_sheet, sheet_name): sheet_name for sheet_name in sheet_names}
                
                for future in concurrent.futures.as_completed(futures):
                    sheet_name = futures[future]
                    try:
                        result = future.result()
                        if result:
                            sheet_name, sheet_info = result
                            self.sheets_info[sheet_name] = sheet_info
                    except Exception as e:
                        logging.error(f"processworksheet '{sheet_name}' resulterror: {str(e)}")

            # check if at least one worksheet was successfully analyzed
            if not self.sheets_info:
                logging.error("All worksheet analyses failed")
                return False

            elapsed_time = time.time() - start_time
            logging.info(f"Excel analysis completed, time elapsed {elapsed_time:.2f} seconds")
            return True

        except Exception as e:
            logging.error(f"analyzeExcelfileerror: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    def import_data(self, batch_size=DEFAULT_BATCH_SIZE):
        """
        Import data from Excel to DuckDB, using batch processing for improved performance
        
        Parameters:
            batch_size (int): Number of rows per batch, default is 5000
            
        Returns:
            dict: Import results
            
        Exceptions:
            Will not throw exceptions, but returns results containing error information
        """
        # validate batch_size parameter
        batch_size = self._validate_parameter("batch_size", batch_size, int, min_value=1, default_value=DEFAULT_BATCH_SIZE)
        
        # Dynamically adjust batch size based on system memory
        try:
            total_memory_gb = psutil.virtual_memory().total / (1024**3)  # Convert to GB
            
            # Adjust batch size based on available memory
            if total_memory_gb >= 64:
                max_batch_size = 200000  # 64GBand above memory
            elif total_memory_gb >= 32:
                max_batch_size = 100000  # 32GBmemory
            elif total_memory_gb >= 16:
                max_batch_size = 50000   # 16GBmemory
            elif total_memory_gb >= 8:
                max_batch_size = 20000   # 8GBmemory
            else:
                max_batch_size = DEFAULT_BATCH_SIZE    # Low memory system
                
            # If user-specified batch size is less than system-recommended value, use user-specified value
            if batch_size < max_batch_size:
                logging.info(f"Using user-specified batch size: {batch_size}rows")
            else:
                # Otherwise use the maximum value recommended by the system
                logging.info(f"Based on system memory({total_memory_gb:.1f}GB)adjust batch size to{max_batch_size}rows")
                batch_size = max_batch_size
                
        except Exception as e:
            # If memory detection fails, use conservative default value
            logging.warning(f"Memory detection failed, using default batch size{DEFAULT_BATCH_SIZE}rows: {str(e)}")
            if batch_size > DEFAULT_BATCH_SIZE:
                batch_size = DEFAULT_BATCH_SIZE
            
        all_results = {}

        for sheet_name, info in self.sheets_info.items():
            table_name = sheet_name.replace(' ', '_').replace('-', '_')
            temp_table_name = f"{table_name}_temp"
            total_rows = info['row_count']
            rows_imported = 0
            import_success = False
            error_message = None

            start_time = time.time()

            try:
                logging.info(f"startimportworksheet '{sheet_name}' to temporary table '{temp_table_name}'")

                # Calculate the number of batches to process
                num_batches = (total_rows + batch_size - 1) // batch_size

                # Get column name list
                column_names = [col['clean_name'] for col in info['columns']]

                # read and import data
                for i in range(num_batches):
                    # calculate the starting row and number of rows for the current batch
                    start_row = i * batch_size + 1  # +1 because skipping header row
                    if i == 0:
                        skiprows = None  # First batch does not skip header row
                    else:
                        skiprows = range(1, start_row)  # Skip header row and previously imported rows

                    # Calculate the number of rows to read
                    if i == num_batches - 1:
                        nrows = total_rows - (i * batch_size)
                    else:
                        nrows = batch_size

                    try:
                        logging.info(f"Start importing batch {i+1}/{num_batches}, from row {start_row if i > 0 else 1} start, plan to read {nrows} rows")

                        # Read batch data
                        excel_params = self._build_excel_params(sheet_name, skiprows=skiprows, nrows=nrows)
                        batch_df = pd.read_excel(
                            self.excel_path, 
                            **excel_params
                        )
                        
                        # clean column names
                        column_mapping = {}
                        for col in info['columns']:
                            column_mapping[col['original_name']] = col['clean_name']

                        # Ensure all columns exist
                        for orig_col in batch_df.columns:
                            if orig_col not in column_mapping:
                                # add possibly missing column mapping
                                clean_col = self._clean_column_name(orig_col)
                                column_mapping[orig_col] = clean_col
                                logging.warning(f"Found column not recorded in analysis phase: {orig_col} -> {clean_col}")

                        # Rename columns
                        batch_df = batch_df.rename(columns=column_mapping)

                        logging.info(f"Successfully read batch {i+1}/{num_batches}, actually read {len(batch_df)} rows")

                        # Usingtransactionbulkprocess
                        self.conn.execute("BEGIN TRANSACTION")

                        # Optimization: First try to import data using COPY statement (most efficient)
                        import tempfile
                        import os
                        
                        try:
                            # Use context manager to create temporary CSV file and ensure proper closure
                            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
                                temp_csv = temp_file.name
                                # Save batch data as CSV
                                batch_df.to_csv(temp_csv, index=False)
                            
                            try:
                                # Import data using COPY statement
                                self.conn.execute(fr'''
                                    COPY "{temp_table_name}" FROM '{temp_csv}' (AUTO_DETECT TRUE)
                                ''')
                                
                                # Delete temporary file after successful import
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                logging.info(f"Batch {i+1}/{num_batches}: Successfully imported data using COPY statement")
                                
                            except duckdb.Error as copy_error:
                                logging.error(f"DuckDB error: Failed to import using COPY statement: {str(copy_error)}")
                                logging.error(traceback.format_exc())
                                
                                # Delete temporary file
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                # Rollback transaction
                                self.conn.execute("ROLLBACK")
                                
                                # Restart transaction
                                self.conn.execute("BEGIN TRANSACTION")
                                
                                # Falling back to bulk INSERT method
                                raise Exception("Falling back to bulk INSERT method")
                            except IOError as io_error:
                                logging.error(f"IO error: Failed to import using COPY statement: {str(io_error)}")
                                logging.error(traceback.format_exc())
                                
                                # Delete temporary file
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                # Rollback transaction
                                self.conn.execute("ROLLBACK")
                                
                                # Restart transaction
                                self.conn.execute("BEGIN TRANSACTION")
                                
                                # Falling back to bulk INSERT method
                                raise Exception("Falling back to bulk INSERT method")
                            except Exception as copy_error:
                                logging.error(f"Other error: Failed to import using COPY statement: {str(copy_error)}")
                                logging.error(traceback.format_exc())
                                
                                # Delete temporary file
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                # Rollback transaction
                                self.conn.execute("ROLLBACK")
                                
                                # Restart transaction
                                self.conn.execute("BEGIN TRANSACTION")
                                
                                # Falling back to bulk INSERT method
                                raise Exception("Falling back to bulk INSERT method")
                                
                        except Exception as csv_error:
                            logging.error(f"Failed to create temporary CSV file or import using COPY statement: {str(csv_error)}")
                            
                            # Falling back to bulk INSERT method
                            logging.info("Trying to import data using bulk INSERT method")
                            
                            # Optimization: Use bulk insertion instead of row-by-row insertion
                            try:
                                # Prepare column name list
                                column_list = [f'"{col}"' for col in batch_df.columns]
                                
                                # Process data values - convert all values to strings or None
                                # Create a new DataFrame to store processed data
                                processed_data = []
                                
                                # Batch process row data
                                for _, row in batch_df.iterrows():
                                    row_values = []
                                    for val in row:
                                        if pd.isna(val):
                                            row_values.append(None)
                                        else:
                                            row_values.append(str(val))
                                    processed_data.append(row_values)
                                
                                # Build bulk insertion statement
                                placeholders = ', '.join(['(' + ', '.join(['?' for _ in range(len(column_list))]) + ')'] * len(processed_data))
                                insert_sql = f'INSERT INTO "{temp_table_name}" ({", ".join(column_list)}) VALUES {placeholders}'
                                
                                # Flatten processed data into a one-dimensional list
                                flat_values = [val for row in processed_data for val in row]
                                
                                # Execute bulk insertion
                                if flat_values:  # Ensure there is data to insert
                                    self.conn.execute(insert_sql, flat_values)
                                
                            except Exception as bulk_error:
                                logging.error(f"Bulk insertion failed, trying to use row-by-row insertion as a fallback: {str(bulk_error)}")
                                logging.error(traceback.format_exc())
                                
                                # Rollback transaction
                                self.conn.execute("ROLLBACK")
                                
                                # Restart transaction
                                self.conn.execute("BEGIN TRANSACTION")
                                
                                # Fallback: row-by-row insertion
                                logging.info("UsingFallback: row-by-row insertiondata")
                                for _, row in batch_df.iterrows():
                                    values = []
                                    placeholders = []
                                    column_list = []

                                    for col in batch_df.columns:
                                        val = row[col]
                                        # Ensure all values are converted to strings
                                        if pd.isna(val):
                                            values.append(None)
                                        else:
                                            values.append(str(val))
                                        placeholders.append('?')
                                        column_list.append(f'"{col}"')

                                    # Execute insertion
                                    insert_sql = f'INSERT INTO "{temp_table_name}" ({", ".join(column_list)}) VALUES ({", ".join(placeholders)})'
                                    self.conn.execute(insert_sql, values)

                        # Commit transaction
                        self.conn.execute("COMMIT")

                        rows_imported += len(batch_df)
                        logging.info(f"Batch {i+1}/{num_batches}: Imported {rows_imported}/{total_rows} rowsto table '{temp_table_name}'")

                    except Exception as batch_error:
                        # Rollback transaction
                        try:
                            self.conn.execute("ROLLBACK")
                        except Exception as rollback_error:
                            logging.error(f"Failed to rollback transaction: {str(rollback_error)}")
                            logging.error(traceback.format_exc())

                        # Record detailed error information
                        logging.error(f"Importing batch {i+1}/{num_batches} to table '{temp_table_name}' error: {str(batch_error)}")
                        logging.error(f"Error type: {type(batch_error).__name__}")
                        logging.error(traceback.format_exc())

                        # Try to record some data samples to help diagnose
                        if 'batch_df' in locals():
                            try:
                                sample_data = batch_df.head(5).to_dict()
                                logging.error(f"Data sample: {sample_data}")
                            except:
                                pass

                        # Re-throw exception
                        raise

                    # Check if operation is cancelled after each batch
                    if i % 3 == 0:  # Check every 3 batches
                        print(f"Imported: {rows_imported}/{total_rows} rows ({rows_imported/total_rows*100:.1f}%)")

                # Confirm if data was actually imported successfully
                self.conn.execute(f'SELECT COUNT(*) FROM "{temp_table_name}"')
                actual_imported = self.conn.fetchone()[0]

                if actual_imported > 0:
                    # Only rename the table when data is actually imported
                    self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    self.conn.execute(f'ALTER TABLE "{temp_table_name}" RENAME TO "{table_name}"')
                    self.conn.commit()  # Explicit commit
                    import_success = True
                    logging.info(f"Successfully renamed '{temp_table_name}' to '{table_name}', containing {actual_imported} rows of data")
                else:
                    error_message = "No data in table after import"
                    import_success = False
                    logging.error(f"Table '{temp_table_name}' no data after import")

                elapsed_time = time.time() - start_time
                logging.info(f"worksheet '{sheet_name}' import completed, time elapsed {elapsed_time:.2f} seconds")

            except Exception as e:
                error_message = str(e)
                logging.error(f"importdatato table '{temp_table_name}' error: {error_message}")

            all_results[sheet_name] = {
                'table_name': table_name,
                'total_rows': total_rows,
                'rows_imported': rows_imported,
                'import_success': import_success,
                'error_message': error_message,
                'import_time': time.time() - start_time
            }

        return all_results

    def run(self):
        """
        Execute the complete import process
        
        Returns:
            dict: Dictionary containing processing results
        """
        overall_start = time.time()
        
        # For storing results of each stage
        stage_results = {
            'database_connection': False,
            'excel_analysis': False,
            'table_creation': False,
            'data_import': None,
            'data_validation': None,
            'table_optimization': False
        }

        try:
            logging.info(f"Start importing Excel file '{self.excel_path}' to DuckDB database '{self.db_path}', safe mode: {'enabled' if self.safe_mode else 'disabled'}")

            print("Step 1/5: Connecting to database...")
            db_connection_success = self.connect_db()
            stage_results['database_connection'] = db_connection_success
            
            if not db_connection_success:
                return {
                    'success': False,
                    'error': "Failed to connect to database",
                    'stage_results': stage_results,
                    'total_time': time.time() - overall_start
                }

            print("Step 2/5: Analyzing Excel file structure...")
            excel_analysis_success = self.analyze_excel()
            stage_results['excel_analysis'] = excel_analysis_success
            
            if not excel_analysis_success:
                return {
                    'success': False,
                    'error': "Failed to analyze Excel file",
                    'stage_results': stage_results,
                    'total_time': time.time() - overall_start
                }

            print("Step 3/5: Creating database table structure...")
            try:
                self.create_tables()
                stage_results['table_creation'] = True
            except Exception as e:
                logging.error(f"Error creating table structure: {str(e)}")
                logging.error(traceback.format_exc())
                return {
                    'success': False,
                    'error': f"Failed to create table structure: {str(e)}",
                    'stage_results': stage_results,
                    'total_time': time.time() - overall_start
                }

            print("Step 4/5: Importing data...")
            import_results = self.import_data()
            stage_results['data_import'] = import_results

            # checkImport results
            successful_imports = [sheet for sheet, result in import_results.items()
                                if result['import_success']]

            if not successful_imports:
                print("\nWarning: All worksheets import failed, skipping validation step")
                logging.warning("All worksheets import failed, skipping validation step")
                # Return error result early
                return {
                    'success': False,
                    'error': "All worksheets import failed",
                    'stage_results': stage_results,
                    'import_results': import_results,
                    'total_time': time.time() - overall_start
                }

            print("Step 5/5: Validating data integrity...")
            try:
                validation_results = self.validate_import()
                stage_results['data_validation'] = validation_results
            except Exception as e:
                logging.error(f"validatedataerror: {str(e)}")
                logging.error(traceback.format_exc())
                validation_results = []
                stage_results['data_validation'] = []
                # Continue execution, as data may have been imported successfully

            # Output validation result summary
            if validation_results:
                print("\nData import validation results:")
                print("-" * 100)
                print(f"{'worksheet':<20} {'Data table':<20} {'Excel rows':<10} {'Database rows':<10} {'Status':<10} {'Message'}")
                print("-" * 100)

                for result in validation_results:
                    status_emoji = "✅" if result['overall_status'] == 'success' else "⚠️" if result['overall_status'] == 'warning' else "❌"
                    msg = result['messages'][0] if result['messages'] else ""
                    print(
                        f"{result['sheet']:<20} {result['table']:<20} "
                        f"{result['excel_rows']:<10} {result['db_rows']:<10} "
                        f"{status_emoji} {result['overall_status']:<8} {msg}"
                    )

                    # Print detailed messages
                    if len(result['messages']) > 1:
                        for msg in result['messages'][1:]:
                            print(f"{'':<60} - {msg}")

            # optimizeTable
            if validation_results and any(result['overall_status'] in ('success', 'warning') for result in validation_results):
                print("\nAt least one table validated successfully, optimizing table structure...")
                try:
                    self.optimize_tables()
                    stage_results['table_optimization'] = True
                except Exception as e:
                    logging.error(f"optimizeTableerror: {str(e)}")
                    logging.error(traceback.format_exc())
                    # Continue execution, as this is just an optimization step

            overall_time = time.time() - overall_start
            print(f"\nImport process completed, total time elapsed: {overall_time:.2f} seconds")

            logging.info(f"Excel to DuckDB import process completed, total time elapsed: {overall_time:.2f} seconds")

            # Return processing result
            return {
                'success': validation_results and any(result['overall_status'] in ('success', 'warning') for result in validation_results),
                'validation_results': validation_results,
                'stage_results': stage_results,
                'import_results': import_results,
                'total_time': overall_time
            }

        except Exception as e:
            logging.error(f"Error during import process: {str(e)}")
            logging.error(traceback.format_exc())
            print(f"Error: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'stage_results': stage_results,
                'total_time': time.time() - overall_start
            }

        finally:
            self.close()

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logging.info("Database connection has been closed")

    def validate_import(self):
        """Comprehensive validation of the correctness and completeness of data import"""
        validation_results = []

        for sheet_name, info in self.sheets_info.items():
            table_name = sheet_name.replace(' ', '_').replace('-', '_')
            excel_count = info['row_count']
            excel_sample = self.samples[sheet_name]

            # Create validation result object
            validation = {
                'sheet': sheet_name,
                'table': table_name,
                'excel_rows': excel_count,
                'db_rows': 0,
                'row_count_match': False,
                'column_count_match': False,
                'data_types_match': [],
                'stats_verification': [],
                'overall_status': 'pending',
                'messages': []
            }

            try:
                # Check if table exists
                try:
                    self.conn.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    db_count = self.conn.fetchone()[0]
                except Exception as e:
                    # Try to check temporary table
                    try:
                        temp_table_name = f"{table_name}_temp"
                        self.conn.execute(f'SELECT COUNT(*) FROM "{temp_table_name}"')
                        db_count = self.conn.fetchone()[0]
                        table_name = temp_table_name  # Use temporary table name for subsequent operations
                        validation['messages'].append(f"Using temporary table for validation")
                    except Exception as inner_e:
                        validation['messages'].append(f"Table and temporary table both do not exist: {str(e)}")
                        db_count = 0

                validation['db_rows'] = db_count
                validation['row_count_match'] = (excel_count == db_count)

                if not validation['row_count_match']:
                    validation['messages'].append(
                        f"Row count does not match! Excel: {excel_count}, Database: {db_count}"
                    )

                # Only continue validation if table exists and has data
                if db_count > 0:
                    # 2. Validate column count and column names
                    self.conn.execute(f'PRAGMA table_info("{table_name}")')
                    db_columns = self.conn.fetchall()
                    db_column_names = [col[1] for col in db_columns]

                    excel_column_names = [col['clean_name'] for col in info['columns']]
                    validation['column_count_match'] = (len(db_column_names) == len(excel_column_names))

                    missing_columns = set(excel_column_names) - set(db_column_names)
                    if missing_columns:
                        validation['messages'].append(
                            f"Missing columns: {missing_columns}"
                        )

                    # 3. datatypevalidate
                    for col in info['columns']:
                        col_name = col['clean_name']
                        if col_name in db_column_names:
                            # Get data types from database
                            db_col_idx = db_column_names.index(col_name)
                            db_col_type = db_columns[db_col_idx][2]

                            # Simplify type comparison (only compare main types)
                            expected_type = col['duck_type'].split('(')[0]
                            actual_type = db_col_type.split('(')[0]

                            # In safe mode, all columns are TEXT type
                            if self.safe_mode:
                                expected_type = 'TEXT'

                            type_match = (expected_type.upper() == actual_type.upper() or
                                         (expected_type.upper() == 'TEXT' and actual_type.upper() == 'VARCHAR') or
                                         (expected_type.upper() == 'VARCHAR' and actual_type.upper() == 'TEXT'))
                            validation['data_types_match'].append({
                                'column': col_name,
                                'expected': expected_type if self.safe_mode else col['duck_type'],
                                'actual': db_col_type,
                                'match': type_match
                            })

                            if not type_match:
                                validation['messages'].append(
                                    f"Column '{col_name}' type mismatch: expected {expected_type if self.safe_mode else col['duck_type']}, actual {db_col_type}"
                                )

                    # 5. Validate statistical information (only performed in non-safe mode)
                    if not self.safe_mode:
                        # Bulk query optimization: Build all statistics: queries and execute them at once
                        validation_queries = []
                        query_columns = []
                        
                        for col_name, stats in info['column_stats'].items():
                            clean_col_name = self._clean_column_name(col_name)
                            
                            if 'min' in stats and 'max' in stats:
                                # Perform statistics: validation for numeric columns
                                validation_queries.append(f'SELECT MIN("{clean_col_name}"), MAX("{clean_col_name}"), AVG("{clean_col_name}"), STDDEV("{clean_col_name}") FROM "{table_name}"')
                                query_columns.append(('numeric', col_name, clean_col_name, stats))
                            elif 'unique_count' in stats:
                                # Validate unique value count for categorical columns
                                validation_queries.append(f'SELECT COUNT(DISTINCT "{clean_col_name}") FROM "{table_name}"')
                                query_columns.append(('categorical', col_name, clean_col_name, stats))
                        
                        # Execute bulk query
                        try:
                            results = []
                            for query in validation_queries:
                                self.conn.execute(query)
                                results.append(self.conn.fetchone())
                            
                            # processqueryresult
                            for i, (query_type, col_name, clean_col_name, stats) in enumerate(query_columns):
                                if query_type == 'numeric':
                                    try:
                                        db_min, db_max, db_avg, db_stddev = results[i]
                                        
                                        # Compare minimum value, maximum value and average value
                                        min_match = abs(stats['min'] - db_min) < 0.0001
                                        max_match = abs(stats['max'] - db_max) < 0.0001
                                        avg_match = abs(stats['mean'] - db_avg) < 0.0001
                                        
                                        # Enhanced validation: Compare standard deviation (if available in Excel sample)
                                        stddev_match = True
                                        if 'std' in stats and db_stddev is not None:
                                            stddev_match = abs(stats['std'] - db_stddev) < max(0.01, stats['std'] * 0.05)
                                        
                                        validation['stats_verification'].append({
                                            'column': col_name,
                                            'min_match': min_match,
                                            'max_match': max_match,
                                            'avg_match': avg_match,
                                            'stddev_match': stddev_match,
                                            'status': 'success' if (min_match and max_match and avg_match and stddev_match) else 'error'
                                        })
                                        
                                        if not (min_match and max_match and avg_match and stddev_match):
                                            stddev_info = f", Std={stats['std']}" if 'std' in stats else ""
                                            db_stddev_info = f", Std={db_stddev}" if db_stddev is not None else ""
                                            validation['messages'].append(
                                                f"Column '{col_name}' Statistics mismatch: Excel(Min={stats['min']}, Max={stats['max']}, Avg={stats['mean']}{stddev_info}), "
                                                f"DB(Min={db_min}, Max={db_max}, Avg={db_avg}{db_stddev_info})"
                                            )
                                    except Exception as e:
                                        validation['messages'].append(
                                            f"Process column numeric value statistics: result error: {str(e)}"
                                        )
                                        
                                elif query_type == 'categorical':
                                    try:
                                        db_unique = results[i][0]
                                        
                                        # Compare unique value count (allowing some margin of error)
                                        unique_match = abs(stats['unique_count'] - db_unique) <= max(5, stats['unique_count'] * 0.05)
                                        
                                        validation['stats_verification'].append({
                                            'column': col_name,
                                            'unique_count_match': unique_match,
                                            'status': 'success' if unique_match else 'error'
                                        })
                                        
                                        if not unique_match:
                                            validation['messages'].append(
                                                f"Column '{col_name}' Unique value count mismatch: Excel={stats['unique_count']}, DB={db_unique}"
                                            )
                                    except Exception as e:
                                        validation['messages'].append(
                                            f"Process column categorical statistics: result error: {str(e)}"
                                        )
                        except Exception as batch_error:
                            logging.error(f"Bulk statistics: query execution error: {str(batch_error)}")
                            logging.error(traceback.format_exc())
                            
                            # Fallback to original one-by-one query method
                            logging.warning("Falling back to one-by-one query method for statistics: validation")
                            for col_name, stats in info['column_stats'].items():
                                clean_col_name = self._clean_column_name(col_name)
                                
                                if 'min' in stats and 'max' in stats:
                                    # Perform statistics: validation for numeric columns
                                    try:
                                        self.conn.execute(f'SELECT MIN("{clean_col_name}"), MAX("{clean_col_name}"), AVG("{clean_col_name}"), STDDEV("{clean_col_name}") FROM "{table_name}"')
                                        result = self.conn.fetchone()
                                        db_min, db_max, db_avg = result[0], result[1], result[2]
                                        db_stddev = result[3] if len(result) > 3 else None
                                        
                                        # Compare minimum value, maximum value and average value
                                        min_match = abs(stats['min'] - db_min) < 0.0001
                                        max_match = abs(stats['max'] - db_max) < 0.0001
                                        avg_match = abs(stats['mean'] - db_avg) < 0.0001
                                        
                                        # Enhanced validation: Compare standard deviation (if available in Excel sample)
                                        stddev_match = True
                                        if 'std' in stats and db_stddev is not None:
                                            stddev_match = abs(stats['std'] - db_stddev) < max(0.01, stats['std'] * 0.05)
                                        
                                        validation['stats_verification'].append({
                                            'column': col_name,
                                            'min_match': min_match,
                                            'max_match': max_match,
                                            'avg_match': avg_match,
                                            'stddev_match': stddev_match,
                                            'status': 'success' if (min_match and max_match and avg_match and stddev_match) else 'error'
                                        })
                                        
                                        if not (min_match and max_match and avg_match and stddev_match):
                                            stddev_info = f", Std={stats['std']}" if 'std' in stats else ""
                                            db_stddev_info = f", Std={db_stddev}" if db_stddev is not None else ""
                                            validation['messages'].append(
                                                f"Column '{col_name}' Statistics mismatch: Excel(Min={stats['min']}, Max={stats['max']}, Avg={stats['mean']}{stddev_info}), "
                                                f"DB(Min={db_min}, Max={db_max}, Avg={db_avg}{db_stddev_info})"
                                            )
                                    except Exception as e:
                                        validation['messages'].append(
                                            f"validateColumn '{col_name}' statistics: error: {str(e)}"
                                        )
                                        
                                elif 'unique_count' in stats:
                                    # Validate unique value count for categorical columns
                                    try:
                                        self.conn.execute(f'SELECT COUNT(DISTINCT "{clean_col_name}") FROM "{table_name}"')
                                        db_unique = self.conn.fetchone()[0]
                                        
                                        # Compare unique value count (allowing some margin of error)
                                        unique_match = abs(stats['unique_count'] - db_unique) <= max(5, stats['unique_count'] * 0.05)
                                        
                                        validation['stats_verification'].append({
                                            'column': col_name,
                                            'unique_count_match': unique_match,
                                            'status': 'success' if unique_match else 'error'
                                        })
                                        
                                        if not unique_match:
                                            validation['messages'].append(
                                                f"Column '{col_name}' Unique value count mismatch: Excel={stats['unique_count']}, DB={db_unique}"
                                            )
                                    except Exception as e:
                                        validation['messages'].append(
                                            f"Validate column unique value error: {str(e)}"
                                        )

                # 6. Comprehensive validation status determination
                if db_count == 0:
                    validation['overall_status'] = 'error'
                    if not validation['messages']:
                        validation['messages'].append("No data in database table")
                elif validation['row_count_match'] and validation['column_count_match'] and \
                   all(dtm['match'] for dtm in validation['data_types_match']):
                    # In safe mode, do not check statistics: information match
                    if self.safe_mode or all(sv['status'] == 'success' for sv in validation['stats_verification'] if 'status' in sv):
                        validation['overall_status'] = 'success'
                        validation['messages'].append("Data validation successful (row count, column count, data type and statistics: information all match)")
                    else:
                        validation['overall_status'] = 'warning'  # Statistics mismatch but main indicators match
                        validation['messages'].append("Data validation basically successful, but partial statistics: mismatch")
                else:
                    validation['overall_status'] = 'error'
                    if not validation['messages']:
                        validation['messages'].append("Data validation failed (row count, column count or data type mismatch)")

            except Exception as e:
                validation['overall_status'] = 'error'
                validation['messages'].append(f"Error in validation process: {str(e)}")
                logging.error(f"validateTable '{table_name}' error: {str(e)}")

            validation_results.append(validation)

        return validation_results

    def optimize_tables(self):
        """Optimize imported tables, add indexes, etc."""
        for sheet_name, info in self.sheets_info.items():
            table_name = sheet_name.replace(' ', '_').replace('-', '_')

            try:
                # Check if table exists
                try:
                    self.conn.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                except Exception as e:
                    logging.warning(f"Cannot optimize table '{table_name}', Table does not exist: {str(e)}")
                    continue

                # Optimize table structure
                self.conn.execute(f'ANALYZE "{table_name}"')

                # If the table was imported in safe mode, type conversion optimization can be performed
                if self.safe_mode:
                    logging.info(f"Safe mode import - trying to optimize table '{table_name}' data types")

                    # Iterate through each column and try automatic type conversion
                    for col in info['columns']:
                        col_name = col['clean_name']
                        original_duck_type = col['duck_type']

                        # Try to convert to appropriate type
                        if original_duck_type != 'TEXT' and not 'VARCHAR' in original_duck_type:
                            try:
                                if original_duck_type in ('INTEGER', 'BIGINT'):
                                    # First validate if all data can be converted to integers
                                    self.conn.execute(f'''
                                        SELECT COUNT(*) FROM "{table_name}" 
                                        WHERE "{col_name}" IS NOT NULL 
                                        AND "{col_name}" <> '' 
                                        AND "{col_name}" NOT SIMILAR TO '-?[0-9]+'
                                    ''')
                                    invalid_count = self.conn.fetchone()[0]
                                    
                                    if invalid_count > 0:
                                        logging.warning(f"Column '{col_name}' containing {invalid_count} rows that cannot be converted to integers, keeping as TEXT type")
                                        continue
                                        
                                    # Try to convert to integers
                                    self.conn.execute(f'''
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col_name}"
                                        TYPE {original_duck_type}
                                        USING CAST("{col_name}" AS {original_duck_type})
                                    ''')
                                    logging.info(f"Successfully optimized table '{table_name}' column '{col_name}' to {original_duck_type} type")
                                elif original_duck_type == 'DOUBLE':
                                    # First validate if all data can be converted to floating point numbers
                                    self.conn.execute(fr'''
                                        SELECT COUNT(*) FROM "{table_name}" 
                                        WHERE "{col_name}" IS NOT NULL 
                                        AND "{col_name}" <> '' 
                                        AND "{col_name}" NOT SIMILAR TO '-?[0-9]*\.?[0-9]*([eE][-+]?[0-9]+)?'
                                    ''')
                                    invalid_count = self.conn.fetchone()[0]
                                    
                                    if invalid_count > 0:
                                        logging.warning(f"Column '{col_name}' containing {invalid_count} rows that cannot be converted to floating point numbers, keeping as TEXT type")
                                        continue
                                    
                                    # Try to convert to floating point numbers
                                    self.conn.execute(f'''
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col_name}"
                                        TYPE DOUBLE
                                        USING CAST("{col_name}" AS DOUBLE)
                                    ''')
                                    logging.info(f"Successfully optimized table '{table_name}' column '{col_name}' to DOUBLE type")
                                elif original_duck_type in ('DATE', 'TIMESTAMP'):
                                    # First validate if all data can be converted to date/time
                                    if original_duck_type == 'DATE':
                                        self.conn.execute(f'''
                                            SELECT COUNT(*) FROM "{table_name}" 
                                            WHERE "{col_name}" IS NOT NULL 
                                            AND "{col_name}" <> '' 
                                            AND TRY_CAST("{col_name}" AS DATE) IS NULL
                                        ''')
                                    else:  # TIMESTAMP
                                        self.conn.execute(f'''
                                            SELECT COUNT(*) FROM "{table_name}" 
                                            WHERE "{col_name}" IS NOT NULL 
                                            AND "{col_name}" <> '' 
                                            AND TRY_CAST("{col_name}" AS TIMESTAMP) IS NULL
                                        ''')
                                    
                                    invalid_count = self.conn.fetchone()[0]
                                    
                                    if invalid_count > 0:
                                        logging.warning(f"Column '{col_name}' containing {invalid_count} rows that cannot be converted, keeping as TEXT type")
                                        continue
                                    
                                    # Try to convert to date
                                    self.conn.execute(f'''
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col_name}"
                                        TYPE {original_duck_type}
                                        USING CAST("{col_name}" AS {original_duck_type})
                                    ''')
                                    logging.info(f"Successfully optimized table '{table_name}' column '{col_name}' to {original_duck_type} type")
                            except Exception as e:
                                logging.warning(f"Cannot convert table '{table_name}' column '{col_name}' to {original_duck_type}: {str(e)}")

                # Create possible indexes
                for col in info['columns']:
                    col_name = col['clean_name']
                    col_stats = info['column_stats'].get(col['original_name'], {})

                    # If column has a small number of unique values and is not a primary key column, consider adding an index
                    if 'unique_count' in col_stats and \
                       col_stats['unique_count'] > 1 and \
                       col_stats['unique_count'] < 1000 and \
                       col_stats['unique_count'] < info['row_count'] / 10:

                        index_name = f"idx_{table_name}_{col_name}"
                        try:
                            self.conn.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ("{col_name}")')
                            logging.info(f"for table '{table_name}' column '{col_name}' create index")
                        except Exception as e:
                            logging.warning(f"Cannot create index for table '{table_name}' column '{col_name}' create index: {str(e)}")

                logging.info(f"Table '{table_name}' optimization completed")

            except Exception as e:
                logging.error(f"optimizeTable '{table_name}' error: {str(e)}")

    def _reservoir_sample(self, sheet_name, total_rows):
        """
        Use reservoir sampling algorithm to get sample data from Excel
        
        Reservoir sampling algorithm explanation:
        1. Reservoir sampling is an algorithm for randomly selecting a fixed number of samples from a stream of data without knowing the total size
        2. The algorithm guarantees that each element has an equal probability of being selected, regardless of the data size
        3. Implementation steps:
           - First put the first k elements into the "reservoir"
           - For the i-th element (i>k), decide whether to replace a random element in the reservoir with a probability of k/i
           - This ensures that the probability of each element being selected is k/n
        4. In this implementation, to avoid loading the entire Excel at once, batch processing is used to read data
        
        Parameters:
            sheet_name (str): worksheetname
            total_rows (int): total row count of worksheet
            
        Returns:
            pandas.DataFrame: Randomly selected sample data
        """
        sample_size = min(self.sample_size, total_rows)
        
        # Using default engine
        excel_engine = None

        if total_rows <= sample_size:
            # If total row count is less than sample size, directly read all rows
            excel_params = self._build_excel_params(sheet_name, excel_engine)
            return pd.read_excel(self.excel_path, **excel_params)

        # Reservoir sampling - read all data but only keep random samples
        reservoir = []

        # Batch read Excel file
        batch_size = RESERVOIR_BATCH_SIZE
        batches = (total_rows + batch_size - 1) // batch_size

        for i in range(batches):
            start_row = i * batch_size

            # If this is the last batch, adjust nrows
            if i == batches - 1:
                nrows = total_rows - (i * batch_size)
            else:
                nrows = batch_size

            try:
                # Build parameter dictionary, only add engine parameter when excel_engine is not None
                excel_params = self._build_excel_params(sheet_name, excel_engine, skiprows=range(1, start_row + 1) if start_row > 0 else None, nrows=nrows)
                
                batch = pd.read_excel(
                    self.excel_path, 
                    **excel_params
                )
                
                # Process each row in this batch
                for j in range(len(batch)):
                    # Currently processing the(start_row + j)rows
                    current_row_index = start_row + j
                    
                    if len(reservoir) < sample_size:
                        # Stage 1: Directly fill the reservoir until the sample size is reached
                        reservoir.append(batch.iloc[j])
                    else:
                        # Stage 2: Replace existing samples with decreasing probability
                        # For the i-th element, decide whether to replace a random element in the reservoir with a probability of k/i
                        # Here k is sample_size, i is current_row_index
                        r = random.randint(0, current_row_index)
                        if r < sample_size:
                            reservoir[r] = batch.iloc[j]
            except Exception as e:
                logging.warning(f"Error collecting sample from worksheet '{sheet_name}' batch {i+1}/{batches} error: {str(e)}")
                # Continue processing other batches

        return pd.DataFrame(reservoir)

    def _compute_sample_hash(self, df):
        """
        Calculate hash value of sample data for later verification
        
        Function description:
        1. This function is used to generate a unique identifier (hash value) for data samples
        2. Main uses:
           - Validate data consistency before and after import
           - Detect if data has changed during processing
           - Serve as part of data completeness validation
        3. Implementation method:
           - Convert DataFrame to CSV string format
           - Use MD5 algorithm to calculate the hash value of the string
           - Return the hash string in hexadecimal format
        
        Parameters:
            df (pandas.DataFrame): Data sample for which to calculate hash value
            
        Returns:
            str: MD5 hash value calculated, or "hash_error" if calculation fails
        """
        # Convert DataFrame to string and calculate hash
        try:
            sample_str = df.to_csv(index=False)
            return hashlib.md5(sample_str.encode()).hexdigest()
        except Exception as e:
            logging.warning(f"Error calculating sample hash value: {str(e)}")
            return "hash_error"

    def _clean_column_name(self, column_name):
        """
        Clean column names, remove illegal characters and ensure names meet DuckDB requirements
        
        Function description:
        1. This function processes column names from Excel to conform to DuckDB naming conventions
        2. Main cleaning operations include:
           - Convert non-string type column names to strings
           - Replace spaces and hyphens with underscores
           - Remove all non-alphanumeric and underscore characters
           - Ensure column names do not start with a digit (DuckDB requirement)
           - Handle cases of empty column names
        3. Importance of cleaning column names:
           - Avoid SQL syntax errors
           - Ensure consistency of column names in the database
           - Simplify subsequent data queries and processing
        
        Parameters:
            column_name: Original column name, can be any type
            
        Returns:
            str: Cleaned column name that conforms to DuckDB naming conventions
        """
        if not isinstance(column_name, str):
            column_name = str(column_name)

        # Replace spaces and special characters
        clean_name = column_name.replace(' ', '_').replace('-', '_')

        # Remove other illegal characters
        clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')

        # Ensure it does not start with a digit
        if clean_name and clean_name[0].isdigit():
            clean_name = 'col_' + clean_name

        # Avoid column name becoming empty
        if not clean_name:
            clean_name = 'column'

        return clean_name

    def _build_excel_params(self, sheet_name, excel_engine=None, **kwargs):
        """
        Build Excel parameter dictionary for pandas to read Excel files
        
        Function description:
        1. This function is used to consistently build parameter dictionaries needed for pandas.read_excel
        2. Main functions:
           - Set basic parameters such as worksheet name
           - Add engine parameters as needed (such as openpyxl, xlrd, etc.)
           - Process other optional parameters (such as row range, column range, etc.)
        3. Benefits of using this function:
           - Reduce code duplication
           - Ensure consistency in parameter settings
           - Simplify parameter management, especially when Excel needs to be read in multiple places
        
        Parameters:
            sheet_name (str): worksheetname
            excel_engine (str, optional): Excel engine name, such as 'openpyxl' or 'xlrd'
            **kwargs: Other parameters supported by pandas.read_excel, commonly used ones include:
                      - skiprows: Number of rows to skip or list of row indices
                      - nrows: Maximum number of rows to read
                      - usecols: Column indices or names to read
                      - header: Position of header row
            
        Returns:
            dict: Parameter dictionary built, can be used directly with pandas.read_excel function
        """
        # Create basic parameter dictionary
        params = {'sheet_name': sheet_name}
        
        # Add other parameters
        for key, value in kwargs.items():
            if value is not None:  # Only add non-None parameters
                params[key] = value
                
        # Only add engine parameter when excel_engine is not None
        if excel_engine is not None:
            params['engine'] = excel_engine
            
        return params
        
    def _validate_parameter(self, param_name, param_value, param_type, min_value=None, max_value=None, default_value=None):
        """
        Validate parameter type and range, ensure parameters meet expectations
        
        Parameter validation algorithm description:
        1. The goal of parameter validation is to ensure user-provided parameters meet expected type and range requirements
        2. Validation process includes the following steps:
           - Check if parameter is None, if so use default value
           - Validate if parameter type meets expectations, if not try to convert
           - For numeric types, validate if within specified minimum and maximum value range
           - If validation fails, log a warning and use default value
        3. Validation result:
           - If validation succeeds, return the validated parameter value
           - If validation fails and there is a default value, return the default value
           - If validation fails and there is no default value, raise a ValueError exception
        
        Parameters:
            param_name (str): Parameter name, used for logging
            param_value: Parameter value to validate
            param_type: Expected parameter type (such as int, str, etc.)
            min_value: Optional, minimum allowed value for the parameter
            max_value: Optional, maximum allowed value for the parameter
            default_value: Optional, default value to use if validation fails
            
        Returns:
            Validated parameter value or default value
            
        Exceptions:
            ValueError: If validation fails and no default value is provided
        """
        # If parameter is None and there is a default value, use default value
        if param_value is None and default_value is not None:
            logging.info(f"Parameter '{param_name}' is None, using default value: {default_value}")
            return default_value
            
        # Validate parameter type
        if not isinstance(param_value, param_type):
            try:
                # Try to convert type
                param_value = param_type(param_value)
                logging.info(f"Parameter '{param_name}' type has been converted from {type(param_value).__name__} to {param_type.__name__}")
            except (ValueError, TypeError) as e:
                if default_value is not None:
                    logging.warning(f"Parameter '{param_name}' type error, cannot convert to {param_type.__name__}, using default value {default_value}: {str(e)}")
                    return default_value
                else:
                    raise ValueError(f"Parameter '{param_name}' type error, should be {param_type.__name__}: {str(e)}")
        
        # Validate numeric value range (only applicable to numeric types)
        if isinstance(param_value, (int, float)):
            if min_value is not None and param_value < min_value:
                if default_value is not None:
                    logging.warning(f"Parameter '{param_name}' value {param_value} is less than minimum value {min_value}, using default value {default_value}")
                    return default_value
                else:
                    raise ValueError(f"Parameter '{param_name}' value {param_value} is less than minimum value {min_value}")
                    
            if max_value is not None and param_value > max_value:
                if default_value is not None:
                    logging.warning(f"Parameter '{param_name}' value {param_value} is greater than maximum value {max_value}, using default value {default_value}")
                    return default_value
                else:
                    raise ValueError(f"Parameter '{param_name}' value {param_value} is greater than maximum value {max_value}")
        
        return param_value

    def _map_dtype_to_duck(self, pandas_dtype, series):
        """
        Map pandas data types to DuckDB data types, and perform intelligent type inference
        
        Type mapping algorithm description:
        1. The goal of type mapping is to map pandas data types to the most appropriate DuckDB data types
        2. Mapping process follows these rules:
           - In safe mode, all columns use TEXT type to avoid type conversion errors
           - For numeric types, distinguish between INTEGER and BIGINT, choose appropriate type based on numeric value range
           - For floating point types, uniformly use DOUBLE type
           - For date and time types, map to DATE, TIMESTAMP or TIME based on specific type
           - For boolean types, map to BOOLEAN
           - For object and string types, use TEXT type as the safest choice
        3. Additional safety checks:
           - For numeric types, check if sample contains non-numeric values, if so use TEXT type instead
        
        Parameters:
            pandas_dtype (str): pandas data type string
            series (pandas.Series): Columndata
            
        Returns:
            str: DuckDB data type after mapping
        """
        # In safe mode, all columns use TEXT type
        if self.safe_mode:
            return 'TEXT'

        # Check if there are any non-numeric value strings
        if 'int' in pandas_dtype or 'float' in pandas_dtype:
            # Additional check - ensure numeric columns do not contain string values
            try:
                # Check if sample contains non-numeric values
                sample = series.dropna().head(100)
                for val in sample:
                    if isinstance(val, str):
                        logging.warning(f"Column numeric type contains string value '{val}', switching to TEXT type")
                        return 'TEXT'
            except:
                # If check fails, use more conservative TEXT type
                return 'TEXT'

        # Process common numeric types
        if 'int' in pandas_dtype:
            if series.max() > 2147483647 or series.min() < -2147483648:
                return 'BIGINT'
            return 'INTEGER'

        elif 'float' in pandas_dtype:
            return 'DOUBLE'

        # Process date and time types
        elif 'datetime' in pandas_dtype:
            return 'TIMESTAMP'

        elif 'date' in pandas_dtype:
            return 'DATE'

        elif 'time' in pandas_dtype:
            return 'TIME'

        # Process boolean types
        elif 'bool' in pandas_dtype:
            return 'BOOLEAN'

        # Process string and object types - more conservative inference
        elif 'object' in pandas_dtype or 'string' in pandas_dtype:
            # For object types, all use TEXT type, safest option
            return 'TEXT'

        # defaulttype
        return 'TEXT'

    def create_tables(self):
        """
        Create DuckDB table based on Excel structure
        
        Returns:
            bool: Whether creation was successful
            
        Exceptions:
            Will not throw exceptions, but returns False and logs errors
        """
        try:
            # Check if already connected to database
            if not self.is_connected or self.conn is None:
                logging.error("Database not connected when trying to create table")
                return False
                
            # Check if worksheet information exists
            if not self.sheets_info:
                logging.error("No available worksheet information to create table")
                return False
                
            start_time = time.time()

            for sheet_name, info in self.sheets_info.items():
                # Check if column information exists
                if 'columns' not in info or not info['columns']:
                    logging.warning(f"worksheet '{sheet_name}' has no column information, skipping table creation")
                    continue
                    
                # Create table name - Replace spaces and special characters
                table_name = sheet_name.replace(' ', '_').replace('-', '_')

                # Create temporary table name for import and validation
                temp_table_name = f"{table_name}_temp"

                # Build CREATE TABLE statement
                columns_def = []
                for col in info['columns']:
                    # If safe mode is enabled, all columns use TEXT type
                    if self.safe_mode:
                        data_type = 'TEXT'
                    else:
                        data_type = col["duck_type"]

                    columns_def.append(f'"{col["clean_name"]}" {data_type}')

                # Check if there are column definitions
                if not columns_def:
                    logging.warning(f"worksheet '{sheet_name}' has no valid column definitions, skipping table creation")
                    continue

                try:
                    # Delete temporary table (if exists)
                    self.conn.execute(f'DROP TABLE IF EXISTS "{temp_table_name}"')

                    # create temporary table
                    create_stmt = f'CREATE TABLE "{temp_table_name}" (\n'
                    create_stmt += ',\n'.join(columns_def)
                    create_stmt += '\n)'

                    logging.info(f"For worksheet '{sheet_name}' create temporary table '{temp_table_name}', {'Using safe mode' if self.safe_mode else 'Using inferred type'}")
                    logging.debug(create_stmt)

                    self.conn.execute(create_stmt)
                except Exception as table_error:
                    logging.error(f"createTable '{temp_table_name}' error: {str(table_error)}")
                    logging.error(traceback.format_exc())
                    # Continue processing other tables

            elapsed_time = time.time() - start_time
            logging.info(f"Table structure creation completed, time elapsed {elapsed_time:.2f} seconds")
            return True

        except Exception as e:
            logging.error(f"createTableerror: {str(e)}")
            logging.error(traceback.format_exc())
            return False

# Usage example
if __name__ == "__main__":
    excel_file = input("Please enter the Excel file path: ")
    db_file = input("Please enter the DuckDB database file path (will be created if it doesn't exist): ")

    # defaultUsingsafe mode
    converter = ExcelToDuckDB(excel_file, db_file, safe_mode=True)
    result = converter.run()

    if result['success']:
        print("\nData has been successfully imported to DuckDB, you can start analyzing!")
        print("Example queries:")
        print("-----------------------------------------------------")
        conn = duckdb.connect(db_file)

        # Get table names
        tables = conn.execute("SHOW TABLES").fetchall()
        if tables:
            table_name = tables[0][0]
            print(f"-- View the first 5 rows of table {table_name} ::")
            print(f"SELECT * FROM \"{table_name}\" LIMIT 5;")

            print(f"\n-- View the first 5 rows of table {table_name} statistics::")
            print(f"SUMMARIZE \"{table_name}\";")

        conn.close()
