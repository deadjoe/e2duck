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
import psutil  # 用于检测系统内存
import concurrent.futures
import threading

# 日志级别标准
# DEBUG: 详细的开发调试信息，如变量值、中间计算结果等
# INFO: 正常操作信息，如开始/完成任务、配置信息等
# WARNING: 潜在问题或需要注意的情况，但不影响主要功能
# ERROR: 导致功能失败的错误，如文件无法打开、数据处理失败等
# CRITICAL: 导致程序无法继续运行的严重错误

# 设置日志
logging.basicConfig(
    filename=f'excel_to_duckdb_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 定义常量
DEFAULT_SAMPLE_SIZE = 100  # 默认样本大小
DEFAULT_BATCH_SIZE = 5000  # 默认批处理大小
DEFAULT_MEMORY_LIMIT = '4GB'  # 默认内存限制
MAX_SAMPLE_ROWS = 1000  # 最大样本行数
MAX_UNIQUE_VALUES = 30  # 最大唯一值数量
RESERVOIR_BATCH_SIZE = 1000  # 蓄水池抽样批次大小

class ExcelToDuckDB:
    def __init__(self, excel_path, db_path, sample_size=DEFAULT_SAMPLE_SIZE, safe_mode=True):
        """
        初始化Excel到DuckDB转换器
        
        参数：
            excel_path (str): Excel文件的路径
            db_path (str): DuckDB数据库文件的路径
            sample_size (int): 用于验证的样本大小，默认为100
            safe_mode (bool): 是否使用安全模式（所有列使用TEXT类型），默认为True
            
        异常：
            ValueError: 如果参数无效
            TypeError: 如果参数类型错误
        """
        # 验证参数类型
        if not isinstance(excel_path, str):
            raise TypeError("excel_path必须是字符串类型")
        if not isinstance(db_path, str):
            raise TypeError("db_path必须是字符串类型")
        if not isinstance(sample_size, int):
            raise TypeError("sample_size必须是整数类型")
        if not isinstance(safe_mode, bool):
            raise TypeError("safe_mode必须是布尔类型")
            
        # 验证参数值
        if not excel_path:
            raise ValueError("excel_path不能为空")
        if not db_path:
            raise ValueError("db_path不能为空")
        if sample_size <= 0:
            raise ValueError("sample_size必须大于0")
            
        # 验证Excel文件路径
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel文件不存在: {excel_path}")
        
        # 验证Excel文件扩展名
        _, ext = os.path.splitext(excel_path)
        if ext.lower() not in ['.xlsx', '.xls', '.xlsm']:
            raise ValueError(f"不支持的Excel文件格式: {ext}。请使用.xlsx、.xls或.xlsm格式")
            
        # 验证数据库路径
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            raise ValueError(f"数据库目录不存在: {db_dir}")
            
        self.excel_path = excel_path
        self.db_path = db_path
        self.conn = None
        self.sheets_info = {}
        self.sample_size = sample_size
        self.samples = {}
        self.safe_mode = safe_mode  # 安全模式 - 默认开启
        self.is_connected = False

    def connect_db(self):
        """
        连接到DuckDB数据库
        
        返回：
            bool: 连接是否成功
            
        异常：
            不会抛出异常，而是返回False并记录错误
        """
        try:
            # 尝试连接到现有数据库，如果不存在则创建
            self.conn = duckdb.connect(self.db_path)
            logging.info(f"成功连接到数据库: {self.db_path}")

            # 获取CPU核心数并设置合适的线程数
            cpu_count = multiprocessing.cpu_count()
            thread_count = min(cpu_count, 16)  # 避免过多线程导致上下文切换开销
            self.conn.execute(f"PRAGMA threads={thread_count}")
            logging.info(f"设置DuckDB并行处理线程数为{thread_count}")

            # 根据机器内存大小调整内存限制
            try:
                total_memory = psutil.virtual_memory().total / (1024**3)  # 转换为GB
                if total_memory >= 32:
                    memory_limit = "16GB"  # 对于32GB及以上内存的机器
                elif total_memory >= 16:
                    memory_limit = "8GB"   # 对于16GB内存的机器
                else:
                    memory_limit = "4GB"   # 默认设置
                
                self.conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
                logging.info(f"根据系统内存({total_memory:.1f}GB)设置DuckDB内存限制为{memory_limit}")
            except Exception as e:
                # 如果内存检测失败，使用默认设置
                self.conn.execute(f"PRAGMA memory_limit='{DEFAULT_MEMORY_LIMIT}'")
                logging.warning(f"内存检测失败，使用默认内存限制{DEFAULT_MEMORY_LIMIT}: {str(e)}")
                
            # 启用进度条
            self.conn.execute("PRAGMA enable_progress_bar=true")
            
            self.is_connected = True
            return True

        except Exception as e:
            logging.error(f"连接数据库时出错: {str(e)}")
            logging.error(traceback.format_exc())
            self.is_connected = False
            return False

    def analyze_excel(self):
        """
        分析Excel文件结构并提取样本数据用于验证
        
        返回：
            bool: 分析是否成功
            
        异常：
            不会抛出异常，而是返回False并记录错误
        """
        try:
            # 文件检查已在__init__中完成，这里不再重复检查
            
            # 检查文件大小
            file_size = os.path.getsize(self.excel_path) / (1024*1024)  # 转换为MB
            logging.info(f"Excel文件大小: {file_size:.2f} MB")

            start_time = time.time()
            logging.info(f"开始分析Excel文件: {self.excel_path}")

            # 使用默认引擎读取Excel文件
            excel_engine = None
            try:
                # 直接使用默认引擎
                xl = pd.ExcelFile(self.excel_path)
                logging.info("使用默认引擎读取Excel")
            except Exception as e:
                logging.error(f"读取Excel文件失败: {str(e)}")
                return False

            sheet_names = xl.sheet_names
            logging.info(f"Excel文件包含以下工作表: {sheet_names}")

            if not sheet_names:
                logging.error("Excel文件不包含任何工作表")
                return False
                
            # 创建线程锁以保护共享资源
            samples_lock = threading.Lock()
            
            # 定义工作表处理函数
            def process_sheet(sheet_name):
                try:
                    logging.info(f"分析工作表: {sheet_name}")
                    
                    # 获取行数 - 使用多种方法并验证
                    total_rows = None
                    
                    # 方法1: 尝试使用openpyxl直接获取行数
                    if excel_engine == 'openpyxl':
                        try:
                            sheet = xl.book[sheet_name]
                            # 找到最后一个非空行
                            max_row = sheet.max_row
                            # 验证最后一行是否真的有数据
                            has_header = True  # 假设有标题行
                            for row in range(max_row, 0, -1):
                                if any(sheet.cell(row=row, column=col).value is not None 
                                      for col in range(1, sheet.max_column + 1)):
                                    if has_header:
                                        total_rows = row - 1  # 减去标题行
                                    else:
                                        total_rows = row
                                    break
                            logging.info(f"使用openpyxl获取工作表 '{sheet_name}' 行数: {total_rows}")
                        except Exception as e:
                            logging.warning(f"使用openpyxl获取行数失败: {str(e)}")
                    
                    # 方法2: 只读取第一列来获取行数
                    if total_rows is None:
                        try:
                            # 构建参数字典，只在excel_engine不为None时添加engine参数
                            excel_params = self._build_excel_params(sheet_name, excel_engine, usecols=[0], header=0)
                            df_count = pd.read_excel(
                                self.excel_path, 
                                **excel_params
                            )
                            total_rows = len(df_count)
                            logging.info(f"使用第一列获取工作表 '{sheet_name}' 行数: {total_rows}")
                        except Exception as e:
                            logging.warning(f"使用第一列获取行数失败: {str(e)}")
                    
                    # 方法3: 传统方法 - 如果前两种方法都失败
                    if total_rows is None:
                        try:
                            # 构建参数字典，只在excel_engine不为None时添加engine参数
                            excel_params = self._build_excel_params(sheet_name, excel_engine)
                            df_count = pd.read_excel(
                                self.excel_path, 
                                **excel_params
                            )
                            total_rows = len(df_count)
                            logging.info(f"使用传统方法获取工作表 '{sheet_name}' 行数: {total_rows}")
                        except Exception as e:
                            logging.error(f"获取工作表 '{sheet_name}' 行数失败: {str(e)}")
                            return None
                    
                    # 读取样本行进行分析 - 确保读取足够的行
                    sample_size = min(MAX_SAMPLE_ROWS, total_rows)
                    
                    # 构建参数字典，只在excel_engine不为None时添加engine参数
                    excel_params = self._build_excel_params(sheet_name, excel_engine, nrows=sample_size)
                    df_sample = pd.read_excel(
                        self.excel_path, 
                        **excel_params
                    )

                    # 检查是否有列
                    if len(df_sample.columns) == 0:
                        logging.warning(f"工作表 '{sheet_name}' 不包含任何列，跳过")
                        return None

                    # 获取列名和数据类型
                    columns_info = []
                    column_stats = {}

                    # 处理列统计信息 - 保留关键统计信息
                    for col_name in df_sample.columns:
                        # 清理列名
                        clean_col_name = self._clean_column_name(col_name)

                        # 推断数据类型
                        col_data = df_sample[col_name]
                        pandas_dtype = str(col_data.dtype)
                        duck_type = self._map_dtype_to_duck(pandas_dtype, col_data)

                        logging.info(f"列 '{col_name}' 推断类型: {duck_type}")

                        # 收集统计信息 - 确保关键统计信息完整
                        try:
                            if pd.api.types.is_numeric_dtype(col_data) and not any(isinstance(x, str) for x in col_data.dropna().head(100)):
                                # 对数值列，保留完整统计信息
                                stats = {
                                    'min': col_data.min(),
                                    'max': col_data.max(),
                                    'mean': col_data.mean(),
                                    'null_count': col_data.isna().sum()
                                }
                            else:
                                # 对非数值列，计算基本信息
                                stats = {
                                    'unique_count': col_data.nunique(),
                                    'null_count': col_data.isna().sum()
                                }

                                # 对小型列收集唯一值，但保留验证所需的信息
                                if col_data.nunique() < MAX_UNIQUE_VALUES:  # 保留更多唯一值
                                    unique_vals = col_data.dropna().unique().tolist()[:MAX_UNIQUE_VALUES]
                                    stats['unique_values'] = [str(val) for val in unique_vals]

                        except Exception as e:
                            stats = {'error': str(e)}
                            logging.warning(f"收集列 '{col_name}' 统计信息时出错: {str(e)}")

                        column_stats[col_name] = stats

                        columns_info.append({
                            'original_name': col_name,
                            'clean_name': clean_col_name,
                            'pandas_type': pandas_dtype,
                            'duck_type': duck_type,
                            'has_nulls': col_data.isna().any()
                        })

                    # 使用蓄水池抽样 - 这是最安全的方法，确保随机性和代表性
                    sample_df = self._reservoir_sample(sheet_name, total_rows)
                    
                    # 线程安全地更新样本
                    with samples_lock:
                        self.samples[sheet_name] = sample_df

                    sheet_info = {
                        'columns': columns_info,
                        'column_stats': column_stats,
                        'row_count': total_rows,
                    }

                    logging.info(f"工作表 '{sheet_name}' 分析完成，包含 {len(columns_info)} 列，{total_rows} 行")
                    return (sheet_name, sheet_info)

                except Exception as sheet_error:
                    logging.error(f"分析工作表 '{sheet_name}' 时出错: {str(sheet_error)}")
                    logging.error(traceback.format_exc())
                    return None

            # 使用线程池并行处理工作表 - 限制线程数以避免资源争用
            max_workers = min(len(sheet_names), os.cpu_count() or 4)
            logging.info(f"使用 {max_workers} 个线程并行处理 {len(sheet_names)} 个工作表")
            
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
                        logging.error(f"处理工作表 '{sheet_name}' 结果时出错: {str(e)}")

            # 检查是否至少有一个工作表被成功分析
            if not self.sheets_info:
                logging.error("所有工作表分析均失败")
                return False

            elapsed_time = time.time() - start_time
            logging.info(f"Excel分析完成，耗时 {elapsed_time:.2f} 秒")
            return True

        except Exception as e:
            logging.error(f"分析Excel文件时出错: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    def import_data(self, batch_size=DEFAULT_BATCH_SIZE):
        """
        将数据从Excel导入到DuckDB，使用批处理以提高性能
        
        参数：
            batch_size (int): 每批处理的行数，默认为5000
            
        返回：
            dict: 导入结果
            
        异常：
            不会抛出异常，而是返回包含错误信息的结果
        """
        # 验证batch_size参数
        batch_size = self._validate_parameter("batch_size", batch_size, int, min_value=1, default_value=DEFAULT_BATCH_SIZE)
        
        # 根据系统内存动态调整批处理大小
        try:
            total_memory_gb = psutil.virtual_memory().total / (1024**3)  # 转换为GB
            
            # 根据可用内存调整批处理大小
            if total_memory_gb >= 64:
                max_batch_size = 200000  # 64GB及以上内存
            elif total_memory_gb >= 32:
                max_batch_size = 100000  # 32GB内存
            elif total_memory_gb >= 16:
                max_batch_size = 50000   # 16GB内存
            elif total_memory_gb >= 8:
                max_batch_size = 20000   # 8GB内存
            else:
                max_batch_size = DEFAULT_BATCH_SIZE    # 低内存系统
                
            # 如果用户指定的批处理大小小于系统建议值，则使用用户指定的值
            if batch_size < max_batch_size:
                logging.info(f"使用用户指定的批处理大小: {batch_size}行")
            else:
                # 否则使用系统建议的最大值
                logging.info(f"根据系统内存({total_memory_gb:.1f}GB)调整批处理大小为{max_batch_size}行")
                batch_size = max_batch_size
                
        except Exception as e:
            # 如果内存检测失败，使用保守的默认值
            logging.warning(f"内存检测失败，使用默认批处理大小{DEFAULT_BATCH_SIZE}行: {str(e)}")
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
                logging.info(f"开始导入工作表 '{sheet_name}' 到临时表 '{temp_table_name}'")

                # 计算需要处理的批次数
                num_batches = (total_rows + batch_size - 1) // batch_size

                # 获取列名列表
                column_names = [col['clean_name'] for col in info['columns']]

                # 读取并导入数据
                for i in range(num_batches):
                    # 计算当前批次的起始行和行数
                    start_row = i * batch_size + 1  # +1 因为跳过标题行
                    if i == 0:
                        skiprows = None  # 第一批次不跳过标题行
                    else:
                        skiprows = range(1, start_row)  # 跳过标题行和之前已导入的行

                    # 计算要读取的行数
                    if i == num_batches - 1:
                        nrows = total_rows - (i * batch_size)
                    else:
                        nrows = batch_size

                    try:
                        logging.info(f"开始导入批次 {i+1}/{num_batches}，从行 {start_row if i > 0 else 1} 开始，计划读取 {nrows} 行")

                        # 读取批次数据
                        excel_params = self._build_excel_params(sheet_name, skiprows=skiprows, nrows=nrows)
                        batch_df = pd.read_excel(
                            self.excel_path, 
                            **excel_params
                        )
                        
                        # 清理列名
                        column_mapping = {}
                        for col in info['columns']:
                            column_mapping[col['original_name']] = col['clean_name']

                        # 确保所有列都存在
                        for orig_col in batch_df.columns:
                            if orig_col not in column_mapping:
                                # 添加可能缺失的列映射
                                clean_col = self._clean_column_name(orig_col)
                                column_mapping[orig_col] = clean_col
                                logging.warning(f"发现未在分析阶段记录的列: {orig_col} -> {clean_col}")

                        # 重命名列
                        batch_df = batch_df.rename(columns=column_mapping)

                        logging.info(f"成功读取批次 {i+1}/{num_batches}，实际读取 {len(batch_df)} 行")

                        # 使用事务批量处理
                        self.conn.execute("BEGIN TRANSACTION")

                        # 优化：首先尝试使用COPY语句导入数据（最高效）
                        import tempfile
                        import os
                        
                        try:
                            # 使用上下文管理器创建临时CSV文件并确保正确关闭
                            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
                                temp_csv = temp_file.name
                                # 将批次数据保存为CSV
                                batch_df.to_csv(temp_csv, index=False)
                            
                            try:
                                # 使用COPY语句导入数据
                                self.conn.execute(fr'''
                                    COPY "{temp_table_name}" FROM '{temp_csv}' (AUTO_DETECT TRUE)
                                ''')
                                
                                # 导入成功后删除临时文件
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                logging.info(f"批次 {i+1}/{num_batches}: 使用COPY语句成功导入数据")
                                
                            except duckdb.Error as copy_error:
                                logging.error(f"DuckDB错误: 使用COPY语句导入失败: {str(copy_error)}")
                                logging.error(traceback.format_exc())
                                
                                # 删除临时文件
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                # 回滚事务
                                self.conn.execute("ROLLBACK")
                                
                                # 重新开始事务
                                self.conn.execute("BEGIN TRANSACTION")
                                
                                # 回退到批量INSERT方式
                                raise Exception("回退到批量INSERT方式")
                            except IOError as io_error:
                                logging.error(f"IO错误: 使用COPY语句导入失败: {str(io_error)}")
                                logging.error(traceback.format_exc())
                                
                                # 删除临时文件
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                # 回滚事务
                                self.conn.execute("ROLLBACK")
                                
                                # 重新开始事务
                                self.conn.execute("BEGIN TRANSACTION")
                                
                                # 回退到批量INSERT方式
                                raise Exception("回退到批量INSERT方式")
                            except Exception as copy_error:
                                logging.error(f"其他错误: 使用COPY语句导入失败: {str(copy_error)}")
                                logging.error(traceback.format_exc())
                                
                                # 删除临时文件
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                # 回滚事务
                                self.conn.execute("ROLLBACK")
                                
                                # 重新开始事务
                                self.conn.execute("BEGIN TRANSACTION")
                                
                                # 回退到批量INSERT方式
                                raise Exception("回退到批量INSERT方式")
                                
                        except Exception as csv_error:
                            logging.error(f"创建临时CSV文件或使用COPY语句导入失败: {str(csv_error)}")
                            
                            # 回退到批量INSERT方式
                            logging.info("尝试使用批量INSERT方式导入数据")
                            
                            # 优化：使用批量插入替代逐行插入
                            try:
                                # 准备列名列表
                                column_list = [f'"{col}"' for col in batch_df.columns]
                                
                                # 处理数据值 - 将所有值转换为字符串或None
                                # 创建一个新的DataFrame来存储处理后的数据
                                processed_data = []
                                
                                # 批量处理行数据
                                for _, row in batch_df.iterrows():
                                    row_values = []
                                    for val in row:
                                        if pd.isna(val):
                                            row_values.append(None)
                                        else:
                                            row_values.append(str(val))
                                    processed_data.append(row_values)
                                
                                # 构建批量插入语句
                                placeholders = ', '.join(['(' + ', '.join(['?' for _ in range(len(column_list))]) + ')'] * len(processed_data))
                                insert_sql = f'INSERT INTO "{temp_table_name}" ({", ".join(column_list)}) VALUES {placeholders}'
                                
                                # 扁平化处理后的数据为一维列表
                                flat_values = [val for row in processed_data for val in row]
                                
                                # 执行批量插入
                                if flat_values:  # 确保有数据要插入
                                    self.conn.execute(insert_sql, flat_values)
                                
                            except Exception as bulk_error:
                                logging.error(f"批量插入失败，尝试使用逐行插入作为备选方案: {str(bulk_error)}")
                                logging.error(traceback.format_exc())
                                
                                # 回滚事务
                                self.conn.execute("ROLLBACK")
                                
                                # 重新开始事务
                                self.conn.execute("BEGIN TRANSACTION")
                                
                                # 备选方案：逐行插入
                                logging.info("使用备选方案：逐行插入数据")
                                for _, row in batch_df.iterrows():
                                    values = []
                                    placeholders = []
                                    column_list = []

                                    for col in batch_df.columns:
                                        val = row[col]
                                        # 确保所有值都转换为字符串
                                        if pd.isna(val):
                                            values.append(None)
                                        else:
                                            values.append(str(val))
                                        placeholders.append('?')
                                        column_list.append(f'"{col}"')

                                    # 执行插入
                                    insert_sql = f'INSERT INTO "{temp_table_name}" ({", ".join(column_list)}) VALUES ({", ".join(placeholders)})'
                                    self.conn.execute(insert_sql, values)

                        # 提交事务
                        self.conn.execute("COMMIT")

                        rows_imported += len(batch_df)
                        logging.info(f"批次 {i+1}/{num_batches}: 已导入 {rows_imported}/{total_rows} 行到表 '{temp_table_name}'")

                    except Exception as batch_error:
                        # 回滚事务
                        try:
                            self.conn.execute("ROLLBACK")
                        except Exception as rollback_error:
                            logging.error(f"回滚事务失败: {str(rollback_error)}")
                            logging.error(traceback.format_exc())

                        # 记录详细错误信息
                        logging.error(f"导入批次 {i+1}/{num_batches} 到表 '{temp_table_name}' 时出错: {str(batch_error)}")
                        logging.error(f"错误类型: {type(batch_error).__name__}")
                        logging.error(traceback.format_exc())

                        # 尝试记录一些数据样本以帮助诊断
                        if 'batch_df' in locals():
                            try:
                                sample_data = batch_df.head(5).to_dict()
                                logging.error(f"数据样本: {sample_data}")
                            except:
                                pass

                        # 重新抛出异常
                        raise

                    # 每批次后检查是否取消操作
                    if i % 3 == 0:  # 每3个批次检查一次
                        print(f"已导入: {rows_imported}/{total_rows} 行 ({rows_imported/total_rows*100:.1f}%)")

                # 确认是否真的有数据导入成功
                self.conn.execute(f'SELECT COUNT(*) FROM "{temp_table_name}"')
                actual_imported = self.conn.fetchone()[0]

                if actual_imported > 0:
                    # 只有当真正导入了数据时才重命名表
                    self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    self.conn.execute(f'ALTER TABLE "{temp_table_name}" RENAME TO "{table_name}"')
                    self.conn.commit()  # 明确提交
                    import_success = True
                    logging.info(f"成功重命名 '{temp_table_name}' 为 '{table_name}'，包含 {actual_imported} 行数据")
                else:
                    error_message = "导入后表中无数据"
                    import_success = False
                    logging.error(f"表 '{temp_table_name}' 导入后无数据")

                elapsed_time = time.time() - start_time
                logging.info(f"工作表 '{sheet_name}' 导入完成，耗时 {elapsed_time:.2f} 秒")

            except Exception as e:
                error_message = str(e)
                logging.error(f"导入数据到表 '{temp_table_name}' 时出错: {error_message}")

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
        执行完整的导入过程
        
        返回：
            dict: 包含处理结果的字典
        """
        overall_start = time.time()
        
        # 用于存储各阶段结果
        stage_results = {
            'database_connection': False,
            'excel_analysis': False,
            'table_creation': False,
            'data_import': None,
            'data_validation': None,
            'table_optimization': False
        }

        try:
            logging.info(f"开始将Excel文件 '{self.excel_path}' 导入到DuckDB数据库 '{self.db_path}'，安全模式: {'开启' if self.safe_mode else '关闭'}")

            print("步骤 1/5: 连接数据库...")
            db_connection_success = self.connect_db()
            stage_results['database_connection'] = db_connection_success
            
            if not db_connection_success:
                return {
                    'success': False,
                    'error': "连接数据库失败",
                    'stage_results': stage_results,
                    'total_time': time.time() - overall_start
                }

            print("步骤 2/5: 分析Excel文件结构...")
            excel_analysis_success = self.analyze_excel()
            stage_results['excel_analysis'] = excel_analysis_success
            
            if not excel_analysis_success:
                return {
                    'success': False,
                    'error': "分析Excel文件失败",
                    'stage_results': stage_results,
                    'total_time': time.time() - overall_start
                }

            print("步骤 3/5: 创建数据库表结构...")
            try:
                self.create_tables()
                stage_results['table_creation'] = True
            except Exception as e:
                logging.error(f"创建表结构时出错: {str(e)}")
                logging.error(traceback.format_exc())
                return {
                    'success': False,
                    'error': f"创建表结构失败: {str(e)}",
                    'stage_results': stage_results,
                    'total_time': time.time() - overall_start
                }

            print("步骤 4/5: 导入数据...")
            import_results = self.import_data()
            stage_results['data_import'] = import_results

            # 检查导入结果
            successful_imports = [sheet for sheet, result in import_results.items()
                                if result['import_success']]

            if not successful_imports:
                print("\n警告: 所有工作表导入均失败，跳过验证步骤")
                logging.warning("所有工作表导入均失败，跳过验证步骤")
                # 提前返回错误结果
                return {
                    'success': False,
                    'error': "所有工作表导入失败",
                    'stage_results': stage_results,
                    'import_results': import_results,
                    'total_time': time.time() - overall_start
                }

            print("步骤 5/5: 验证数据完整性...")
            try:
                validation_results = self.validate_import()
                stage_results['data_validation'] = validation_results
            except Exception as e:
                logging.error(f"验证数据时出错: {str(e)}")
                logging.error(traceback.format_exc())
                validation_results = []
                stage_results['data_validation'] = []
                # 继续执行，因为数据可能已经导入成功

            # 输出验证结果摘要
            if validation_results:
                print("\n数据导入验证结果:")
                print("-" * 100)
                print(f"{'工作表':<20} {'数据表':<20} {'Excel行数':<10} {'数据库行数':<10} {'状态':<10} {'消息'}")
                print("-" * 100)

                for result in validation_results:
                    status_emoji = "✅" if result['overall_status'] == 'success' else "⚠️" if result['overall_status'] == 'warning' else "❌"
                    msg = result['messages'][0] if result['messages'] else ""
                    print(
                        f"{result['sheet']:<20} {result['table']:<20} "
                        f"{result['excel_rows']:<10} {result['db_rows']:<10} "
                        f"{status_emoji} {result['overall_status']:<8} {msg}"
                    )

                    # 打印详细消息
                    if len(result['messages']) > 1:
                        for msg in result['messages'][1:]:
                            print(f"{'':<60} - {msg}")

            # 优化表
            if validation_results and any(result['overall_status'] in ('success', 'warning') for result in validation_results):
                print("\n至少有一个表格验证成功，正在优化表结构...")
                try:
                    self.optimize_tables()
                    stage_results['table_optimization'] = True
                except Exception as e:
                    logging.error(f"优化表时出错: {str(e)}")
                    logging.error(traceback.format_exc())
                    # 继续执行，因为这只是优化步骤

            overall_time = time.time() - overall_start
            print(f"\n导入过程完成，总耗时: {overall_time:.2f} 秒")

            logging.info(f"Excel到DuckDB导入过程完成，总耗时: {overall_time:.2f} 秒")

            # 返回处理结果
            return {
                'success': validation_results and any(result['overall_status'] in ('success', 'warning') for result in validation_results),
                'validation_results': validation_results,
                'stage_results': stage_results,
                'import_results': import_results,
                'total_time': overall_time
            }

        except Exception as e:
            logging.error(f"导入过程中出错: {str(e)}")
            logging.error(traceback.format_exc())
            print(f"错误: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'stage_results': stage_results,
                'total_time': time.time() - overall_start
            }

        finally:
            self.close()

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logging.info("数据库连接已关闭")

    def validate_import(self):
        """全面验证数据导入的正确性和完整性"""
        validation_results = []

        for sheet_name, info in self.sheets_info.items():
            table_name = sheet_name.replace(' ', '_').replace('-', '_')
            excel_count = info['row_count']
            excel_sample = self.samples[sheet_name]

            # 创建验证结果对象
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
                # 检查表是否存在
                try:
                    self.conn.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    db_count = self.conn.fetchone()[0]
                except Exception as e:
                    # 尝试检查临时表
                    try:
                        temp_table_name = f"{table_name}_temp"
                        self.conn.execute(f'SELECT COUNT(*) FROM "{temp_table_name}"')
                        db_count = self.conn.fetchone()[0]
                        table_name = temp_table_name  # 后续使用临时表名
                        validation['messages'].append(f"使用临时表 '{temp_table_name}' 进行验证")
                    except Exception as inner_e:
                        validation['messages'].append(f"表 '{table_name}' 和临时表都不存在: {str(e)}")
                        db_count = 0

                validation['db_rows'] = db_count
                validation['row_count_match'] = (excel_count == db_count)

                if not validation['row_count_match']:
                    validation['messages'].append(
                        f"行数不匹配! Excel: {excel_count}, 数据库: {db_count}"
                    )

                # 只有在表存在且有数据时才继续验证
                if db_count > 0:
                    # 2. 列数和列名验证
                    self.conn.execute(f'PRAGMA table_info("{table_name}")')
                    db_columns = self.conn.fetchall()
                    db_column_names = [col[1] for col in db_columns]

                    excel_column_names = [col['clean_name'] for col in info['columns']]
                    validation['column_count_match'] = (len(db_column_names) == len(excel_column_names))

                    missing_columns = set(excel_column_names) - set(db_column_names)
                    if missing_columns:
                        validation['messages'].append(
                            f"缺少列: {missing_columns}"
                        )

                    # 3. 数据类型验证
                    for col in info['columns']:
                        col_name = col['clean_name']
                        if col_name in db_column_names:
                            # 获取数据库中的数据类型
                            db_col_idx = db_column_names.index(col_name)
                            db_col_type = db_columns[db_col_idx][2]

                            # 简化类型比较 (仅比较主要类型)
                            expected_type = col['duck_type'].split('(')[0]
                            actual_type = db_col_type.split('(')[0]

                            # 安全模式下，所有列都是TEXT类型
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
                                    f"列 '{col_name}' 类型不匹配: 预期 {expected_type if self.safe_mode else col['duck_type']}, 实际 {db_col_type}"
                                )

                    # 5. 统计信息验证 (仅当非安全模式时进行)
                    if not self.safe_mode:
                        # 批量查询优化：构建所有统计查询并一次性执行
                        validation_queries = []
                        query_columns = []
                        
                        for col_name, stats in info['column_stats'].items():
                            clean_col_name = self._clean_column_name(col_name)
                            
                            if 'min' in stats and 'max' in stats:
                                # 对数值列进行统计验证
                                validation_queries.append(f'SELECT MIN("{clean_col_name}"), MAX("{clean_col_name}"), AVG("{clean_col_name}"), STDDEV("{clean_col_name}") FROM "{table_name}"')
                                query_columns.append(('numeric', col_name, clean_col_name, stats))
                            elif 'unique_count' in stats:
                                # 对分类列验证唯一值数量
                                validation_queries.append(f'SELECT COUNT(DISTINCT "{clean_col_name}") FROM "{table_name}"')
                                query_columns.append(('categorical', col_name, clean_col_name, stats))
                        
                        # 批量执行查询
                        try:
                            results = []
                            for query in validation_queries:
                                self.conn.execute(query)
                                results.append(self.conn.fetchone())
                            
                            # 处理查询结果
                            for i, (query_type, col_name, clean_col_name, stats) in enumerate(query_columns):
                                if query_type == 'numeric':
                                    try:
                                        db_min, db_max, db_avg, db_stddev = results[i]
                                        
                                        # 比较最小值、最大值和平均值
                                        min_match = abs(stats['min'] - db_min) < 0.0001
                                        max_match = abs(stats['max'] - db_max) < 0.0001
                                        avg_match = abs(stats['mean'] - db_avg) < 0.0001
                                        
                                        # 增强验证：比较标准差（如果Excel样本中有）
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
                                                f"列 '{col_name}' 统计信息不匹配: Excel(Min={stats['min']}, Max={stats['max']}, Avg={stats['mean']}{stddev_info}), "
                                                f"DB(Min={db_min}, Max={db_max}, Avg={db_avg}{db_stddev_info})"
                                            )
                                    except Exception as e:
                                        validation['messages'].append(
                                            f"处理列 '{col_name}' 数值统计结果时出错: {str(e)}"
                                        )
                                        
                                elif query_type == 'categorical':
                                    try:
                                        db_unique = results[i][0]
                                        
                                        # 比较唯一值数量 (允许一定误差)
                                        unique_match = abs(stats['unique_count'] - db_unique) <= max(5, stats['unique_count'] * 0.05)
                                        
                                        validation['stats_verification'].append({
                                            'column': col_name,
                                            'unique_count_match': unique_match,
                                            'status': 'success' if unique_match else 'error'
                                        })
                                        
                                        if not unique_match:
                                            validation['messages'].append(
                                                f"列 '{col_name}' 唯一值数量不匹配: Excel={stats['unique_count']}, DB={db_unique}"
                                            )
                                    except Exception as e:
                                        validation['messages'].append(
                                            f"处理列 '{col_name}' 分类统计结果时出错: {str(e)}"
                                        )
                        except Exception as batch_error:
                            logging.error(f"批量执行统计查询时出错: {str(batch_error)}")
                            logging.error(traceback.format_exc())
                            
                            # 回退到原有的逐个查询方式
                            logging.warning("回退到逐个查询方式进行统计验证")
                            for col_name, stats in info['column_stats'].items():
                                clean_col_name = self._clean_column_name(col_name)
                                
                                if 'min' in stats and 'max' in stats:
                                    # 对数值列进行统计验证
                                    try:
                                        self.conn.execute(f'SELECT MIN("{clean_col_name}"), MAX("{clean_col_name}"), AVG("{clean_col_name}"), STDDEV("{clean_col_name}") FROM "{table_name}"')
                                        result = self.conn.fetchone()
                                        db_min, db_max, db_avg = result[0], result[1], result[2]
                                        db_stddev = result[3] if len(result) > 3 else None
                                        
                                        # 比较最小值、最大值和平均值
                                        min_match = abs(stats['min'] - db_min) < 0.0001
                                        max_match = abs(stats['max'] - db_max) < 0.0001
                                        avg_match = abs(stats['mean'] - db_avg) < 0.0001
                                        
                                        # 增强验证：比较标准差（如果Excel样本中有）
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
                                                f"列 '{col_name}' 统计信息不匹配: Excel(Min={stats['min']}, Max={stats['max']}, Avg={stats['mean']}{stddev_info}), "
                                                f"DB(Min={db_min}, Max={db_max}, Avg={db_avg}{db_stddev_info})"
                                            )
                                    except Exception as e:
                                        validation['messages'].append(
                                            f"验证列 '{col_name}' 统计信息时出错: {str(e)}"
                                        )
                                        
                                elif 'unique_count' in stats:
                                    # 对分类列验证唯一值数量
                                    try:
                                        self.conn.execute(f'SELECT COUNT(DISTINCT "{clean_col_name}") FROM "{table_name}"')
                                        db_unique = self.conn.fetchone()[0]
                                        
                                        # 比较唯一值数量 (允许一定误差)
                                        unique_match = abs(stats['unique_count'] - db_unique) <= max(5, stats['unique_count'] * 0.05)
                                        
                                        validation['stats_verification'].append({
                                            'column': col_name,
                                            'unique_count_match': unique_match,
                                            'status': 'success' if unique_match else 'error'
                                        })
                                        
                                        if not unique_match:
                                            validation['messages'].append(
                                                f"列 '{col_name}' 唯一值数量不匹配: Excel={stats['unique_count']}, DB={db_unique}"
                                            )
                                    except Exception as e:
                                        validation['messages'].append(
                                            f"验证列 '{col_name}' 唯一值时出错: {str(e)}"
                                        )

                # 6. 综合判断验证状态
                if db_count == 0:
                    validation['overall_status'] = 'error'
                    if not validation['messages']:
                        validation['messages'].append("数据库表中无数据")
                elif validation['row_count_match'] and validation['column_count_match'] and \
                   all(dtm['match'] for dtm in validation['data_types_match']):
                    # 安全模式下不检查统计信息匹配
                    if self.safe_mode or all(sv['status'] == 'success' for sv in validation['stats_verification'] if 'status' in sv):
                        validation['overall_status'] = 'success'
                        validation['messages'].append("数据验证成功 (行数、列数、数据类型和统计信息均匹配)")
                    else:
                        validation['overall_status'] = 'warning'  # 统计信息不匹配但主要指标匹配
                        validation['messages'].append("数据基本验证成功，但部分统计信息不匹配")
                else:
                    validation['overall_status'] = 'error'
                    if not validation['messages']:
                        validation['messages'].append("数据验证失败 (行数、列数或数据类型不匹配)")

            except Exception as e:
                validation['overall_status'] = 'error'
                validation['messages'].append(f"验证过程出错: {str(e)}")
                logging.error(f"验证表 '{table_name}' 时出错: {str(e)}")

            validation_results.append(validation)

        return validation_results

    def optimize_tables(self):
        """优化导入的表，添加索引等"""
        for sheet_name, info in self.sheets_info.items():
            table_name = sheet_name.replace(' ', '_').replace('-', '_')

            try:
                # 检查表是否存在
                try:
                    self.conn.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                except Exception as e:
                    logging.warning(f"无法优化表 '{table_name}'，表不存在: {str(e)}")
                    continue

                # 优化表结构
                self.conn.execute(f'ANALYZE "{table_name}"')

                # 如果是安全模式导入的表，可以进行类型转换优化
                if self.safe_mode:
                    logging.info(f"安全模式导入 - 尝试优化表 '{table_name}' 的数据类型")

                    # 遍历每列并尝试自动类型转换
                    for col in info['columns']:
                        col_name = col['clean_name']
                        original_duck_type = col['duck_type']

                        # 尝试转换到合适的类型
                        if original_duck_type != 'TEXT' and not 'VARCHAR' in original_duck_type:
                            try:
                                if original_duck_type in ('INTEGER', 'BIGINT'):
                                    # 先验证数据是否都可以转换为整数
                                    self.conn.execute(f'''
                                        SELECT COUNT(*) FROM "{table_name}" 
                                        WHERE "{col_name}" IS NOT NULL 
                                        AND "{col_name}" <> '' 
                                        AND "{col_name}" NOT SIMILAR TO '-?[0-9]+'
                                    ''')
                                    invalid_count = self.conn.fetchone()[0]
                                    
                                    if invalid_count > 0:
                                        logging.warning(f"列 '{col_name}' 包含 {invalid_count} 行无法转换为整数的数据，保留为TEXT类型")
                                        continue
                                        
                                    # 尝试转换为整数
                                    self.conn.execute(f'''
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col_name}"
                                        TYPE {original_duck_type}
                                        USING CAST("{col_name}" AS {original_duck_type})
                                    ''')
                                    logging.info(f"成功将表 '{table_name}' 的列 '{col_name}' 优化为 {original_duck_type} 类型")
                                elif original_duck_type == 'DOUBLE':
                                    # 先验证数据是否都可以转换为浮点数
                                    self.conn.execute(fr'''
                                        SELECT COUNT(*) FROM "{table_name}" 
                                        WHERE "{col_name}" IS NOT NULL 
                                        AND "{col_name}" <> '' 
                                        AND "{col_name}" NOT SIMILAR TO '-?[0-9]*\.?[0-9]*([eE][-+]?[0-9]+)?'
                                    ''')
                                    invalid_count = self.conn.fetchone()[0]
                                    
                                    if invalid_count > 0:
                                        logging.warning(f"列 '{col_name}' 包含 {invalid_count} 行无法转换为浮点数的数据，保留为TEXT类型")
                                        continue
                                    
                                    # 尝试转换为浮点数
                                    self.conn.execute(f'''
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col_name}"
                                        TYPE DOUBLE
                                        USING CAST("{col_name}" AS DOUBLE)
                                    ''')
                                    logging.info(f"成功将表 '{table_name}' 的列 '{col_name}' 优化为 DOUBLE 类型")
                                elif original_duck_type in ('DATE', 'TIMESTAMP'):
                                    # 先验证数据是否都可以转换为日期/时间
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
                                        logging.warning(f"列 '{col_name}' 包含 {invalid_count} 行无法转换为{original_duck_type}的数据，保留为TEXT类型")
                                        continue
                                    
                                    # 尝试转换为日期
                                    self.conn.execute(f'''
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col_name}"
                                        TYPE {original_duck_type}
                                        USING CAST("{col_name}" AS {original_duck_type})
                                    ''')
                                    logging.info(f"成功将表 '{table_name}' 的列 '{col_name}' 优化为 {original_duck_type} 类型")
                            except Exception as e:
                                logging.warning(f"无法将表 '{table_name}' 的列 '{col_name}' 转换为 {original_duck_type}: {str(e)}")

                # 创建可能的索引
                for col in info['columns']:
                    col_name = col['clean_name']
                    col_stats = info['column_stats'].get(col['original_name'], {})

                    # 如果列有少量唯一值且不是主键列，考虑添加索引
                    if 'unique_count' in col_stats and \
                       col_stats['unique_count'] > 1 and \
                       col_stats['unique_count'] < 1000 and \
                       col_stats['unique_count'] < info['row_count'] / 10:

                        index_name = f"idx_{table_name}_{col_name}"
                        try:
                            self.conn.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ("{col_name}")')
                            logging.info(f"为表 '{table_name}' 的列 '{col_name}' 创建索引")
                        except Exception as e:
                            logging.warning(f"无法为表 '{table_name}' 的列 '{col_name}' 创建索引: {str(e)}")

                logging.info(f"表 '{table_name}' 优化完成")

            except Exception as e:
                logging.error(f"优化表 '{table_name}' 时出错: {str(e)}")

    def _reservoir_sample(self, sheet_name, total_rows):
        """
        使用蓄水池抽样算法从Excel获取样本数据
        
        蓄水池抽样算法说明：
        1. 蓄水池抽样是一种在不知道总体大小的情况下，从流式数据中随机抽取固定数量样本的算法
        2. 算法保证每个元素被抽取的概率相等，无论数据量多大
        3. 实现步骤：
           - 先将前k个元素放入"蓄水池"
           - 对于第i个元素(i>k)，以k/i的概率决定是否替换蓄水池中的随机元素
           - 这样保证了每个元素最终被选中的概率都是k/n
        4. 本实现中，为了避免一次性加载整个Excel，使用批处理方式读取数据
        
        参数：
            sheet_name (str): 工作表名称
            total_rows (int): 工作表总行数
            
        返回：
            pandas.DataFrame: 随机抽取的样本数据
        """
        sample_size = min(self.sample_size, total_rows)
        
        # 使用默认引擎
        excel_engine = None

        if total_rows <= sample_size:
            # 如果总行数小于样本大小，直接读取所有行
            excel_params = self._build_excel_params(sheet_name, excel_engine)
            return pd.read_excel(self.excel_path, **excel_params)

        # 蓄水池抽样 - 读取所有数据但只保留随机样本
        reservoir = []

        # 批量读取Excel文件
        batch_size = RESERVOIR_BATCH_SIZE
        batches = (total_rows + batch_size - 1) // batch_size

        for i in range(batches):
            start_row = i * batch_size

            # 如果是最后一个批次，调整nrows
            if i == batches - 1:
                nrows = total_rows - (i * batch_size)
            else:
                nrows = batch_size

            try:
                # 构建参数字典，只在excel_engine不为None时添加engine参数
                excel_params = self._build_excel_params(sheet_name, excel_engine, skiprows=range(1, start_row + 1) if start_row > 0 else None, nrows=nrows)
                
                batch = pd.read_excel(
                    self.excel_path, 
                    **excel_params
                )
                
                # 处理此批次的每一行
                for j in range(len(batch)):
                    # 当前处理的是整个数据集中的第(start_row + j)行
                    current_row_index = start_row + j
                    
                    if len(reservoir) < sample_size:
                        # 阶段1: 直接填充蓄水池直到达到样本大小
                        reservoir.append(batch.iloc[j])
                    else:
                        # 阶段2: 以递减概率替换现有样本
                        # 对于第i个元素，以k/i的概率决定是否替换蓄水池中的随机元素
                        # 这里k是sample_size，i是current_row_index
                        r = random.randint(0, current_row_index)
                        if r < sample_size:
                            reservoir[r] = batch.iloc[j]
            except Exception as e:
                logging.warning(f"从工作表 '{sheet_name}' 收集样本时批次 {i+1}/{batches} 出错: {str(e)}")
                # 继续处理其他批次

        return pd.DataFrame(reservoir)

    def _compute_sample_hash(self, df):
        """
        计算样本数据的哈希值，用于稍后的验证
        
        函数说明：
        1. 此函数用于生成数据样本的唯一标识符（哈希值）
        2. 主要用途：
           - 在导入前后验证数据的一致性
           - 检测数据在处理过程中是否发生变化
           - 作为数据完整性验证的一部分
        3. 实现方法：
           - 将DataFrame转换为CSV字符串格式
           - 使用MD5算法计算字符串的哈希值
           - 返回16进制格式的哈希字符串
        
        参数：
            df (pandas.DataFrame): 要计算哈希值的数据样本
            
        返回：
            str: 计算得到的MD5哈希值，或在计算失败时返回"hash_error"
        """
        # 将DataFrame转换为字符串并计算哈希
        try:
            sample_str = df.to_csv(index=False)
            return hashlib.md5(sample_str.encode()).hexdigest()
        except Exception as e:
            logging.warning(f"计算样本哈希值时出错: {str(e)}")
            return "hash_error"

    def _clean_column_name(self, column_name):
        """
        清理列名，移除非法字符并确保名称符合DuckDB要求
        
        函数说明：
        1. 此函数用于处理Excel中的列名，使其符合DuckDB的命名规范
        2. 主要清理操作包括：
           - 将非字符串类型的列名转换为字符串
           - 替换空格和连字符为下划线
           - 移除所有非字母数字和下划线的字符
           - 确保列名不以数字开头（DuckDB要求）
           - 处理空列名的情况
        3. 列名清理的重要性：
           - 避免SQL语法错误
           - 确保列名在数据库中的一致性
           - 简化后续的数据查询和处理
        
        参数：
            column_name: 原始列名，可以是任何类型
            
        返回：
            str: 清理后的列名，符合DuckDB命名规范
        """
        if not isinstance(column_name, str):
            column_name = str(column_name)

        # 替换空格和特殊字符
        clean_name = column_name.replace(' ', '_').replace('-', '_')

        # 移除其他非法字符
        clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')

        # 确保不以数字开头
        if clean_name and clean_name[0].isdigit():
            clean_name = 'col_' + clean_name

        # 避免列名为空
        if not clean_name:
            clean_name = 'column'

        return clean_name

    def _build_excel_params(self, sheet_name, excel_engine=None, **kwargs):
        """
        构建Excel参数字典，用于pandas读取Excel文件
        
        函数说明：
        1. 此函数用于统一构建pandas读取Excel文件时所需的参数字典
        2. 主要功能：
           - 设置基本参数如工作表名称
           - 根据需要添加引擎参数（如openpyxl、xlrd等）
           - 处理其他可选参数（如行范围、列范围等）
        3. 使用此函数的好处：
           - 减少代码重复
           - 确保参数设置的一致性
           - 简化参数管理，特别是在多处需要读取Excel的情况下
        
        参数：
            sheet_name (str): 工作表名称
            excel_engine (str, optional): Excel引擎名称，如'openpyxl'或'xlrd'
            **kwargs: 其他pandas.read_excel支持的参数，常用的包括：
                      - skiprows: 跳过的行数或行索引列表
                      - nrows: 读取的最大行数
                      - usecols: 要读取的列索引或名称
                      - header: 标题行的位置
            
        返回：
            dict: 构建好的参数字典，可直接用于pandas.read_excel函数
        """
        # 创建基本参数字典
        params = {'sheet_name': sheet_name}
        
        # 添加其他参数
        for key, value in kwargs.items():
            if value is not None:  # 只添加非None的参数
                params[key] = value
                
        # 只在excel_engine不为None时添加engine参数
        if excel_engine is not None:
            params['engine'] = excel_engine
            
        return params
        
    def _validate_parameter(self, param_name, param_value, param_type, min_value=None, max_value=None, default_value=None):
        """
        验证参数的类型和范围，确保参数符合预期
        
        参数验证算法说明：
        1. 参数验证的目标是确保用户提供的参数符合预期的类型和范围要求
        2. 验证过程包括以下步骤：
           - 检查参数是否为None，如果是则使用默认值
           - 验证参数类型是否符合预期，如不符合则尝试转换
           - 对于数值类型，验证是否在指定的最小值和最大值范围内
           - 如果验证失败，记录警告并使用默认值
        3. 验证结果：
           - 如果验证成功，返回验证后的参数值
           - 如果验证失败且有默认值，返回默认值
           - 如果验证失败且无默认值，抛出ValueError异常
        
        参数：
            param_name (str): 参数名称，用于日志记录
            param_value: 要验证的参数值
            param_type: 预期的参数类型（如int, str等）
            min_value: 可选，参数的最小允许值
            max_value: 可选，参数的最大允许值
            default_value: 可选，如果验证失败时使用的默认值
            
        返回：
            验证后的参数值或默认值
            
        异常：
            ValueError: 如果验证失败且没有提供默认值
        """
        # 如果参数为None且有默认值，使用默认值
        if param_value is None and default_value is not None:
            logging.info(f"参数 '{param_name}' 为None，使用默认值: {default_value}")
            return default_value
            
        # 验证参数类型
        if not isinstance(param_value, param_type):
            try:
                # 尝试转换类型
                param_value = param_type(param_value)
                logging.info(f"参数 '{param_name}' 类型已从 {type(param_value).__name__} 转换为 {param_type.__name__}")
            except (ValueError, TypeError) as e:
                if default_value is not None:
                    logging.warning(f"参数 '{param_name}' 类型错误，无法转换为 {param_type.__name__}，使用默认值 {default_value}: {str(e)}")
                    return default_value
                else:
                    raise ValueError(f"参数 '{param_name}' 类型错误，应为 {param_type.__name__}: {str(e)}")
        
        # 验证数值范围（仅适用于数值类型）
        if isinstance(param_value, (int, float)):
            if min_value is not None and param_value < min_value:
                if default_value is not None:
                    logging.warning(f"参数 '{param_name}' 值 {param_value} 小于最小值 {min_value}，使用默认值 {default_value}")
                    return default_value
                else:
                    raise ValueError(f"参数 '{param_name}' 值 {param_value} 小于最小值 {min_value}")
                    
            if max_value is not None and param_value > max_value:
                if default_value is not None:
                    logging.warning(f"参数 '{param_name}' 值 {param_value} 大于最大值 {max_value}，使用默认值 {default_value}")
                    return default_value
                else:
                    raise ValueError(f"参数 '{param_name}' 值 {param_value} 大于最大值 {max_value}")
        
        return param_value

    def _map_dtype_to_duck(self, pandas_dtype, series):
        """
        将pandas数据类型映射到DuckDB数据类型，并进行智能类型推断
        
        类型映射算法说明：
        1. 类型映射的目标是将pandas的数据类型转换为最合适的DuckDB数据类型
        2. 映射过程遵循以下规则：
           - 安全模式下，所有列都使用TEXT类型，避免类型转换错误
           - 对于数值类型，区分INTEGER和BIGINT，根据数值范围选择合适类型
           - 对于浮点类型，统一使用DOUBLE类型
           - 对于日期时间类型，根据具体类型映射为DATE、TIMESTAMP或TIME
           - 对于布尔类型，映射为BOOLEAN
           - 对于对象和字符串类型，使用TEXT类型作为最安全的选择
        3. 额外的安全检查：
           - 对于数值类型，检查样本中是否含有非数值内容，如有则改用TEXT类型
        
        参数：
            pandas_dtype (str): pandas数据类型字符串
            series (pandas.Series): 列数据
            
        返回：
            str: 映射后的DuckDB数据类型
        """
        # 安全模式下，所有列使用TEXT类型
        if self.safe_mode:
            return 'TEXT'

        # 检查是否有任何非数值字符串
        if 'int' in pandas_dtype or 'float' in pandas_dtype:
            # 额外检查 - 确保数值列中没有字符串值
            try:
                # 检查样本中是否含有非数值内容
                sample = series.dropna().head(100)
                for val in sample:
                    if isinstance(val, str):
                        logging.warning(f"列 {series.name} 数值类型中包含字符串值 '{val}'，改用TEXT类型")
                        return 'TEXT'
            except:
                # 如果检查失败，使用更保守的TEXT类型
                return 'TEXT'

        # 处理常见数值类型
        if 'int' in pandas_dtype:
            if series.max() > 2147483647 or series.min() < -2147483648:
                return 'BIGINT'
            return 'INTEGER'

        elif 'float' in pandas_dtype:
            return 'DOUBLE'

        # 处理日期和时间类型
        elif 'datetime' in pandas_dtype:
            return 'TIMESTAMP'

        elif 'date' in pandas_dtype:
            return 'DATE'

        elif 'time' in pandas_dtype:
            return 'TIME'

        # 处理布尔类型
        elif 'bool' in pandas_dtype:
            return 'BOOLEAN'

        # 处理字符串和对象类型 - 更保守的推断
        elif 'object' in pandas_dtype or 'string' in pandas_dtype:
            # 对于对象类型，全部使用TEXT类型，最安全
            return 'TEXT'

        # 默认类型
        return 'TEXT'

    def create_tables(self):
        """
        基于Excel结构创建DuckDB表
        
        返回：
            bool: 创建是否成功
            
        异常：
            不会抛出异常，而是返回False并记录错误
        """
        try:
            # 检查是否已连接到数据库
            if not self.is_connected or self.conn is None:
                logging.error("尝试创建表时数据库未连接")
                return False
                
            # 检查是否有工作表信息
            if not self.sheets_info:
                logging.error("没有可用的工作表信息来创建表")
                return False
                
            start_time = time.time()

            for sheet_name, info in self.sheets_info.items():
                # 检查列信息是否存在
                if 'columns' not in info or not info['columns']:
                    logging.warning(f"工作表 '{sheet_name}' 没有列信息，跳过创建表")
                    continue
                    
                # 创建表名 - 替换空格和特殊字符
                table_name = sheet_name.replace(' ', '_').replace('-', '_')

                # 创建临时表名用于导入和验证
                temp_table_name = f"{table_name}_temp"

                # 构建CREATE TABLE语句
                columns_def = []
                for col in info['columns']:
                    # 如果启用安全模式，所有列都使用TEXT类型
                    if self.safe_mode:
                        data_type = 'TEXT'
                    else:
                        data_type = col["duck_type"]

                    columns_def.append(f'"{col["clean_name"]}" {data_type}')

                # 检查是否有列定义
                if not columns_def:
                    logging.warning(f"工作表 '{sheet_name}' 没有有效的列定义，跳过创建表")
                    continue

                try:
                    # 删除临时表（如果存在）
                    self.conn.execute(f'DROP TABLE IF EXISTS "{temp_table_name}"')

                    # 创建临时表
                    create_stmt = f'CREATE TABLE "{temp_table_name}" (\n'
                    create_stmt += ',\n'.join(columns_def)
                    create_stmt += '\n)'

                    logging.info(f"为工作表 '{sheet_name}' 创建临时表 '{temp_table_name}'，{'使用安全模式' if self.safe_mode else '使用推断类型'}")
                    logging.debug(create_stmt)

                    self.conn.execute(create_stmt)
                except Exception as table_error:
                    logging.error(f"创建表 '{temp_table_name}' 时出错: {str(table_error)}")
                    logging.error(traceback.format_exc())
                    # 继续处理其他表

            elapsed_time = time.time() - start_time
            logging.info(f"表结构创建完成，耗时 {elapsed_time:.2f} 秒")
            return True

        except Exception as e:
            logging.error(f"创建表时出错: {str(e)}")
            logging.error(traceback.format_exc())
            return False

# 使用示例
if __name__ == "__main__":
    excel_file = input("请输入Excel文件路径: ")
    db_file = input("请输入DuckDB数据库文件路径(如果不存在将创建): ")

    # 默认使用安全模式
    converter = ExcelToDuckDB(excel_file, db_file, safe_mode=True)
    result = converter.run()

    if result['success']:
        print("\n数据已成功导入DuckDB，可以开始进行分析!")
        print("示例查询:")
        print("-----------------------------------------------------")
        conn = duckdb.connect(db_file)

        # 获取表名
        tables = conn.execute("SHOW TABLES").fetchall()
        if tables:
            table_name = tables[0][0]
            print(f"-- 查看表 {table_name} 的前5行:")
            print(f"SELECT * FROM \"{table_name}\" LIMIT 5;")

            print(f"\n-- 查看表 {table_name} 的统计信息:")
            print(f"SUMMARIZE \"{table_name}\";")

        conn.close()
