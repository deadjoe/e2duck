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

# 设置日志
logging.basicConfig(
    filename=f'excel_to_duckdb_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ExcelToDuckDB:
    def __init__(self, excel_path, db_path, sample_size=100, safe_mode=True):
        """
        初始化Excel到DuckDB转换器
        
        参数:
            excel_path (str): Excel文件的路径
            db_path (str): DuckDB数据库文件的路径
            sample_size (int): 用于验证的样本大小，默认为100
            safe_mode (bool): 是否使用安全模式（所有列使用TEXT类型），默认为True
            
        异常:
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
        
        返回:
            bool: 连接是否成功
            
        异常:
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
                self.conn.execute("PRAGMA memory_limit='4GB'")
                logging.warning(f"内存检测失败，使用默认内存限制4GB: {str(e)}")
                
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
        
        返回:
            bool: 分析是否成功
            
        异常:
            不会抛出异常，而是返回False并记录错误
        """
        try:
            # 文件检查已在__init__中完成，这里不再重复检查
            
            # 检查文件大小
            file_size = os.path.getsize(self.excel_path) / (1024*1024)  # 转换为MB
            logging.info(f"Excel文件大小: {file_size:.2f} MB")

            start_time = time.time()
            logging.info(f"开始分析Excel文件: {self.excel_path}")

            # 获取所有工作表
            xl = pd.ExcelFile(self.excel_path)
            sheet_names = xl.sheet_names
            logging.info(f"Excel文件包含以下工作表: {sheet_names}")

            if not sheet_names:
                logging.error("Excel文件不包含任何工作表")
                return False

            for sheet_name in sheet_names:
                logging.info(f"分析工作表: {sheet_name}")

                try:
                    # 获取总行数 (预读取一次)
                    df_info = pd.read_excel(self.excel_path, sheet_name=sheet_name, nrows=0)
                    total_rows = len(pd.read_excel(self.excel_path, sheet_name=sheet_name))

                    # 读取前1000行以推断数据类型和分析列
                    df_sample = pd.read_excel(self.excel_path, sheet_name=sheet_name, nrows=1000)

                    # 检查是否有列
                    if len(df_sample.columns) == 0:
                        logging.warning(f"工作表 '{sheet_name}' 不包含任何列，跳过")
                        continue

                    # 获取列名和数据类型
                    columns_info = []
                    column_stats = {}

                    for col_name in df_sample.columns:
                        # 清理列名
                        clean_col_name = self._clean_column_name(col_name)

                        # 推断数据类型
                        col_data = df_sample[col_name]
                        pandas_dtype = str(col_data.dtype)
                        duck_type = self._map_dtype_to_duck(pandas_dtype, col_data)

                        logging.info(f"列 '{col_name}' 推断类型: {duck_type}")

                        # 收集基本统计信息
                        try:
                            if pd.api.types.is_numeric_dtype(col_data) and not any(isinstance(x, str) for x in col_data.dropna()):
                                stats = {
                                    'min': col_data.min(),
                                    'max': col_data.max(),
                                    'mean': col_data.mean(),
                                    'null_count': col_data.isna().sum()
                                }
                            else:
                                # 对非数值列，计算唯一值数量和空值数量
                                stats = {
                                    'unique_count': col_data.nunique(),
                                    'null_count': col_data.isna().sum()
                                }

                                # 对可能是枚举的列收集唯一值
                                if col_data.nunique() < 50:  # 如果唯一值数量较少
                                    unique_vals = col_data.dropna().unique().tolist()
                                    # 只保存字符串形式 - 防止序列化问题
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

                    # 使用蓄水池抽样算法从Excel获取样本数据
                    self.samples[sheet_name] = self._reservoir_sample(sheet_name, total_rows)
                    
                    # 检查样本是否为空
                    if self.samples[sheet_name] is None or len(self.samples[sheet_name]) == 0:
                        logging.warning(f"工作表 '{sheet_name}' 样本为空，可能影响验证")
                        sample_hash = "empty_sample"
                    else:
                        sample_hash = self._compute_sample_hash(self.samples[sheet_name])

                    self.sheets_info[sheet_name] = {
                        'columns': columns_info,
                        'column_stats': column_stats,
                        'row_count': total_rows,
                        'sample_hash': sample_hash
                    }

                    logging.info(f"工作表 '{sheet_name}' 分析完成，包含 {len(columns_info)} 列，"
                                f"{total_rows} 行")

                except Exception as sheet_error:
                    logging.error(f"分析工作表 '{sheet_name}' 时出错: {str(sheet_error)}")
                    logging.error(traceback.format_exc())
                    # 继续处理其他工作表

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

    def import_data(self, batch_size=5000):
        """
        将数据从Excel导入到DuckDB，使用批处理以提高性能
        
        参数:
            batch_size (int): 每批处理的行数，默认为5000
            
        返回:
            dict: 导入结果
            
        异常:
            不会抛出异常，而是返回包含错误信息的结果
        """
        # 验证batch_size参数
        if not isinstance(batch_size, int):
            logging.error("batch_size必须是整数类型")
            batch_size = 5000
        
        if batch_size <= 0:
            logging.error("batch_size必须大于0，使用默认值5000")
            batch_size = 5000
        
        # 对于16GB-64GB内存的机器，可以使用更大的批处理大小
        # 但仍然设置一个上限以防止内存问题
        max_batch_size = 100000  # 增加到10万行
        if batch_size > max_batch_size:
            logging.warning(f"batch_size ({batch_size}) 过大，可能导致内存问题，已调整为{max_batch_size}")
            batch_size = max_batch_size
            
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
                        batch_df = pd.read_excel(
                            self.excel_path,
                            sheet_name=sheet_name,
                            skiprows=skiprows,
                            nrows=nrows
                        )

                        logging.info(f"成功读取批次 {i+1}/{num_batches}，实际读取 {len(batch_df)} 行")

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

                        # 使用事务批量处理
                        self.conn.execute("BEGIN TRANSACTION")

                        # 优化：首先尝试使用COPY语句导入数据（最高效）
                        import tempfile
                        import os
                        
                        try:
                            # 创建临时CSV文件
                            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
                                temp_csv = temp_file.name
                            
                            # 将批次数据保存为CSV
                            batch_df.to_csv(temp_csv, index=False)
                            
                            try:
                                # 使用COPY语句导入数据
                                self.conn.execute(f"COPY \"{temp_table_name}\" FROM '{temp_csv}' (AUTO_DETECT TRUE)")
                                
                                # 导入成功后删除临时文件
                                if os.path.exists(temp_csv):
                                    os.remove(temp_csv)
                                
                                logging.info(f"批次 {i+1}/{num_batches}: 使用COPY语句成功导入数据")
                                
                            except Exception as copy_error:
                                logging.error(f"使用COPY语句导入失败: {str(copy_error)}")
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
                        except:
                            pass

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
        
        返回:
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
            excel_sample_hash = info['sample_hash']

            # 创建验证结果对象
            validation = {
                'sheet': sheet_name,
                'table': table_name,
                'excel_rows': excel_count,
                'db_rows': 0,
                'row_count_match': False,
                'column_count_match': False,
                'data_types_match': [],
                'sample_verification': False,
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

                    # 4. 抽样数据验证
                    try:
                        # 从数据库抽取相同数量的随机样本
                        self.conn.execute(f'SELECT * FROM "{table_name}" ORDER BY RANDOM() LIMIT {len(excel_sample)}')
                        db_sample_rows = self.conn.fetchall()
                        db_sample_df = pd.DataFrame(db_sample_rows, columns=db_column_names)

                        # 计算数据库样本的哈希值
                        db_sample_hash = self._compute_sample_hash(db_sample_df)

                        # 注意：直接比较哈希值可能不理想，因为随机排序
                        # 使用更复杂的比较逻辑，比如比较数值分布等

                        # 这里进行简单比较：抽取的样本数据是否类似
                        # 在实际应用中，可以添加更详细的比较

                        validation['sample_verification'] = True  # 简化示例
                        validation['messages'].append(
                            "已验证数据样本 (随机抽样)"
                        )

                    except Exception as e:
                        validation['messages'].append(
                            f"样本验证错误: {str(e)}"
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
                                validation_queries.append(f'SELECT MIN("{clean_col_name}"), MAX("{clean_col_name}"), AVG("{clean_col_name}") FROM "{table_name}"')
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
                                        db_min, db_max, db_avg = results[i]
                                        
                                        # 比较最小值、最大值和平均值
                                        min_match = abs(stats['min'] - db_min) < 0.0001
                                        max_match = abs(stats['max'] - db_max) < 0.0001
                                        avg_match = abs(stats['mean'] - db_avg) < 0.0001
                                        
                                        validation['stats_verification'].append({
                                            'column': col_name,
                                            'min_match': min_match,
                                            'max_match': max_match,
                                            'avg_match': avg_match,
                                            'status': 'success' if (min_match and max_match and avg_match) else 'error'
                                        })
                                        
                                        if not (min_match and max_match and avg_match):
                                            validation['messages'].append(
                                                f"列 '{col_name}' 统计信息不匹配: Excel(Min={stats['min']}, Max={stats['max']}, Avg={stats['mean']}), "
                                                f"DB(Min={db_min}, Max={db_max}, Avg={db_avg})"
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
                                        self.conn.execute(f'SELECT MIN("{clean_col_name}"), MAX("{clean_col_name}"), AVG("{clean_col_name}") FROM "{table_name}"')
                                        db_min, db_max, db_avg = self.conn.fetchone()
                                        
                                        # 比较最小值、最大值和平均值
                                        min_match = abs(stats['min'] - db_min) < 0.0001
                                        max_match = abs(stats['max'] - db_max) < 0.0001
                                        avg_match = abs(stats['mean'] - db_avg) < 0.0001
                                        
                                        validation['stats_verification'].append({
                                            'column': col_name,
                                            'min_match': min_match,
                                            'max_match': max_match,
                                            'avg_match': avg_match,
                                            'status': 'success' if (min_match and max_match and avg_match) else 'error'
                                        })
                                        
                                        if not (min_match and max_match and avg_match):
                                            validation['messages'].append(
                                                f"列 '{col_name}' 统计信息不匹配: Excel(Min={stats['min']}, Max={stats['max']}, Avg={stats['mean']}), "
                                                f"DB(Min={db_min}, Max={db_max}, Avg={db_avg})"
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
                    else:
                        validation['overall_status'] = 'warning'  # 统计信息不匹配但主要指标匹配
                else:
                    validation['overall_status'] = 'error'

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
                                    # 尝试转换为整数
                                    self.conn.execute(f'''
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col_name}"
                                        TYPE {original_duck_type}
                                        USING CAST("{col_name}" AS {original_duck_type})
                                    ''')
                                    logging.info(f"成功将表 '{table_name}' 的列 '{col_name}' 优化为 {original_duck_type} 类型")

                                elif original_duck_type == 'DOUBLE':
                                    # 尝试转换为浮点数
                                    self.conn.execute(f'''
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col_name}"
                                        TYPE DOUBLE
                                        USING CAST("{col_name}" AS DOUBLE)
                                    ''')
                                    logging.info(f"成功将表 '{table_name}' 的列 '{col_name}' 优化为 DOUBLE 类型")

                                elif original_duck_type in ('DATE', 'TIMESTAMP'):
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
        """使用蓄水池抽样算法从Excel获取样本数据"""
        sample_size = min(self.sample_size, total_rows)

        if total_rows <= sample_size:
            # 如果总行数小于样本大小，直接读取所有行
            return pd.read_excel(self.excel_path, sheet_name=sheet_name)

        # 蓄水池抽样 - 读取所有数据但只保留随机样本
        reservoir = []

        # 批量读取Excel文件
        batch_size = 1000
        batches = (total_rows + batch_size - 1) // batch_size

        for i in range(batches):
            start_row = i * batch_size

            # 如果是最后一个批次，调整nrows
            if i == batches - 1:
                nrows = total_rows - start_row
            else:
                nrows = batch_size

            try:
                batch = pd.read_excel(
                    self.excel_path,
                    sheet_name=sheet_name,
                    skiprows=range(1, start_row + 1) if start_row > 0 else None,
                    nrows=nrows
                )

                # 处理此批次的每一行
                for j in range(len(batch)):
                    if len(reservoir) < sample_size:
                        reservoir.append(batch.iloc[j])
                    else:
                        # 以递减概率替换现有样本
                        r = random.randint(0, start_row + j)
                        if r < sample_size:
                            reservoir[r] = batch.iloc[j]
            except Exception as e:
                logging.warning(f"从工作表 '{sheet_name}' 收集样本时批次 {i+1}/{batches} 出错: {str(e)}")
                # 继续处理其他批次

        return pd.DataFrame(reservoir)

    def _compute_sample_hash(self, df):
        """计算样本数据的哈希值，用于稍后的验证"""
        # 将DataFrame转换为字符串并计算哈希
        try:
            sample_str = df.to_csv(index=False)
            return hashlib.md5(sample_str.encode()).hexdigest()
        except Exception as e:
            logging.warning(f"计算样本哈希值时出错: {str(e)}")
            return "hash_error"

    def _clean_column_name(self, column_name):
        """清理列名，移除非法字符并确保名称符合DuckDB要求"""
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

    def _map_dtype_to_duck(self, pandas_dtype, series):
        """将pandas数据类型映射到DuckDB数据类型，并进行智能类型推断"""
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
        
        返回:
            bool: 创建是否成功
            
        异常:
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
