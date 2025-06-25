import duckdb
import time
import re
import os
import requests
import json

'''
每次重新部署后, 需要运行
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' scheduler0
来查看scheduler0的ip, 并赋值在下面, 更新action, 才能正确更新working状态
'''
# 从文件读取scheduler0的IP地址

with open('/db/dockerIP.json', 'r') as f:
    data = json.load(f)
    scheduler0_ip = data.get("scheduler0IP")
print(f"scheduler0_ip: {scheduler0_ip}")


def safe_connect_duckdb(db_path):
    """安全连接DuckDB，处理空文件和损坏的数据库文件"""
    
    # 检查文件状态
    if os.path.exists(db_path):
        file_size = os.path.getsize(db_path)
        print(f"Database file exists, size: {file_size} bytes")
        
        # 如果是空文件，直接删除让DuckDB重新创建
        if file_size == 0:
            print("Removing empty database file to allow DuckDB to create new one")
            os.remove(db_path)
    else:
        print(f"Database file does not exist, will be created: {db_path}")
    
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # 连接数据库（如果不存在会自动创建）
            conn = duckdb.connect(db_path)
            
            # 测试连接是否有效，同时初始化数据库
            conn.execute("SELECT 1").fetchone()
            
            # 检查数据库是否正常工作
            conn.execute("CREATE TABLE IF NOT EXISTS _test_table (id INTEGER)")
            conn.execute("DROP TABLE IF EXISTS _test_table")
            
            print(f"Successfully connected to database: {db_path}")
            return conn
            
        except duckdb.IOException as e:
            print(f"Database connection attempt {attempt + 1} failed: {e}")
            
            if "not a valid DuckDB database file" in str(e) or "database is locked" in str(e).lower():
                # 数据库文件问题，尝试重新创建
                try:
                    if os.path.exists(db_path):
                        # 创建备份文件名（添加时间戳避免冲突）
                        backup_path = f"{db_path}.backup_{int(time.time())}"
                        os.rename(db_path, backup_path)
                        print(f"Moved problematic database to: {backup_path}")
                    
                    # 重新创建数据库
                    conn = duckdb.connect(db_path)
                    conn.execute("SELECT 1").fetchone()  # 验证连接
                    print(f"Created new database: {db_path}")
                    return conn
                    
                except Exception as cleanup_error:
                    print(f"Failed to cleanup database: {cleanup_error}")
                    if attempt == max_retries - 1:
                        raise
            
            elif attempt == max_retries - 1:
                # 最后一次尝试失败
                raise
            
            # 等待一小段时间再重试
            time.sleep(1)
        
        except Exception as e:
            print(f"Unexpected database connection error: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(1)
    
    raise Exception("Failed to connect to database after multiple attempts")


def main(args):
    data_file = args.get("data_file")
    # data_path = f"/db/remote_db/data/{data_file}/parquet/"
    data_path = f"/db/local_data/{data_file}/parquet/"
    print(f"data_path: {data_path}")
    db_file = args.get("db_file")
    db_path = f"/db/{db_file}"
    
    # 确保数据库文件有合适的扩展名
    if not db_path.endswith(('.db', '.duckdb', '.sqlite')):
        # 如果没有扩展名，保持原名但添加后缀用于标识
        print(f"Database file without standard extension: {db_path}")
    
    print(f"db_path: {db_path}")
    
    # 检查数据库文件状态
    if os.path.exists(db_path):
        file_size = os.path.getsize(db_path)
        print(f"Database file status - exists: True, size: {file_size} bytes")
    else:
        print(f"Database file status - exists: False, will be created")
    
    # 安全处理predict_time参数
    predict_time_value = args.get("predict_time")
    if predict_time_value is not None:
        try:
            predict_time = float(predict_time_value)
        except (ValueError, TypeError):
            predict_time = 0.0  # 默认值
            print(f"Warning: Invalid predict_time value '{predict_time_value}', using default 0.0")
    else:
        predict_time = 0.0  # 默认值
        print("Warning: predict_time parameter not provided, using default 0.0")
    
    print(f"predict_time:{predict_time}, type:{type(predict_time)}")
    
    # 内部设定结果限制，避免返回过大数据集
    max_results = 5  # 限制返回前100行结果
    print(f"max_results: {max_results}")
    
    sql_file = args.get("sql_file")
    print(f"sql_file: {sql_file}")
    sql_query_provided = args.get("sql_query")

    # 确定SQL语句的来源
    if sql_query_provided:
        sql_query = sql_query_provided
        sql_source = "sql_query"
    elif sql_file:
        sql_query_path = f"/sql/{sql_file}"
        try:
            with open(sql_query_path, "r") as f:
                sql_query = f.read()
                # print(f"sql_query: {sql_query}")
            sql_source = "sql_file"
        except FileNotFoundError:
            return {"error": f"SQL文件未找到: {sql_query_path}"}
        except Exception as e:
            return {"error": str(e)}
    else:
        return {"error": "未提供sql_file或sql_query参数"}

    # 提取query涉及的表名
    def extract_table_names(sql):
        table_names = []
        tpch_tables = [
            "nation",
            "region",
            "part",
            "supplier",
            "partsupp",
            "customer",
            "orders",
            "lineitem",
        ]
        for table in tpch_tables:
            pattern = re.compile(r"\b" + re.escape(table) + r"\b", re.IGNORECASE)
            if pattern.search(sql):
                table_names.append(table)
        print(f"table_names: {table_names}")
        return table_names

    # 读取建表语句
    def read_table_schema(table_name):
        schema_file = os.path.join("/sql/tpch_init_sqls", f"{table_name}.sql")
        if os.path.exists(schema_file):
            with open(schema_file, "r") as file:
                return file.read()
        else:
            return None

    # TPC-H表的标准定义（无外键约束版本）
    def get_tpch_schema(table_name):
        """获取TPC-H表的标准定义"""
        schemas = {
            "nation": """
            CREATE TABLE nation (
                n_nationkey INTEGER NOT NULL,
                n_name CHAR(25) NOT NULL,
                n_regionkey INTEGER NOT NULL,
                n_comment VARCHAR(152)
            );
            """,
            "region": """
            CREATE TABLE region (
                r_regionkey INTEGER NOT NULL,
                r_name CHAR(25) NOT NULL,
                r_comment VARCHAR(152)
            );
            """,
            "part": """
            CREATE TABLE part (
                p_partkey INTEGER NOT NULL,
                p_name VARCHAR(55) NOT NULL,
                p_mfgr CHAR(25) NOT NULL,
                p_brand CHAR(10) NOT NULL,
                p_type VARCHAR(25) NOT NULL,
                p_size INTEGER NOT NULL,
                p_container CHAR(10) NOT NULL,
                p_retailprice DECIMAL(15,2) NOT NULL,
                p_comment VARCHAR(23) NOT NULL
            );
            """,
            "supplier": """
            CREATE TABLE supplier (
                s_suppkey INTEGER NOT NULL,
                s_name CHAR(25) NOT NULL,
                s_address VARCHAR(40) NOT NULL,
                s_nationkey INTEGER NOT NULL,
                s_phone CHAR(15) NOT NULL,
                s_acctbal DECIMAL(15,2) NOT NULL,
                s_comment VARCHAR(101) NOT NULL
            );
            """,
            "customer": """
            CREATE TABLE customer (
                c_custkey INTEGER NOT NULL,
                c_name VARCHAR(25) NOT NULL,
                c_address VARCHAR(40) NOT NULL,
                c_nationkey INTEGER NOT NULL,
                c_phone CHAR(15) NOT NULL,
                c_acctbal DECIMAL(15,2) NOT NULL,
                c_mktsegment CHAR(10) NOT NULL,
                c_comment VARCHAR(117) NOT NULL
            );
            """,
            "partsupp": """
            CREATE TABLE partsupp (
                ps_partkey INTEGER NOT NULL,
                ps_suppkey INTEGER NOT NULL,
                ps_availqty INTEGER NOT NULL,
                ps_supplycost DECIMAL(15,2) NOT NULL,
                ps_comment VARCHAR(199) NOT NULL
            );
            """,
            "orders": """
            CREATE TABLE orders (
                o_orderkey INTEGER NOT NULL,
                o_custkey INTEGER NOT NULL,
                o_orderstatus CHAR(1) NOT NULL,
                o_totalprice DECIMAL(15,2) NOT NULL,
                o_orderdate DATE NOT NULL,
                o_orderpriority CHAR(15) NOT NULL,
                o_clerk CHAR(15) NOT NULL,
                o_shippriority INTEGER NOT NULL,
                o_comment VARCHAR(79) NOT NULL
            );
            """,
            "lineitem": """
            CREATE TABLE lineitem (
                l_orderkey INTEGER NOT NULL,
                l_partkey INTEGER NOT NULL,
                l_suppkey INTEGER NOT NULL,
                l_linenumber INTEGER NOT NULL,
                l_quantity DECIMAL(15,2) NOT NULL,
                l_extendedprice DECIMAL(15,2) NOT NULL,
                l_discount DECIMAL(15,2) NOT NULL,
                l_tax DECIMAL(15,2) NOT NULL,
                l_returnflag CHAR(1) NOT NULL,
                l_linestatus CHAR(1) NOT NULL,
                l_shipdate DATE NOT NULL,
                l_commitdate DATE NOT NULL,
                l_receiptdate DATE NOT NULL,
                l_shipinstruct CHAR(25) NOT NULL,
                l_shipmode CHAR(10) NOT NULL,
                l_comment VARCHAR(44) NOT NULL
            );
            """
        }
        return schemas.get(table_name)

    # 创建表的改进函数
    def create_table_safely(conn, table_name):
        """安全地创建表，处理依赖关系"""
        # 首先尝试从文件读取
        create_table_sql = read_table_schema(table_name)
        
        if create_table_sql:
            try:
                print(f"Creating table {table_name} from file schema...")
                # conn.execute(create_table_sql)
                print(f"已创建表 {table_name}")
                return True
            except Exception as e:
                print(f"从文件创建表 {table_name} 失败: {e}")
        
        # 如果文件方法失败，使用内置schema
        builtin_schema = get_tpch_schema(table_name)
        if builtin_schema:
            try:
                print(f"Creating table {table_name} using built-in schema...")
                conn.execute(builtin_schema)
                print(f"已创建表 {table_name} (使用内置schema)")
                return True
            except Exception as e:
                print(f"使用内置schema创建表 {table_name} 也失败: {e}")
        
        print(f"无法创建表 {table_name}")
        return False

    # DuckDB 优化的数据导入函数 - 使用 COPY 命令直接从文件导入
    def import_data_into_table(conn, table_name, data_file):
        import_t1 = time.time()

        # DuckDB 支持从Parquet文件导入数据
        # copy_sql = f"""
        # COPY {table_name} FROM '{data_file}' 
        # (FORMAT 'parquet');
        # """
        copy_sql = f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_parquet('{data_file}')
            """
        conn.execute(copy_sql)
            
        import_time = time.time() - import_t1
        return import_time

    def update_db_info(db_name, state, working_timestamp):
        service_url = f"http://{scheduler0_ip}:6789/dbInfo/updateByDbName"
        print(f"scheduler0 service_url : {service_url}")
        # Construct the payload
        payload = {
            "dbName": db_name,
            "state": state,
            "workingTimestamp": working_timestamp
        }

        try:
            # Send the POST request with timeout
            response = requests.post(service_url, json=payload, timeout=10)
            response.raise_for_status()
            print(f"Successfully updated DB state to: {state}")

        except requests.exceptions.RequestException as e:
            print(f"Error updating DB info: {e}")
    
    # DuckDB 连接优化设置    
    def optimize_duckdb_connection(conn):
        # DuckDB 的优化配置
        try:
            # 确保没有活跃的事务
            try:
                conn.execute("ROLLBACK;")  # 回滚任何未完成的事务
            except:
                pass  # 忽略如果没有事务的错误
            
            # 内存相关优化
            conn.execute("SET memory_limit='1GB';")  # 设置内存限制
            conn.execute("SET threads=1")             # 单线程
            conn.execute("SET enable_progress_bar=false")
            conn.execute("SET preserve_insertion_order=false")
            
            # 新版本DuckDB的优化参数
            try:
                conn.execute("SET enable_optimizer=true;")
            except:
                # 如果不支持enable_optimizer，尝试其他参数
                pass
            
            try:
                conn.execute("SET enable_profiling=false;")  # 在生产中禁用性能分析
            except:
                pass
            
            # 自动提交模式，避免事务问题
            try:
                conn.execute("SET autocommit=true;")
            except:
                pass
            
            # 临时目录设置（如果需要）
            # conn.execute("SET temp_directory='/tmp/duckdb_temp';")
            
        except Exception as e:
            print(f"DuckDB 优化设置警告: {e}")
        
        return conn
    
    # 限制结果行数的函数
    def limit_results(results, max_rows):
        """限制返回的结果数量以避免内存问题"""
        if len(results) > max_rows:
            print(f"结果被限制为前 {max_rows} 行（总共 {len(results)} 行）")
            return results[:max_rows]
        return results
        
    # 安全连接数据库
    conn_t1 = time.time()
    try:
        conn = safe_connect_duckdb(db_path)
        conn = optimize_duckdb_connection(conn)  # 应用优化设置
        conn_time = time.time() - conn_t1
    except Exception as e:
        return {"error": f"Database connection failed: {str(e)}"}
    
    table_names = extract_table_names(sql_query)
    table_import_times = {}  # 用于存储每个表的数据导入时间
    
    # TPC-H表的依赖关系和创建顺序
    tpch_table_order = [
        "nation",    # 无依赖
        "region",    # 无依赖  
        "part",      # 无依赖
        "supplier",  # 依赖nation
        "customer",  # 依赖nation
        "partsupp",  # 依赖part, supplier
        "orders",    # 依赖customer
        "lineitem"   # 依赖orders, partsupp
    ]
    
    # 根据依赖关系排序需要创建的表
    ordered_tables = [table for table in tpch_table_order if table in table_names]
    print(f"Tables to create in order: {ordered_tables}")
    
    current_timestamp = time.time()  # 当前时间戳，单位为秒
    update_db_info(db_file, "loading", current_timestamp)
    
    for table in ordered_tables:
        # 检查表是否存在 - DuckDB 使用 information_schema
        try:
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = ?", 
                [table]
            ).fetchone()
            table_exists = result is not None
        except:
            # 备用方法：直接查询表
            try:
                conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
                table_exists = True
            except:
                table_exists = False
        
        if table_exists:
            print(f"表 {table} 已存在，跳过创建和数据导入。")
            continue
        else:
            # 创建表
            if not create_table_safely(conn, table):
                print(f"跳过表 {table} 的数据导入，因为表创建失败")
                continue
            
            # 导入数据
            source_data_path = os.path.join(data_path, table + '.parquet')
            print(source_data_path)
            if not os.path.exists(source_data_path):
                print(f"数据文件 {source_data_path} 不存在。")
                continue
            else:
                print(f"正在将 {source_data_path} 的数据导入表 {table}")

            # 记录每个表的数据导入时间
            try:
                import_time = import_data_into_table(conn, table, source_data_path)
                table_import_times[table] = import_time
                print(f"表 {table} 数据导入完成，耗时: {import_time:.2f}秒")
            except Exception as e:
                print(f"表 {table} 数据导入失败: {e}")
                return {"error": f"数据导入失败: {str(e)}"}
            
    try:
        # 如果是从文件读取，记录读取时间
        if sql_source == "sql_file":
            read_t1 = time.time()
            # SQL已经在上面读取
            read_time = time.time() - read_t1
        else:
            read_time = 0  # 直接提供了SQL语句，无需读取文件

        # 导入表结束，更新dbInfo
        current_timestamp = time.time()  # 当前时间戳，单位为秒
        update_db_info(db_file, "working", current_timestamp)
        
        # 执行SQL语句
        sql_t1 = time.time()
        try:
            # 确保连接状态正常
            conn.execute("SELECT 1").fetchone()  # 测试连接
            
            # 执行查询并立即获取结果
            all_results = conn.execute(sql_query).fetchall()
            sql_time = time.time() - sql_t1
            print(f"SQL执行完成，耗时: {sql_time:.2f}秒")
            
            total_rows = len(all_results)
            
            # 限制结果数量
            limited_results = limit_results(all_results, max_results)
            
        except Exception as sql_error:
            print(f"SQL执行错误: {sql_error}")
            # 尝试重新连接并重试一次
            try:
                conn.close()
                conn = safe_connect_duckdb(db_path)
                conn = optimize_duckdb_connection(conn)
                
                all_results = conn.execute(sql_query).fetchall()
                sql_time = time.time() - sql_t1
                print(f"SQL重试执行完成，耗时: {sql_time:.2f}秒")
                
                total_rows = len(all_results)
                limited_results = limit_results(all_results, max_results)
                
            except Exception as retry_error:
                print(f"SQL重试也失败: {retry_error}")
                raise retry_error

        # 关闭数据库连接
        close_t1 = time.time()
        try:
            # 确保清理任何未完成的事务
            try:
                conn.execute("ROLLBACK;")
            except:
                pass
            conn.close()
        except Exception as close_error:
            print(f"关闭连接时出现警告: {close_error}")
        close_time = time.time() - close_t1

        return {
            "executed_sql_source": sql_source,
            "sql_file": sql_file,
            "read_time": read_time,
            "conn_time": conn_time,
            "sql_time": sql_time,
            "close_time": close_time,
            "table_import_times": table_import_times,
            "total_rows": total_rows,  # 新增：总行数
            "returned_rows": len(limited_results),  # 新增：返回行数
            "max_results_limit": max_results,  # 新增：限制参数
            "results": limited_results  # 修改：返回限制后的结果
        }
    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        # 安全关闭连接
        try:
            # 清理事务状态
            try:
                conn.execute("ROLLBACK;")
            except:
                pass
            conn.close()
        except Exception as cleanup_error:
            print(f"清理连接时出现错误: {cleanup_error}")
        return {"error": str(e)}