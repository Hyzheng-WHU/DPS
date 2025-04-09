import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"])

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


def main(args):
    data_file = args.get("data_file")
    data_path = f"/db/remote_db/data/{data_file}/"
    print(f"data_path: {data_path}")
    db_file = args.get("db_file")
    db_path = f"/db/{db_file}"
    print(f"db_path: {db_path}")
    
    predict_time = float(args.get("predict_time"))
    print(f"predict_time:{predict_time}, type:{type(predict_time)}")
    
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

    # 优化的数据导入函数 - 使用批量插入
    def import_data_into_table(conn, table_name, data_file):
        cursor = conn.cursor()
        
        # 开始事务
        cursor.execute("BEGIN TRANSACTION;")
        
        import_t1 = time.time()
        batch_size = 10000  # 批量插入的行数
        batch = []
        
        with open(data_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # 按 | 分隔数据
                row = line.split('|')
                batch.append(row)
                
                # 当积累了足够的行时执行批量插入
                if len(batch) >= batch_size:
                    placeholders = ','.join(['?'] * len(batch[0]))
                    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    cursor.executemany(insert_sql, batch)
                    batch = []
            
            # 插入剩余的行
            if batch:
                placeholders = ','.join(['?'] * len(batch[0]))
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.executemany(insert_sql, batch)
            
            # 提交事务
            conn.commit()
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
            # Send the POST request
            response = requests.post(service_url, json=payload)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"Error updating DB info: {e}")
    
    # 优化的SQLite连接设置    
    def optimize_sqlite_connection(conn):
        cursor = conn.cursor()
        
        # 内存相关优化
        cursor.execute("PRAGMA main.page_size = 4096;")         # 设置页大小为4KB，可提高缓存利用率
        cursor.execute("PRAGMA main.cache_size = -30000;")      # 设置缓存大小约为30MB (-30000表示30000页，即30000*4096bytes)
        
        # 写入相关优化
        cursor.execute("PRAGMA main.journal_mode = MEMORY;")    # 内存中的日志
        cursor.execute("PRAGMA main.synchronous = OFF;")        # 禁用同步写入
        cursor.execute("PRAGMA main.temp_store = MEMORY;")      # 临时表存储在内存中
        
        # 查询优化
        cursor.execute("PRAGMA main.mmap_size = 268435456;")    # 为内存映射分配约256MB内存
        cursor.execute("PRAGMA main.foreign_keys = OFF;")       # 禁用外键约束检查以提高性能
        cursor.execute("PRAGMA main.auto_vacuum = NONE;")       # 禁用自动整理
        cursor.execute("PRAGMA main.locking_mode = EXCLUSIVE;") # 独占锁定模式
        cursor.execute("PRAGMA main.count_changes = OFF;")      # 禁用更改计数
        
        return cursor
        
    # 连接数据库
    conn_t1 = time.time()
    conn = duckdb.connect(db_path)
    cursor = optimize_sqlite_connection(conn)  # 应用优化设置
    conn_time = time.time() - conn_t1
    

    table_names = extract_table_names(sql_query)
    table_import_times = {}  # 用于存储每个表的数据导入时间
    
    
    current_timestamp = time.time()  # 当前时间戳，单位为秒
    update_db_info(db_file, "loading", current_timestamp)
    for table in table_names:
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
        table_exists = cursor.fetchone()
        if table_exists:
            print(f"表 {table} 已存在，跳过创建和数据导入。")
            continue
        else:
            # 从文件中读取建表语句
            create_table_sql = read_table_schema(table)
            if create_table_sql:
                cursor.executescript(create_table_sql)
                print(f"已创建表 {table}")
            else:
                print(f"未找到表 {table} 的建表语句文件。")
                continue
            
            # 导入数据
            source_data_path = os.path.join(data_path, table + '.tbl.clean')
            print(source_data_path)
            if not os.path.exists(source_data_path):
                print(f"数据文件 {source_data_path} 不存在。")
                continue
            else:
                print(f"正在将 {source_data_path} 的数据导入表 {table}")

            # 记录每个表的数据导入时间
            import_time = import_data_into_table(conn, table, source_data_path)
            table_import_times[table] = import_time
            
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
        
        # 执行查询前，进行额外的优化
        cursor.execute("PRAGMA analysis_limit=1000;")  # 限制查询优化器分析的行数
        cursor.execute("PRAGMA query_only=ON;")        # 设置为只读模式，可以优化读取性能

        # 执行SQL语句
        sql_t1 = time.time()
        cursor.execute(sql_query)
        sql_time = time.time() - sql_t1
        # results = cursor.fetchall()  # 这一步对某些SQL比较花时间

        # 关闭数据库连接
        close_t1 = time.time()
        conn.close()
        close_time = time.time() - close_t1

        return {
            "executed_sql_source": sql_source,
            "sql_file": sql_file,
            "read_time": read_time,
            "conn_time": conn_time,
            "sql_time": sql_time,
            "close_time": close_time,
            "table_import_times": table_import_times
        }
    except Exception as e:
        return {"error": str(e)}