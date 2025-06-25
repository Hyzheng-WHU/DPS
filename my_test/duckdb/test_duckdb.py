import duckdb
import time
import re
import os
import requests
import json

def safe_connect_duckdb(db_path):
    """安全连接DuckDB"""
    if os.path.exists(db_path) and os.path.getsize(db_path) == 0:
        os.remove(db_path)
    
    conn = duckdb.connect(db_path)
    conn.execute("SELECT 1").fetchone()
    print(f"Connected to database: {db_path}")
    return conn

def extract_table_names(sql):
    """提取SQL中涉及的TPC-H表名"""
    table_names = []
    tpch_tables = ["nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]
    
    for table in tpch_tables:
        pattern = re.compile(r"\b" + re.escape(table) + r"\b", re.IGNORECASE)
        if pattern.search(sql):
            table_names.append(table)
    
    print(f"table_names: {table_names}")
    return table_names

def import_data_from_parquet(conn, table_name, data_file):
    """从Parquet文件直接创建表"""
    import_t1 = time.time()
    copy_sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{data_file}')"
    conn.execute(copy_sql)
    import_time = time.time() - import_t1
    return import_time

def optimize_duckdb_connection(conn):
    """DuckDB连接优化设置"""
    conn.execute("SET memory_limit='1GB';")
    conn.execute("SET threads=1")
    conn.execute("SET enable_progress_bar=false")
    return conn

def update_db_info(scheduler_ip, db_name, state, working_timestamp):
    """更新数据库状态信息"""
    if not scheduler_ip:
        return
        
    service_url = f"http://{scheduler_ip}:6789/dbInfo/updateByDbName"
    payload = {
        "dbName": db_name,
        "state": state,
        "workingTimestamp": working_timestamp
    }
    
    requests.post(service_url, json=payload, timeout=10)
    print(f"Updated DB state to: {state}")

def table_exists(conn, table_name):
    """检查表是否存在"""
    try:
        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except:
        return False

def main(args):
    # 读取scheduler IP
    scheduler0_ip = None
    try:
        with open('/db/dockerIP.json', 'r') as f:
            data = json.load(f)
            scheduler0_ip = data.get("scheduler0IP")
        print(f"scheduler0_ip: {scheduler0_ip}")
    except:
        pass
    
    # 解析参数
    data_file = args.get("data_file")
    # data_path = f"/db/remote_db/data/{data_file}/parquet/"
    data_path = f"/db/local_data/{data_file}/parquet/"
    
    db_file = args.get("db_file")
    db_path = f"/db/{db_file}"
    
    sql_file = args.get("sql_file")
    sql_query_provided = args.get("sql_query")

    # 获取SQL语句
    if sql_query_provided:
        sql_query = sql_query_provided
        sql_source = "sql_query"
    else:
        sql_query_path = f"/sql/{sql_file}"
        with open(sql_query_path, "r") as f:
            sql_query = f.read()
        sql_source = "sql_file"

    # 连接数据库
    conn = safe_connect_duckdb(db_path)
    conn = optimize_duckdb_connection(conn)
    
    table_names = extract_table_names(sql_query)
    table_import_times = {}
    
    # TPC-H表的创建顺序
    tpch_table_order = ["nation", "region", "part", "supplier", "customer", "partsupp", "orders", "lineitem"]
    ordered_tables = [table for table in tpch_table_order if table in table_names]
    print(f"Tables to create: {ordered_tables}")
    
    # 更新状态为加载中
    update_db_info(scheduler0_ip, db_file, "loading", time.time())
    
    # 创建表并导入数据
    for table in ordered_tables:
        if table_exists(conn, table):
            print(f"Table {table} exists, skipping")
            continue
        
        source_data_path = os.path.join(data_path, table + '.parquet')
        print(f"Creating table {table} from {source_data_path}")
        
        import_time = import_data_from_parquet(conn, table, source_data_path)
        table_import_times[table] = import_time
        print(f"Table {table} created, time: {import_time:.2f}s")
    
    # 更新状态为工作中
    update_db_info(scheduler0_ip, db_file, "working", time.time())
    
    # 执行SQL查询
    sql_t1 = time.time()
    result = conn.sql(sql_query).fetchone()  # 只获取一行结果
    # conn.sql(sql_query)
    sql_time = time.time() - sql_t1
    print(f"SQL executed, time: {sql_time:.2f}s")
    
    # 关闭连接
    conn.close()

    return {
        "executed_sql_source": sql_source,
        "sql_file": sql_file,
        "sql_time": sql_time,
        "table_import_times": table_import_times,
        # "result": result  # 只返回一行结果
    }