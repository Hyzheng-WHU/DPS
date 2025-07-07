import duckdb
import time
import re
import os
import requests
import json

def safe_connect_duckdb(db_path):
    """安全连接DuckDB"""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    
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

def optimize_duckdb_connection(conn):
    """DuckDB连接优化设置"""
    conn.execute("SET memory_limit='1.8GB';")
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
    
    try:
        requests.post(service_url, json=payload, timeout=10)
        print(f"Updated DB state to: {state}")
    except Exception as e:
        print(f"Failed to update DB state: {e}")

def verify_tables_exist(conn, table_names):
    """验证所需表是否存在"""
    missing_tables = []
    for table in table_names:
        try:
            conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
            print(f"Table {table} exists")
        except Exception as e:
            missing_tables.append(table)
            print(f"Table {table} missing: {e}")
    
    if missing_tables:
        raise Exception(f"Missing required tables: {missing_tables}")

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
    sql_file = args.get("sql_file")
    print(f"query: {sql_file}")
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

    # 直接连接到预构建的数据库
    db_path = "/db/remote_db/tpch_5g.db"
    print(f"Connecting to pre-built database: {db_path}")
    
    conn = safe_connect_duckdb(db_path)
    conn = optimize_duckdb_connection(conn)
    
    # 提取并验证所需表
    table_names = extract_table_names(sql_query)
    verify_tables_exist(conn, table_names)
    
    # 更新状态为工作中
    db_file = args.get("db_file", "tpch_5g.db")  # 使用传入的db_file或默认值
    update_db_info(scheduler0_ip, db_file, "working", time.time())
    
    # 执行SQL查询
    print("Executing SQL query...")
    sql_t1 = time.time()
    result = conn.sql(sql_query).fetchone()  # 只获取一行结果
    sql_time = time.time() - sql_t1
    print(f"SQL executed, time: {sql_time:.2f}s")
    
    # 关闭连接
    conn.close()
    print("Database connection closed")

    return {
        "executed_sql_source": sql_source,
        "sql_file": sql_file,
        "sql_time": sql_time,
        "db_path": db_path,
        "table_names": table_names,
        # "result": result  # 只返回一行结果
    }