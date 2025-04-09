import sqlite3
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

# with open('/db/dockerIP.json', 'r') as f:
#     data = json.load(f)
#     scheduler0_ip = data.get("scheduler0IP")
# print(f"scheduler0_ip: {scheduler0_ip}")


def main(args):
    data_file = args.get("data_file")
    data_path = data_file
    print(f"data_path: {data_path}")
    db_file = args.get("db_file")
    db_path = db_file
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
        # sql_query_path = f"/sql/{sql_file}"
        sql_query_path = sql_file
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

    # 替换为TPC-DS相关逻辑
    def extract_table_names(sql):
        table_names = []
        tpcds_tables = [
            "call_center",
            "catalog_page",
            "catalog_returns",
            "catalog_sales",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_returns",
            "store_sales",
            "time_dim",
            "warehouse",
            "web_page",
            "web_returns",
            "web_sales",
            "web_site"
        ]
        for table in tpcds_tables:
            pattern = re.compile(r"\b" + re.escape(table) + r"\b", re.IGNORECASE)
            if pattern.search(sql):
                table_names.append(table)
        print(f"table_names: {table_names}")
        return table_names


    def read_table_schema(table_name):
        schema_file = os.path.join("/home/t1/sqlite/sql/tpcds_init_sqls", f"{table_name}.sql")
        print("schema_file:",schema_file)
        if os.path.exists(schema_file):
            with open(schema_file, "r") as file:
                return file.read()
        else:
            return None


    def get_table_column_count(table_name):
        tpcds_table_columns = {
            "call_center": 8,
            "catalog_page": 18,
            "catalog_returns": 23,
            "catalog_sales": 34,
            "customer": 8,
            "customer_address": 8,
            "customer_demographics": 4,
            "date_dim": 15,
            "household_demographics": 2,
            "income_band": 2,
            "inventory": 6,
            "item": 18,
            "promotion": 15,
            "reason": 3,
            "ship_mode": 4,
            "store": 24,
            "store_returns": 23,
            "store_sales": 23,
            "time_dim": 11,
            "warehouse": 9,
            "web_page": 7,
            "web_returns": 23,
            "web_sales": 34,
            "web_site": 11
        }
        return tpcds_table_columns.get(table_name, 0)


    def import_data_into_table(conn, table_name, data_file):
        """
        使用 SQLite 内置批量插入方法导入数据
        """
        cursor = conn.cursor()

        # 启用更高效的事务模式
        cursor.execute("PRAGMA synchronous = OFF;")
        cursor.execute("PRAGMA journal_mode = MEMORY;")

        with open(data_file, 'r') as f:
            import_t1 = time.time()
            for line in f:
                line = line.strip()
                # print(line)
                if not line:
                    continue
                # 按 | 分隔数据
                row = line.split('|')[:-1]  # 移除最后的空值列

                # 构造插入 SQL（列名在建表时已匹配）
                placeholders = ','.join(['?'] * len(row))
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.execute(insert_sql, row)

            conn.commit()
            import_time = time.time() - import_t1
            print(f"数据导入完成，耗时：{import_time:.2f}秒")
    
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
            print("e")
        
    # 连接数据库
    conn_t1 = time.time()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    conn_time = time.time() - conn_t1
    

    table_names = extract_table_names(sql_query)
    table_import_times = {}  # 用于存储每个表的数据导入时间
    
    
    current_timestamp = time.time()  # 当前时间戳，单位为秒
    # update_db_info(db_file, "loading", current_timestamp)
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
            
            # 提交修改
            conn.commit()
    try:
        # 如果是从文件读取，记录读取时间
        if sql_source == "sql_file":
            read_t1 = time.time()
            # SQL已经在上面读取
            read_time = time.time() - read_t1
        else:
            read_time = 0  # 直接提供了SQL语句，无需读取文件


        # 导入表结束，更新dbInfo
        # update_state_to_working()
        current_timestamp = time.time()  # 当前时间戳，单位为秒
        # update_db_info(db_file, "working", current_timestamp)

        # 执行SQL语句
        sql_t1 = time.time()
        cursor.execute(sql_query)
        results = cursor.fetchall()  # 这一步对某些SQL比较花时间
        sql_time = time.time() - sql_t1

        # 关闭数据库连接
        close_t1 = time.time()
        conn.close()
        close_time = time.time() - close_t1

        return {
            "executed_sql_source": sql_source,
            # "executed_sql": sql_file if sql_source == "sql_file" else "direct_sql_query",
            # "target_table": sql_query.split("from")[-1].replace(";", ""),
            # "query": sql_query,
            "sql_file": sql_file,
            "read_time": read_time,
            "conn_time": conn_time,
            "sql_time": sql_time,
            "close_time": close_time,
            # 暂不计算sql_result，也不传回
            "sql_result": str(results),
            "table_import_times": table_import_times
        }
    except Exception as e:
        return {"error": str(e)}
    
if __name__ == "__main__":
    # 构造参数
    args = {
        "db_file": "/home/t1/sqlite/db/test_tpcds.db",  # 数据库文件名
        "data_file": "/home/t1/sqlite/db/remote_db/data/tpcds",  # 数据文件目录名
        "sql_file": None,  # 初始时没有提供单个SQL文件
        "predict_time": 30.0,  # 预测运行时间（假设为 30 秒，可以根据实际情况调整）
        "sql_query": None  # 如果直接提供 SQL 查询字符串，可以在这里填充，否则为 None
    }

    # 获取 SQL 文件夹路径
    root_sql_dir = "/home/t1/sqlite/sql/tpcds_sqls"  # 根文件夹路径
    executed_sql_files = []

    # 遍历根目录下的查询文件夹（如 query001）
    for query_folder in os.listdir(root_sql_dir):
        query_folder_path = os.path.join(root_sql_dir, query_folder)

        # 如果是文件夹（例如 query001），则遍历该文件夹中的 SQL 文件
        if os.path.isdir(query_folder_path):
            for sql_filename in os.listdir(query_folder_path):
                sql_file_path = os.path.join(query_folder_path, sql_filename)
                
                if sql_file_path.endswith('.sql'):
                    # 更新参数，设置当前SQL文件路径
                    args["sql_file"] = sql_file_path

                    # 调用 main 函数执行 SQL 查询
                    result = main(args)

                    # 打印返回的 result，以便调试
                    print(f"Result for {sql_file_path}: {json.dumps(result, indent=4, ensure_ascii=False)}")

                    # 检查 result 是否包含 'sql_result' 键
                    if "sql_result" in result and result["sql_result"] and len(result["sql_result"]) > 0:
                        executed_sql_files.append({
                            "sql_file": sql_file_path,
                            "sql_result": result["sql_result"]
                        })

    # 输出执行结果
    print(json.dumps({"executed_sql_files": executed_sql_files}, indent=4, ensure_ascii=False))