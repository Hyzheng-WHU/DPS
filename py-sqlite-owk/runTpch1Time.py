import sqlite3
import time

def main(args):
    # 数据库与要执行的SQL文件的路径
    db_path = "/db/tpch.sqlite"
    # 若未指定sql文件，则默认1.sql
    sql_file = args.get("sql_file", "1.sql")
    sql_query_path = f"/sql/tpch_sqls/{sql_file}"
    
    try:
        # 读取SQL语句
        with open(sql_query_path, 'r') as f:
            sql_query = f.read()

        # 连接数据库（读取数据库文件）
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 执行SQL语句
        t1=time.time()
        cursor.execute(sql_query)
        t2=time.time()
        spend_time=t2-t1
        results = cursor.fetchall()

        # 关闭数据库连接
        conn.close()
        return {
            "executed_sql": sql_file,
            "spend_time": spend_time,
            "sql_result": str(results),
        }
    except Exception as e:
        return {'error': str(e)}