import sqlite3
import time

def main(dict):
    # 数据库与要执行的SQL文件的路径
    db_path = "/db/tpch.sqlite"
    sql_query_path = "/sql/tpch_sqls/1.sql"
    try:
        # 读取SQL语句
        with open(sql_query_path, 'r') as f:
            sql_query = f.read()

        # 连接数据库（读取数据库文件）
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 执行SQL语句
        cursor.execute(sql_query)
        results = cursor.fetchall()

        # 关闭数据库连接
        conn.close()
        return {'result':str(results)}
    except Exception as e:
        return {'error': str(e)}