# 修改版的代码，执行sql语句并详细记录各项时间
import sqlite3
import time

def main(args):
    # 数据库与要执行的SQL文件的路径
    db_path = "/db/tpch.sqlite"
    # 若未指定sql文件，则默认1.sql
    sql_file = args.get("sql_file")
    sql_query_path = f"/sql/tpch_sqls/{sql_file}"
    try:
        # 读取SQL语句
        read_t1=time.time()
        with open(sql_query_path, 'r') as f:
            sql_query = f.read()
        read_time=time.time()-read_t1

        # 连接数据库（读取数据库文件）
        conn_t1=time.time()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        conn_time=time.time()-conn_t1

        # 执行SQL语句
        sql_t1=time.time()
        cursor.execute(sql_query)
        results = cursor.fetchall() # 这一步对某些sql比较花时间
        sql_time=time.time()-sql_t1

        # 关闭数据库连接
        close_t1=time.time()
        conn.close()
        close_time=time.time()-close_t1
        return {
            "executed_sql": sql_file,
            "read_time":read_time,
            "conn_time":conn_time,
            "sql_time": sql_time,
            "close_time":close_time,
            # "sql_result": str(results),
        }
    except Exception as e:
        return {'error': str(e)}