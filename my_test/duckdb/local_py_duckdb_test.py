import duckdb
import os
import time
from pathlib import Path

def create_database_and_tables():
    """创建数据库并建表"""
    # 创建数据库连接
    conn = duckdb.connect('./test_duckdb.db')
    
    # 定义表名和对应的文件
    tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
    
    print("开始创建表...")
    
    for table in tables:
        sql_file = f'/home/t1/sqlite/sql/tpch_init_sqls/{table}.sql'
        data_file = f'/home/t1/sqlite/data/tpch_1g/{table}.tbl.clean'
        
        try:
            # 读取并执行建表SQL
            print(f"创建表: {table}")
            with open(sql_file, 'r', encoding='utf-8') as f:
                create_sql = f.read()
            conn.execute(create_sql)
            
            # 导入数据
            print(f"导入数据: {table}")
            # 使用DuckDB的COPY命令导入数据，指定分隔符为'|'
            copy_sql = f"COPY {table} FROM '{data_file}' (DELIMITER '|')"
            conn.execute(copy_sql)
            
            # 检查导入的数据量
            count_result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            print(f"表 {table} 导入了 {count_result[0]} 行数据")
            
        except Exception as e:
            print(f"处理表 {table} 时出错: {e}")
            continue
    
    print("表创建和数据导入完成！")
    return conn

def execute_queries(conn):
    """执行所有查询"""
    query_dir = '/home/t1/sqlite/sql/tpch_sqls'
    
    # 获取所有SQL文件并排序
    sql_files = []
    for i in range(1, 25):  # TPC-H有24个查询
        sql_file = f'{query_dir}/{i}.sql'
        if os.path.exists(sql_file):
            sql_files.append((i, sql_file))
    
    print(f"\n开始执行 {len(sql_files)} 个查询...")
    
    results = {}
    
    for query_num, sql_file in sql_files:
        try:
            print(f"执行查询 {query_num}...")
            
            # 读取SQL文件
            with open(sql_file, 'r', encoding='utf-8') as f:
                query_sql = f.read()
            
            # 记录执行时间
            start_time = time.time()
            
            # 执行查询
            result = conn.execute(query_sql).fetchall()
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # 存储结果信息
            results[query_num] = {
                'rows': len(result),
                'time': execution_time,
                'success': True
            }
            
            print(f"查询 {query_num} 完成 - 返回 {len(result)} 行，耗时 {execution_time:.2f} 秒")
            
        except Exception as e:
            print(f"查询 {query_num} 执行失败: {e}")
            results[query_num] = {
                'rows': 0,
                'time': 0,
                'success': False,
                'error': str(e)
            }
    
    return results

def print_summary(results):
    """打印执行摘要"""
    print("\n" + "="*50)
    print("执行摘要")
    print("="*50)
    
    successful_queries = [q for q, r in results.items() if r['success']]
    failed_queries = [q for q, r in results.items() if not r['success']]
    
    print(f"成功执行的查询: {len(successful_queries)}")
    print(f"失败的查询: {len(failed_queries)}")
    
    if successful_queries:
        total_time = sum(results[q]['time'] for q in successful_queries)
        print(f"总执行时间: {total_time:.2f} 秒")
        print(f"平均执行时间: {total_time/len(successful_queries):.2f} 秒")
    
    if failed_queries:
        print(f"\n失败的查询: {failed_queries}")
        for q in failed_queries:
            print(f"  查询 {q}: {results[q].get('error', '未知错误')}")
    
    print("\n详细结果:")
    for query_num in sorted(results.keys()):
        r = results[query_num]
        if r['success']:
            print(f"查询 {query_num:2d}: {r['rows']:6d} 行, {r['time']:6.2f} 秒")
        else:
            print(f"查询 {query_num:2d}: 失败")

def main():
    """主函数"""
    print("开始DuckDB TPC-H测试...")
    
    try:
        # 如果数据库文件已存在，删除它
        if os.path.exists('./test_duckdb.db'):
            os.remove('./test_duckdb.db')
            print("删除了已存在的数据库文件")
        
        # 创建数据库和表
        conn = create_database_and_tables()
        
        # 执行查询
        results = execute_queries(conn)
        
        # 打印摘要
        print_summary(results)
        
        # 关闭连接
        conn.close()
        print("\n测试完成！")
        
    except Exception as e:
        print(f"测试过程中出现错误: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()