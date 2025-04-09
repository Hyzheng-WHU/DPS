import time

def main(dict):
    n = dict.get("n", 1000000)  # 默认计算 1,000,000 次
    t = dict.get("t", 5)  # 默认5s
    start_time = time.time()
    result = 0
    for i in range(n):
        result += i * i
    end_time = time.time()
    
    exec_time = end_time - start_time

    time.sleep(t)


    return {"result": result, "execution_time": exec_time}
