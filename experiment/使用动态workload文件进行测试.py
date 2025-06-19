# requests包用于发送HTTP请求
import requests

# 构建header时，需要将auth转化为Base64编码
import base64
from datetime import datetime, timezone
import pytz
import time
import pandas as pd
import warnings
import os
import paramiko
import matplotlib.pyplot as plt
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser()
parser.add_argument('--warm_time', type=str, required=True)
parser.add_argument('--schedule_strategy', type=str, required=True)
parser.add_argument('--containerStrategy', type=str, required=True)
parser.add_argument('--least_save_containers', type=str, required=True)
parser.add_argument('--prefix', type=str, required=True)
parser.add_argument('--suffix', type=str, default='')
parser.add_argument('--workload', type=str, default='')
parser.add_argument('--window_size', type=str, default='')
parser.add_argument('--total_time_window', type=str, default='')
parser.add_argument('--sliding_window_size', type=str, default='')
parser.add_argument('--sliding_step_size', type=str, default='')
parser.add_argument('--trigger_threshold', type=str, default='')


args = parser.parse_args()
workload_path = args.workload
warm_time = args.warm_time
least_save_containers = args.least_save_containers
schedule_strategy = args.schedule_strategy
containerStrategy = args.containerStrategy
sliding_window_size = args.sliding_window_size
sliding_step_size = args.sliding_step_size
trigger_threshold = args.trigger_threshold
prefix = args.prefix
suffix = args.suffix
suffix += "(new_code_t)"
suffix += "(sh)"

# 配置openwhisk
ow_default_apihost = "100.88.221.100"  # ow-default
# ow_change_apihost = "100.114.134.35"  # ow-change
ow_change_apihost = "127.0.0.1"
ow_default_clone_apihost = "100.126.214.106"  # ow-default-clone
# wsk property get
auth = "23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
auth_key = "Basic " + base64.b64encode(auth.encode()).decode()
# print(auth_key)

# 构建消息Headers，发送消息
def send_req(url, params):
    # 构建header
    headers = {
        "Content-Type": "application/json",
        "Authorization": auth_key,
        # "User-Agent": ["OpenWhisk-CLI/1.0 (2021-04-01T23:49:54.523+0000) linux amd64"]
    }
    # print(headers)
    try:
        response = requests.post(
            url, json=params, headers=headers, verify=False, timeout=(5, 30)
        )
        if response.status_code == 202:
            # 立即返回 activationId
            activation_id = response.json().get("activationId")
        return response
    except requests.exceptions.RequestException as e:
        print(f"HTTP请求失败: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"响应内容: {e.response.text}")
        raise


# 根据要执行的函数构建url
def build_url(func):
    url = (
        "https://"
        + apihost
        + "/api/v1/namespaces/_/actions/"
        + func
        # + "?blocking=true&result=false"  # 阻塞调用
        + "?blocking=false&result=false"  # 非阻塞调用，无-r
    )
    # print(url)
    return url


# 解析返回消息
def call_and_get_activationId(url, params={}):
    current_time = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )
    print("正在执行", params.get("sql_file"), "UTC时间：", current_time)
    response = send_req(url, params)
    # 处理函数返回结果
    if response.status_code == 200:
        print("执行成功")
        # func_result = response.json()

        # print(func_result.get("spend_time"))

        # for key, value in func_result.items():
        # print(f"{key}: {value}")
        return response
    elif response.status_code == 202:
        # print("202，无结果，仅有activationId")

        return response
        # print(response.json())
    else:
        try:
            error_detail = response.json()  # 尝试从响应中解析JSON
            print(
                f"{params.get('sql_file')}执行失败，失败码: {response.status_code}, error: {error_detail}"
            )
        except ValueError:
            # 如果响应不是JSON格式，打印文本内容
            print(
                f"{params.get('sql_file')}执行失败，失败码: {response.status_code}, error: {response.text}"
            )
        return None
# 根据activation id查找详细信息
def get_activation_result(activation_id):
    url = f"https://{apihost}/api/v1/namespaces/_/activations/{activation_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": auth_key,
    }
    try:
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            return response.json()

    except requests.exceptions.RequestException as e:
        print(f"HTTP 请求失败: {e}")
        return None


# 提取特定属性并生成表格
def extract_activation_info(activation):
    activation_id = activation.get("activationId")
    wait_time = (
        next(
            (
                annotation["value"]
                for annotation in activation["annotations"]
                if annotation["key"] == "waitTime"
            ),
            None,
        )
        / 1000
    )
    memory = next(
        (
            annotation["value"]["memory"]
            for annotation in activation["annotations"]
            if annotation["key"] == "limits"
        ),
        None,
    )
    tz = pytz.timezone("Asia/Shanghai")  # 东八区时区
    start_time = (
        datetime.utcfromtimestamp(activation.get("start") / 1000)
        .replace(tzinfo=pytz.utc)
        .astimezone(tz)
        .strftime("%m-%d %H:%M:%S.%f")[:-3]
    )
    end_time = (
        datetime.utcfromtimestamp(activation.get("end") / 1000)
        .replace(tzinfo=pytz.utc)
        .astimezone(tz)
        .strftime("%m-%d %H:%M:%S.%f")[:-3]
    )

    duration = activation.get("duration") / 1000
    name = activation.get("name")

    # 判断是否冷启动
    if any(annotation["key"] == "initTime" for annotation in activation["annotations"]):
        # 如果有initTime字段，则为冷启动
        start_mode = "cold"
        init_time = str(
            (
                next(
                    (
                        annotation["value"]
                        for annotation in activation["annotations"]
                        if annotation["key"] == "initTime"
                    ),
                    None,
                )
                / 1000
            )
        )
    else:
        start_mode = "warm"
        init_time = "-"

    # 获取 调用结果
    activation_detail = get_activation_result(activation_id)
    if activation_detail:
        result = activation_detail["response"]["result"]
        error = result.get("error")
        if error:
            print(f"执行{activation_id}出错: {error}")
            return None

        sql_time = result.get("sql_time")
        table_import_times = result.get("table_import_times")
        total_table_import_time = sum(table_import_times.values())
        executed_sql = result.get("sql_file")

    info = {
        "activationId": activation_id,
        "name": name,
        "memory(m)": memory,
        "executed_sql": executed_sql,
        "start_mode": start_mode,
        "waitTime(s)": wait_time,
        "init_time(s)": init_time,
        "start_time": start_time,
        "end_time": end_time,
        "duration(s)": duration,
        "sql_time(s)": sql_time,
        "table_import_times(s)": table_import_times,
        "total_table_import_time(s)": total_table_import_time,
    }
    return info
  # 初始化 DataFrame
columns = [
    "activationId",
    "name",
    "memory(m)",
    "executed_sql",
    "start_mode",
    "waitTime(s)",
    "init_time(s)",
    "start_time",
    "end_time",
    "duration(s)",
    "sql_time(s)",
    "table_import_times(s)",
    "total_table_import_time(s)",
    "send_to_receive_duration",
]
# 接收func参数，发送请求并返回activationId
def process_function(func):

    url = build_url(func["func_name"])

    params = func["params"]

    response = call_and_get_activationId(url, params)

    activationId = response.text.split(":")[1].replace('"', "").replace("}", "")

    print("activationId:" + activationId)

    return activationId

    # if activationId:

    #     while True:

    #         try:

    #             activation = get_activation_result(activationId)

    #             if activation:

    #                 formatted_info = extract_activation_info(activation)

    #                 formatted_info["send_to_receive_duration(s)"] = (

    #                     response.elapsed.total_seconds()
    #                 )

    #                 return formatted_info

    #         except Exception as e:

    #             print("error!", e)

    #             time.sleep(0.1)
# 连接服务器，获取预测的sql运行时间
def predict_sql_execution_time(cpu, memory, sql_file=None, sql_query=None):

    # 如果没有指定 sql_query，则根据 sql_file 路径读取查询

    if not sql_query:

        sql_file_path = os.path.join("../../../../sql/", sql_file)

        with open(sql_file_path, "r") as file:

            sql_query = file.read()

        # print(sql_query)

    # 构建请求数据

    data = {"data": sql_query, "cpu": cpu, "memory": memory}

    # 开始计时
    st = time.perf_counter()

    try:

        # 发送 POST 请求

        response = requests.post("http://10.254.25.30:8000/process", json=data)

        response.raise_for_status()  # 检查是否有请求错误

        result = response.json()

    except requests.exceptions.RequestException as e:

        print(f"Request failed: {e}")

        return None

    # 结束计时
    et = time.perf_counter()

    # 解析服务器返回结果

    predict_time = result.get("predict_time", [None])[0]  # 提取 predict_time

    elapsed_time = et - st  # 计算实际花费的时间

    # 返回预测的执行时间和请求耗时

    return predict_time, elapsed_time
def download_and_rename_file(
    remote_file_path,
    save_name,
    save_path,
    host=ow_change_apihost,
    username="t1",
    password="0",
):
    # 准备完整的本地保存文件路径
    local_file_path = os.path.join(save_path, f"{save_name}")

    # 使用 paramiko 连接远程服务器
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 自动接受未知主机密钥
    try:
        # 建立 SSH 连接
        ssh.connect(host, username=username, password=password)

        # 使用 SFTP 下载文件
        sftp = ssh.open_sftp()
        sftp.get(remote_file_path, local_file_path)  # 从远程路径下载到本地路径

        # 删除远端文件
        # sftp.remove(remote_file_path)
        print(
            f"文件已下载并保存为: {local_file_path}"
        )

        sftp.close()
    except Exception as e:
        print(f"下载文件或删除远程文件失败: {e}")
    finally:
        ssh.close()  # 确保连接关闭
def upload_and_delete_local_file(
    local_file_path,
    remote_save_name,
    remote_path,
    host="100.102.32.71",  # Windows主机IP
    username="联想",
    password="857571",
):
    """
    将本地文件上传到远程Windows主机并可选删除本地文件
    参数:
    local_file_path: 本地文件完整路径
    remote_save_name: 上传到远程主机后的文件名
    remote_path: 远程主机上的保存路径
    host: 远程Windows主机IP地址
    username: Windows用户名
    password: Windows密码
    """
    # 准备完整的远程保存文件路径
    remote_file_path = os.path.join(remote_path, f"{remote_save_name}").replace('\\', '/')
    
    # 使用 paramiko 连接远程服务器
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 自动接受未知主机密钥
    
    try:
        # 建立 SSH 连接
        ssh.connect(host, username=username, password=password)
        
        # 检查远程目录是否存在，如果不存在则创建
        check_dir_cmd = f'powershell -Command "if (-not (Test-Path -Path \'{remote_path}\')) {{ New-Item -ItemType Directory -Path \'{remote_path}\' -Force }}"'
        stdin, stdout, stderr = ssh.exec_command(check_dir_cmd)
        
        # 等待命令执行完成
        stdout.channel.recv_exit_status()
        
        # 使用 SFTP 上传文件
        sftp = ssh.open_sftp()
        sftp.put(local_file_path, remote_file_path)  # 从本地路径上传到远程路径
        
        # 删除本地文件(可选，取决于您是否需要这个功能)
        # os.remove(local_file_path)
        
        print(f"文件已上传到远程路径: {remote_file_path}\n")
        
        sftp.close()
    except Exception as e:
        print(f"上传文件或删除本地文件失败: {e}")
    finally:
        ssh.close()  # 确保连接关闭

def batch_process_activations(activation_id_list, df, max_workers=10):
    """
    并行处理多个activation并将结果写入DataFrame

    Args:
        activation_id_list (list): 要处理的activation ID列表
        df (pd.DataFrame): 用于存储结果的DataFrame
        max_workers (int, optional): 并行线程数，默认10

    Returns:
        tuple: (失败的activation ID列表, 总耗时)
    """
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    import time

    # 创建一个锁用于同步DataFrame的写入
    df_lock = Lock()

    def process_activation(args):
        index, activation_id, total_activations = args
        try:
            print(
                f"Processing {index}/{total_activations} - activationId: {activation_id}"
            )
            activation = get_activation_result(activation_id)

            if activation:
                try:
                    formatted_info = extract_activation_info(activation)
                    if formatted_info is None:
                        # 获取失败
                        print(
                            f"Activation result not found for activationId {activation_id}"
                        )
                        return activation_id  # 返回失败的ID

                    # 使用锁来保护DataFrame的写入操作
                    with df_lock:
                        df.loc[len(df)] = formatted_info
                    return None  # 成功处理
                except Exception as e:
                    print(f"Error processing activationId {activation_id}: {e}")
                    return activation_id  # 返回失败的ID
            else:
                print(f"Activation result not found for activationId {activation_id}")
                return activation_id  # 返回失败的ID

        except Exception as e:
            print(f"Unexpected error processing {activation_id}: {e}")
            return activation_id

    # 准备参数列表
    args_list = [
        (i, aid, len(activation_id_list))
        for i, aid in enumerate(activation_id_list, start=1)
    ]

    # 使用线程池执行
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 收集所有失败的activation IDs
        failed_activation_ids = [
            aid
            for aid in executor.map(process_activation, args_list)
            if aid is not None
        ]

    # 计算总耗时
    elapsed_time = time.time() - start_time

    # 输出执行结果
    if failed_activation_ids:
        print("\n获取activation完成:")
        print(f"失败 activations: {len(failed_activation_ids)}个")
        for failed_id in failed_activation_ids:
            print(f"  - {failed_id}")
        print(f"失败 activations: {len(failed_activation_ids)}个")
    else:
        print("\n所有activation执行结果获取成功")

    print(f"\n总耗时: {elapsed_time:.2f} 秒")

    return failed_activation_ids, elapsed_time

# 检查文件是否已存在
def get_unique_path(save_path):
    """
    生成一个唯一的保存路径。
    如果原路径已存在，则添加(2)，再存在则添加(3)，以此类推。
    """
    if not os.path.exists(save_path):
        return save_path
    
    # 分离文件路径、文件名和扩展名
    directory, filename = os.path.split(save_path)
    name, ext = os.path.splitext(filename)
    
    counter = 2
    while True:
        # 构建新的文件名：原名(数字).扩展名
        new_filename = f"{name}({counter}){ext}"
        new_path = os.path.join(directory, new_filename)
        
        if not os.path.exists(new_path):
            return new_path
        
        counter += 1

import json

# 读取 JSON 文件
with open(workload_path, "r") as file:
    data = json.load(file)

# 提取各个属性值
# requests_per_cycle = data["requests_per_cycle"]
cycle_minutes = data["cycle_minutes"]
data_file = data["data_file"]
sql_path = data["sql_path"]
sql_list = data["sql_list"]
requests_per_cycle = len(sql_list)
interval_list = data["interval_list"]
# 新增一个interval，防止最后一个请求运行完后报错out of range
interval_list.append(0)

# 打印属性值
print("每周期总请求总量:", requests_per_cycle)
print("每周期总时间（分钟）:", cycle_minutes)
print("目标数据文件:", data_file)
print("SQL 文件夹路径:", sql_path)
print(f"SQL 列表长度：{len(sql_list)}")
print(f"请求间隔列表长度：{len(interval_list)}")
print(f"请求总间隔：{sum(interval_list)/60}分钟")
print(f"预计需要运行：{sum(interval_list)/60+0.002*requests_per_cycle}分钟")
num_cycles = 1  # 周期数
num_requests_per_cycle = requests_per_cycle
# num_requests_per_cycle = 100
cpu = 1
memory = 1.5
resource = f"{cpu}cpu-{memory}g"

apihost = ow_change_apihost
activationIdList = []
if data_file == "tpcds_1g":
    func_name = "remote_data_to_run_sql_tpcds"
else:
    func_name = "remote_data_to_run_sql"
    # func_name = "test_new_code"
print("cpu: ", cpu, "memory: ", memory)
folder_name = f"{workload_path.split('/')[-1].replace('.json','')}({num_cycles}x{num_requests_per_cycle})({resource})"
folder_path = f"./实验结果/数据处理/data/主记录/{folder_name}"
if not os.path.exists(folder_path):
    os .makedirs(folder_path)
    print(f"创建文件夹{folder_path}")
if containerStrategy == "被动":
    save_name = f"(warm{warm_time}){prefix}({schedule_strategy}){containerStrategy}{suffix}"
else:
    save_name = f"(warm{warm_time}){prefix}({schedule_strategy})(least{least_save_containers}){sliding_window_size}s_{trigger_threshold}x{sliding_step_size}s_{containerStrategy}{suffix}"
save_path = f"{folder_path}/{save_name}.csv"

save_path = get_unique_path(save_path)
save_name = os.path.basename(save_path).replace(".csv", "")
print(f"save_name: {save_name}")
print(f"save_path: {save_path}")

executor = ThreadPoolExecutor(max_workers=5)
futures = []
for cycle in range(num_cycles):
    for request in range(num_requests_per_cycle):
        start_time = time.time()  # 记录当前时间
        print(
            f"正在执行 cycle {cycle + 1}/{num_cycles}, request {request + 1}/{num_requests_per_cycle}"
        )
        sql_file = sql_list[request].replace("sql_", "")
        # 动态构建func
        func = {
            "func_name": func_name,
            "params": {
                "data_file": data_file,
                "db_file": "",
                "sql_file": f"{sql_path}/{sql_file}",
                "predict_time": predict_sql_execution_time(
                    cpu=1, memory=1.5, sql_file=f"{sql_path}/{sql_file}"
                )[0],
            },
        }
        # 使用线程池提交请求
        future = executor.submit(process_function, func)

        # 添加回调函数，当任务完成时自动将结果添加到activationIdList
        def add_to_list(future):
            result = future.result()
            activationIdList.append(result)
            # print(f"任务完成，获取到结果: {result}")

        future.add_done_callback(add_to_list)
        futures.append(future)

        # 计算请求完成时间与目标间隔的差值
        elapsed_time = time.time() - start_time
        # break
        sleep_time = interval_list[request] - elapsed_time
        if sleep_time > 0:  # 如果有剩余时间需要等待
            print(f"sleeping for {sleep_time:.2f} s")
            print("-----------------------------------------------------------")
            time.sleep(sleep_time)
        else:
            print("sleeping for 0.00 s")

    # 每个cycle运行结束后，等待所有热容器释放，再运行下一个cycle
    # time.sleep(180)
time.sleep(120)
executor.shutdown()
df = pd.DataFrame(columns=columns)
failed_ids, total_time = batch_process_activations(
    activation_id_list=activationIdList,  # 你的activation ID列表
    df=df,  # 用于存储结果的DataFrame
    max_workers=16,  # 可选参数，设置并行线程数
)
print(f"正在将结果储存到{save_path}")
df.to_csv(
    save_path,
    index=False,
)
upload_and_delete_local_file(
    local_file_path = save_path,
    remote_save_name = f"{save_name}.csv",
    remote_path=f"C:/Users/联想/Desktop/Serverless/ServerlessDB/实验代码/sqlite_ow_tpch/新架构/代码/数据处理/data/主记录/{folder_name}/",
)
upload_and_delete_local_file(
    local_file_path = "/home/t1/sqlite/db/container_stats.csv",
    remote_save_name = f"{save_name}.csv",
    remote_path=f"C:/Users/联想/Desktop/Serverless/ServerlessDB/实验代码/sqlite_ow_tpch/新架构/代码/数据处理/data/容器状态/{folder_name}/",
)
upload_and_delete_local_file(
    local_file_path = "/home/t1/sqlite/db/request_record.txt",
    remote_save_name = f"{save_name}.txt",
    remote_path=f"C:/Users/联想/Desktop/Serverless/ServerlessDB/实验代码/sqlite_ow_tpch/新架构/代码/数据处理/data/请求启动决策/{folder_name}/",
)
upload_and_delete_local_file(
    local_file_path = "/home/t1/sqlite/db/waiting_times.csv",
    remote_save_name = f"{save_name}.csv",
    remote_path=f"C:/Users/联想/Desktop/Serverless/ServerlessDB/实验代码/sqlite_ow_tpch/新架构/代码/数据处理/data/请求等待时间/{folder_name}/",
)
upload_and_delete_local_file(
    local_file_path = "/home/t1/sqlite/db/container_idle_times.csv",
    remote_save_name = f"{save_name}.csv",
    remote_path=f"C:/Users/联想/Desktop/Serverless/ServerlessDB/实验代码/sqlite_ow_tpch/新架构/代码/数据处理/data/容器idle时间/{folder_name}/",
)
upload_and_delete_local_file(
    local_file_path = "/home/t1/sqlite/db/schedule_time.txt",
    remote_save_name = f"{save_name}.txt",
    remote_path=f"C:/Users/联想/Desktop/Serverless/ServerlessDB/实验代码/sqlite_ow_tpch/新架构/代码/数据处理/data/请求调度时间/{folder_name}/",
)